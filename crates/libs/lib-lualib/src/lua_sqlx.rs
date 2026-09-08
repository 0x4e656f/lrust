use std::collections::HashSet;
use std::ffi::CStr;
use std::sync::{
    Arc, Once,
    atomic::{AtomicBool, AtomicI64, AtomicIsize, Ordering},
};
use std::time::{Duration, Instant};

use chrono::SecondsFormat;
use dashmap::DashMap;
use futures::TryStreamExt;
use lazy_static::lazy_static;
use sqlx::types::{Json, Uuid};
use sqlx::{
    Column, ColumnIndex, Database, MySql, MySqlPool, PgPool, Postgres, Row, Sqlite, SqlitePool,
    TypeInfo, ValueRef,
    migrate::MigrateDatabase,
    mysql::{MySqlPoolOptions, MySqlRow},
    postgres::{PgPoolOptions, PgRow, PgValueRef, types::PgTimeTz},
    sqlite::{SqlitePoolOptions, SqliteRow},
    types::chrono::{DateTime, FixedOffset, NaiveDate, NaiveDateTime, NaiveTime, Utc},
};
use tokio::{
    sync::{mpsc, watch},
    time::timeout,
};

use lib_core::context::CONTEXT;
use lib_lua::{
    self, cstr, ffi, laux,
    laux::{LuaArgs, LuaState, LuaTable, LuaValue},
    lreg, lreg_null, luaL_newlib, push_lua_table,
};

use crate::lua_json::{JsonOptions, encode_table};
use crate::{LOG_LEVEL_ERROR, moon_log};

lazy_static! {
    static ref DATABASE_CONNECTIONS: DashMap<String, DatabaseRegistration> = DashMap::new();
    static ref PENDING_RESPONSES: DashMap<isize, PendingResponse> = DashMap::new();
}

struct PendingResponse {
    created_at: Instant,
    owner: u32,
    value: DatabaseResponse,
}

static RESPONSE_ID: AtomicIsize = AtomicIsize::new(1);
static START_RESPONSE_REAPER: Once = Once::new();

// SQLx owns these tokens. Other lrust modules keep their original transport,
// and the host's existing void-returning ABI remains unchanged.
fn send_response(protocol_type: u8, owner: u32, session: i64, value: DatabaseResponse) {
    if session == 0 {
        return;
    }
    START_RESPONSE_REAPER.call_once(|| {
        CONTEXT.tokio_runtime.spawn(async {
            loop {
                tokio::time::sleep(Duration::from_secs(60)).await;
                // The host cannot report dropped integer messages. Reclaim
                // results whose Lua service has exited or abandoned its wait.
                PENDING_RESPONSES
                    .retain(|_, response| response.created_at.elapsed() < Duration::from_secs(300));
            }
        });
    });
    let response_id = RESPONSE_ID.fetch_add(1, Ordering::Relaxed);
    PENDING_RESPONSES.insert(
        response_id,
        PendingResponse {
            created_at: Instant::now(),
            owner,
            value,
        },
    );
    unsafe extern "C-unwind" {
        fn send_integer_message(type_: u8, receiver: u32, session: i64, val: isize);
    }
    unsafe { send_integer_message(protocol_type, owner, session, response_id) };
}

fn checked_string(state: LuaState, index: i32) -> String {
    let bytes = laux::lua_get::<&[u8]>(state, index);
    utf8_string(bytes, "SQLx string").unwrap_or_else(|error| laux::lua_error(state, error))
}

fn positive_option(state: LuaState, index: i32, default: i64, max: i64) -> i64 {
    let value = if unsafe { ffi::lua_isnoneornil(state.as_ptr(), index) } != 0 {
        default
    } else {
        laux::lua_get::<i64>(state, index)
    };
    if value <= 0 || value > max {
        laux::lua_error(
            state,
            format!("SQLx option #{index} must be between 1 and {max}"),
        );
    }
    value
}

trait SqlxUserdata {
    const METATABLE: &'static CStr;
}

// Validate type and size before any cast; a Lua caller can pass another
// userdata to a method. Option also makes repeated __gc calls harmless.
fn userdata_ptr<T: SqlxUserdata>(state: LuaState, index: i32) -> *mut Option<T> {
    unsafe {
        let ptr = ffi::luaL_checkudata(state.as_ptr(), index, T::METATABLE.as_ptr());
        if ffi::lua_rawlen(state.as_ptr(), index) != std::mem::size_of::<Option<T>>() {
            laux::lua_error(state, "invalid SQLx userdata size".to_string());
        }
        ptr.cast()
    }
}

fn connection_arg(state: LuaState, index: i32) -> DatabaseConnection {
    unsafe { &*userdata_ptr::<DatabaseConnection>(state, index) }
        .as_ref()
        .cloned()
        .unwrap_or_else(|| laux::lua_error(state, "SQLx connection has been collected".to_string()))
}

fn push_userdata<T: SqlxUserdata>(state: LuaState, value: T, lib: &[laux::LuaReg]) {
    extern "C-unwind" fn gc<T: SqlxUserdata>(state: *mut ffi::lua_State) -> i32 {
        unsafe {
            let ptr = ffi::luaL_testudata(state, 1, T::METATABLE.as_ptr());
            if !ptr.is_null() && ffi::lua_rawlen(state, 1) == std::mem::size_of::<Option<T>>() {
                (*ptr.cast::<Option<T>>()).take();
            }
        }
        0
    }
    laux::lua_checkstack(state, 4, std::ptr::null());
    unsafe {
        let ptr = ffi::lua_newuserdatauv(state.as_ptr(), std::mem::size_of::<Option<T>>(), 0);
        ptr.cast::<Option<T>>().write(Some(value));
        if ffi::luaL_newmetatable(state.as_ptr(), T::METATABLE.as_ptr()) != 0 {
            ffi::lua_createtable(state.as_ptr(), 0, lib.len() as i32);
            ffi::luaL_setfuncs(state.as_ptr(), lib.as_ptr().cast(), 0);
            ffi::lua_setfield(state.as_ptr(), -2, cstr!("__index"));
            ffi::lua_pushcfunction(state.as_ptr(), gc::<T>);
            ffi::lua_setfield(state.as_ptr(), -2, cstr!("__gc"));
            ffi::lua_pushboolean(state.as_ptr(), 0);
            ffi::lua_setfield(state.as_ptr(), -2, cstr!("__metatable"));
        }
        ffi::lua_setmetatable(state.as_ptr(), -2);
    }
}

enum DatabasePool {
    MySql(MySqlPool),
    Postgres(PgPool),
    Sqlite(SqlitePool),
}

impl DatabasePool {
    async fn connect(database_url: &str, timeout_duration: Duration) -> Result<Self, sqlx::Error> {
        async fn connect_with_timeout<F, T>(
            timeout_duration: Duration,
            connect_future: F,
        ) -> Result<T, sqlx::Error>
        where
            F: std::future::Future<Output = Result<T, sqlx::Error>>,
        {
            timeout(timeout_duration, connect_future)
                .await
                .map_err(|err| {
                    sqlx::Error::Io(std::io::Error::other(format!("Connection error: {}", err)))
                })?
        }

        if database_url.starts_with("mysql://") {
            let pool = connect_with_timeout(
                timeout_duration,
                MySqlPoolOptions::new()
                    .max_connections(1)
                    .connect(database_url),
            )
            .await?;
            Ok(DatabasePool::MySql(pool))
        } else if database_url.starts_with("postgres://")
            || database_url.starts_with("postgresql://")
        {
            let pool = connect_with_timeout(
                timeout_duration,
                PgPoolOptions::new()
                    .max_connections(1)
                    .connect(database_url),
            )
            .await?;
            Ok(DatabasePool::Postgres(pool))
        } else if database_url.starts_with("sqlite:") {
            let pool = connect_with_timeout(timeout_duration, async {
                if !Sqlite::database_exists(database_url).await? {
                    Sqlite::create_database(database_url).await?;
                }
                SqlitePoolOptions::new()
                    .max_connections(1)
                    // In-memory databases disappear when their last connection
                    // is reaped. Keep this actor's connection until close().
                    .idle_timeout(None)
                    .max_lifetime(None)
                    .connect(database_url)
                    .await
            })
            .await?;
            Ok(DatabasePool::Sqlite(pool))
        } else {
            Err(sqlx::Error::Configuration(
                "Unsupported database type".into(),
            ))
        }
    }

    fn make_query<'a, DB: sqlx::Database>(
        sql: &'a str,
        binds: &'a [QueryParams],
    ) -> Result<sqlx::query::Query<'a, DB, <DB as sqlx::Database>::Arguments<'a>>, sqlx::Error>
    where
        bool: sqlx::Encode<'a, DB> + sqlx::Type<DB>,
        i16: sqlx::Encode<'a, DB> + sqlx::Type<DB>,
        i32: sqlx::Encode<'a, DB> + sqlx::Type<DB>,
        i64: sqlx::Encode<'a, DB> + sqlx::Type<DB>,
        f32: sqlx::Encode<'a, DB> + sqlx::Type<DB>,
        f64: sqlx::Encode<'a, DB> + sqlx::Type<DB>,
        &'a str: sqlx::Encode<'a, DB> + sqlx::Type<DB>,
        serde_json::Value: sqlx::Encode<'a, DB> + sqlx::Type<DB>,
        &'a Vec<u8>: sqlx::Encode<'a, DB> + sqlx::Type<DB>,
        Uuid: sqlx::Encode<'a, DB> + sqlx::Type<DB>,
        NaiveDate: sqlx::Encode<'a, DB> + sqlx::Type<DB>,
        NaiveDateTime: sqlx::Encode<'a, DB> + sqlx::Type<DB>,
        NaiveTime: sqlx::Encode<'a, DB> + sqlx::Type<DB>,
        Option<bool>: sqlx::Encode<'a, DB>,
        Option<i16>: sqlx::Encode<'a, DB>,
        Option<i32>: sqlx::Encode<'a, DB>,
        Option<i64>: sqlx::Encode<'a, DB>,
        Option<f32>: sqlx::Encode<'a, DB>,
        Option<f64>: sqlx::Encode<'a, DB>,
        Option<&'a str>: sqlx::Encode<'a, DB>,
        Option<&'a Vec<u8>>: sqlx::Encode<'a, DB>,
        Option<serde_json::Value>: sqlx::Encode<'a, DB>,
        Option<Uuid>: sqlx::Encode<'a, DB>,
        Option<NaiveDate>: sqlx::Encode<'a, DB>,
        Option<NaiveDateTime>: sqlx::Encode<'a, DB>,
        Option<NaiveTime>: sqlx::Encode<'a, DB>,
    {
        let mut query = sqlx::query(sql);
        for bind in binds {
            query = match bind {
                QueryParams::Null(kind) => match kind {
                    NullType::Bool => query.bind(Option::<bool>::None),
                    NullType::Int16 => query.bind(Option::<i16>::None),
                    NullType::Int32 => query.bind(Option::<i32>::None),
                    NullType::Int64 => query.bind(Option::<i64>::None),
                    NullType::Float32 => query.bind(Option::<f32>::None),
                    NullType::Float64 => query.bind(Option::<f64>::None),
                    NullType::Text => query.bind(Option::<&str>::None),
                    NullType::Bytes => query.bind(Option::<&Vec<u8>>::None),
                    NullType::Json => query.bind(Option::<serde_json::Value>::None),
                    NullType::Uuid => query.bind(Option::<Uuid>::None),
                    NullType::Date => query.bind(Option::<NaiveDate>::None),
                    NullType::Timestamp => query.bind(Option::<NaiveDateTime>::None),
                    NullType::Time => query.bind(Option::<NaiveTime>::None),
                    NullType::TimestampTz | NullType::TimeTz => {
                        return Err(sqlx::Error::Configuration(
                            "timezone-aware null is only supported by PostgreSQL".into(),
                        ));
                    }
                },
                QueryParams::Bool(value) => query.bind(*value),
                QueryParams::Int(value) => query.bind(*value),
                QueryParams::Float(value) => query.bind(*value),
                QueryParams::Text(value) => query.bind(value.as_str()),
                QueryParams::Json(value) => query.bind(value),
                QueryParams::Bytes(value) => query.bind(value),
                QueryParams::PgBoolArray(_)
                | QueryParams::PgInt2Array(_)
                | QueryParams::PgInt4Array(_)
                | QueryParams::PgInt8Array(_)
                | QueryParams::PgFloat4Array(_)
                | QueryParams::PgFloat8Array(_)
                | QueryParams::PgTextArray(_)
                | QueryParams::PgBytesArray(_)
                | QueryParams::PgUuidArray(_)
                | QueryParams::PgJsonArray(_) => {
                    return Err(sqlx::Error::Configuration(
                        "PostgreSQL array parameter used with a non-PostgreSQL connection".into(),
                    ));
                }
            };
        }
        Ok(query)
    }

    fn make_pg_query<'a>(
        sql: &'a str,
        binds: &'a [QueryParams],
    ) -> Result<
        sqlx::query::Query<'a, Postgres, <Postgres as sqlx::Database>::Arguments<'a>>,
        sqlx::Error,
    > {
        let mut query = sqlx::query(sql);
        for bind in binds {
            query = match bind {
                QueryParams::Null(kind) => match kind {
                    NullType::Bool => query.bind(Option::<bool>::None),
                    NullType::Int16 => query.bind(Option::<i16>::None),
                    NullType::Int32 => query.bind(Option::<i32>::None),
                    NullType::Int64 => query.bind(Option::<i64>::None),
                    NullType::Float32 => query.bind(Option::<f32>::None),
                    NullType::Float64 => query.bind(Option::<f64>::None),
                    NullType::Text => query.bind(Option::<&str>::None),
                    NullType::Bytes => query.bind(Option::<&Vec<u8>>::None),
                    NullType::Json => query.bind(Option::<serde_json::Value>::None),
                    NullType::Uuid => query.bind(Option::<Uuid>::None),
                    NullType::Date => query.bind(Option::<NaiveDate>::None),
                    NullType::Timestamp => query.bind(Option::<NaiveDateTime>::None),
                    NullType::TimestampTz => query.bind(Option::<DateTime<Utc>>::None),
                    NullType::Time => query.bind(Option::<NaiveTime>::None),
                    NullType::TimeTz => {
                        query.bind(Option::<PgTimeTz<NaiveTime, FixedOffset>>::None)
                    }
                },
                QueryParams::Bool(value) => query.bind(*value),
                QueryParams::Int(value) => query.bind(*value),
                QueryParams::Float(value) => query.bind(*value),
                QueryParams::Text(value) => query.bind(value.as_str()),
                QueryParams::Json(value) => query.bind(value),
                QueryParams::Bytes(value) => query.bind(value),
                QueryParams::PgBoolArray(value) => query.bind(value.as_slice()),
                QueryParams::PgInt2Array(value) => query.bind(value.as_slice()),
                QueryParams::PgInt4Array(value) => query.bind(value.as_slice()),
                QueryParams::PgInt8Array(value) => query.bind(value.as_slice()),
                QueryParams::PgFloat4Array(value) => query.bind(value.as_slice()),
                QueryParams::PgFloat8Array(value) => query.bind(value.as_slice()),
                QueryParams::PgTextArray(value) => query.bind(value.as_slice()),
                QueryParams::PgBytesArray(value) => query.bind(value.as_slice()),
                QueryParams::PgUuidArray(value) => query.bind(value.as_slice()),
                QueryParams::PgJsonArray(value) => query.bind(value.as_slice()),
            };
        }
        Ok(query)
    }

    async fn query(
        &self,
        request: &DatabaseQuery,
        max_rows: usize,
    ) -> Result<DatabaseResponse, sqlx::Error> {
        match self {
            DatabasePool::MySql(pool) => {
                let query = Self::make_query(&request.sql, &request.binds)?;
                let mut stream = query.fetch(pool);
                let mut rows = Vec::new();
                while let Some(row) = stream.try_next().await? {
                    if rows.len() >= max_rows {
                        return Err(sqlx::Error::Configuration(
                            format!("query result exceeded max_rows ({max_rows})").into(),
                        ));
                    }
                    rows.push(row);
                }
                Ok(DatabaseResponse::MysqlRows(rows))
            }
            DatabasePool::Postgres(pool) => {
                let query = Self::make_pg_query(&request.sql, &request.binds)?;
                let mut stream = query.fetch(pool);
                let mut rows = Vec::new();
                while let Some(row) = stream.try_next().await? {
                    if rows.len() >= max_rows {
                        return Err(sqlx::Error::Configuration(
                            format!("query result exceeded max_rows ({max_rows})").into(),
                        ));
                    }
                    rows.push(row);
                }
                Ok(DatabaseResponse::PgRows(rows))
            }
            DatabasePool::Sqlite(pool) => {
                let query = Self::make_query(&request.sql, &request.binds)?;
                let mut stream = query.fetch(pool);
                let mut rows = Vec::new();
                while let Some(row) = stream.try_next().await? {
                    if rows.len() >= max_rows {
                        return Err(sqlx::Error::Configuration(
                            format!("query result exceeded max_rows ({max_rows})").into(),
                        ));
                    }
                    rows.push(row);
                }
                Ok(DatabaseResponse::SqliteRows(rows))
            }
        }
    }

    async fn execute(&self, request: &DatabaseQuery) -> Result<DatabaseResponse, sqlx::Error> {
        match self {
            DatabasePool::MySql(pool) => {
                let result = Self::make_query(&request.sql, &request.binds)?
                    .execute(pool)
                    .await?;
                Ok(DatabaseResponse::Execute {
                    rows_affected: result.rows_affected(),
                    last_insert_id: Some(result.last_insert_id()),
                })
            }
            DatabasePool::Postgres(pool) => {
                let result = Self::make_pg_query(&request.sql, &request.binds)?
                    .execute(pool)
                    .await?;
                Ok(DatabaseResponse::Execute {
                    rows_affected: result.rows_affected(),
                    last_insert_id: None,
                })
            }
            DatabasePool::Sqlite(pool) => {
                let result = Self::make_query(&request.sql, &request.binds)?
                    .execute(pool)
                    .await?;
                Ok(DatabaseResponse::Execute {
                    rows_affected: result.rows_affected(),
                    last_insert_id: u64::try_from(result.last_insert_rowid()).ok(),
                })
            }
        }
    }

    async fn batch(&self, sql: &str) -> Result<DatabaseResponse, sqlx::Error> {
        match self {
            DatabasePool::MySql(pool) => {
                let result = sqlx::raw_sql(sql).execute(pool).await?;
                Ok(DatabaseResponse::Execute {
                    rows_affected: result.rows_affected(),
                    last_insert_id: Some(result.last_insert_id()),
                })
            }
            DatabasePool::Postgres(pool) => {
                let result = sqlx::raw_sql(sql).execute(pool).await?;
                Ok(DatabaseResponse::Execute {
                    rows_affected: result.rows_affected(),
                    last_insert_id: None,
                })
            }
            DatabasePool::Sqlite(pool) => {
                let result = sqlx::raw_sql(sql).execute(pool).await?;
                Ok(DatabaseResponse::Execute {
                    rows_affected: result.rows_affected(),
                    last_insert_id: u64::try_from(result.last_insert_rowid()).ok(),
                })
            }
        }
    }

    async fn transaction(
        &self,
        requests: &[DatabaseQuery],
    ) -> Result<DatabaseResponse, sqlx::Error> {
        match self {
            DatabasePool::MySql(pool) => {
                let mut transaction = pool.begin().await?;
                let mut rows_affected = 0u64;
                for request in requests {
                    let query = Self::make_query(&request.sql, &request.binds)?;
                    rows_affected = rows_affected
                        .saturating_add(query.execute(&mut *transaction).await?.rows_affected());
                }
                transaction.commit().await?;
                Ok(DatabaseResponse::Transaction { rows_affected })
            }
            DatabasePool::Postgres(pool) => {
                let mut transaction = pool.begin().await?;
                let mut rows_affected = 0u64;
                for request in requests {
                    let query = Self::make_pg_query(&request.sql, &request.binds)?;
                    rows_affected = rows_affected
                        .saturating_add(query.execute(&mut *transaction).await?.rows_affected());
                }
                transaction.commit().await?;
                Ok(DatabaseResponse::Transaction { rows_affected })
            }
            DatabasePool::Sqlite(pool) => {
                let mut transaction = pool.begin().await?;
                let mut rows_affected = 0u64;
                for request in requests {
                    let query = Self::make_query(&request.sql, &request.binds)?;
                    rows_affected = rows_affected
                        .saturating_add(query.execute(&mut *transaction).await?.rows_affected());
                }
                transaction.commit().await?;
                Ok(DatabaseResponse::Transaction { rows_affected })
            }
        }
    }

    async fn close(&self) {
        match self {
            DatabasePool::MySql(pool) => pool.close().await,
            DatabasePool::Postgres(pool) => pool.close().await,
            DatabasePool::Sqlite(pool) => pool.close().await,
        }
    }
}

enum DatabaseRequest {
    Query(u32, i64, DatabaseQuery),
    Execute(u32, i64, DatabaseQuery),
    Batch(u32, i64, String),
    Transaction(u32, i64, Vec<DatabaseQuery>),
    Close,
}

#[derive(Clone)]
struct DatabaseConnection {
    tx: mpsc::Sender<DatabaseRequest>,
    counter: Arc<AtomicI64>,
    closing: Arc<AtomicBool>,
    closed: watch::Receiver<bool>,
}

impl SqlxUserdata for DatabaseConnection {
    const METATABLE: &'static CStr = c"sqlx_connection_metatable";
}

#[derive(Clone)]
struct DatabaseRegistration {
    identity: Arc<()>,
    tx: mpsc::WeakSender<DatabaseRequest>,
    counter: Arc<AtomicI64>,
    closing: Arc<AtomicBool>,
    closed: watch::Receiver<bool>,
}

struct DatabaseHandlerContext {
    protocol_type: u8,
    connection_name: String,
    identity: Arc<()>,
    counter: Arc<AtomicI64>,
    request_timeout: Duration,
    max_rows: usize,
    closed: watch::Sender<bool>,
}

enum DatabaseResponse {
    Connect(DatabaseConnection),
    Closed,
    PgRows(Vec<PgRow>),
    MysqlRows(Vec<MySqlRow>),
    SqliteRows(Vec<SqliteRow>),
    Error(sqlx::Error),
    Timeout(String),
    Execute {
        rows_affected: u64,
        last_insert_id: Option<u64>,
    },
    Transaction {
        rows_affected: u64,
    },
}

#[derive(Debug, Clone, Copy)]
enum NullType {
    Bool,
    Int16,
    Int32,
    Int64,
    Float32,
    Float64,
    Text,
    Bytes,
    Json,
    Uuid,
    Date,
    Timestamp,
    TimestampTz,
    Time,
    TimeTz,
}

#[derive(Debug, Clone)]
enum QueryParams {
    Null(NullType),
    Bool(bool),
    Int(i64),
    Float(f64),
    Text(String),
    Json(serde_json::Value),
    Bytes(Vec<u8>),
    PgBoolArray(Vec<Option<bool>>),
    PgInt2Array(Vec<Option<i16>>),
    PgInt4Array(Vec<Option<i32>>),
    PgInt8Array(Vec<Option<i64>>),
    PgFloat4Array(Vec<Option<f32>>),
    PgFloat8Array(Vec<Option<f64>>),
    PgTextArray(Vec<Option<String>>),
    PgBytesArray(Vec<Option<Vec<u8>>>),
    PgUuidArray(Vec<Option<Uuid>>),
    PgJsonArray(Vec<Option<Json<serde_json::Value>>>),
}

#[derive(Debug, Clone)]
struct DatabaseQuery {
    sql: String,
    binds: Vec<QueryParams>,
}

fn finish_request(
    context: &DatabaseHandlerContext,
    owner: u32,
    session: i64,
    response: DatabaseResponse,
) {
    context.counter.fetch_sub(1, Ordering::AcqRel);

    if session == 0 {
        let message = match &response {
            DatabaseResponse::Error(err) => Some(err.to_string()),
            DatabaseResponse::Timeout(message) => Some(message.clone()),
            _ => None,
        };
        if let Some(message) = message {
            moon_log(
                owner,
                LOG_LEVEL_ERROR,
                format!("Database '{}' error: {message}", context.connection_name),
            );
        }
        return;
    }

    send_response(context.protocol_type, owner, session, response);
}

async fn database_handler(
    pool: &DatabasePool,
    mut rx: mpsc::Receiver<DatabaseRequest>,
    context: DatabaseHandlerContext,
) {
    while let Some(op) = rx.recv().await {
        match op {
            DatabaseRequest::Query(owner, session, query_op) => {
                let response = match timeout(
                    context.request_timeout,
                    pool.query(&query_op, context.max_rows),
                )
                .await
                {
                    Ok(Ok(response)) => response,
                    Ok(Err(err)) => DatabaseResponse::Error(err),
                    Err(_) => DatabaseResponse::Timeout(format!(
                        "query timed out after {} ms",
                        context.request_timeout.as_millis()
                    )),
                };
                finish_request(&context, owner, session, response);
            }
            DatabaseRequest::Execute(owner, session, query_op) => {
                let response = match timeout(context.request_timeout, pool.execute(&query_op)).await
                {
                    Ok(Ok(response)) => response,
                    Ok(Err(err)) => DatabaseResponse::Error(err),
                    Err(_) => DatabaseResponse::Timeout(format!(
                        "execute timed out after {} ms",
                        context.request_timeout.as_millis()
                    )),
                };
                finish_request(&context, owner, session, response);
            }
            DatabaseRequest::Batch(owner, session, sql) => {
                let response = match timeout(context.request_timeout, pool.batch(&sql)).await {
                    Ok(Ok(response)) => response,
                    Ok(Err(err)) => DatabaseResponse::Error(err),
                    Err(_) => DatabaseResponse::Timeout(format!(
                        "batch timed out after {} ms",
                        context.request_timeout.as_millis()
                    )),
                };
                finish_request(&context, owner, session, response);
            }
            DatabaseRequest::Transaction(owner, session, query_ops) => {
                let response =
                    match timeout(context.request_timeout, pool.transaction(&query_ops)).await {
                        Ok(Ok(response)) => response,
                        Ok(Err(err)) => DatabaseResponse::Error(err),
                        Err(_) => DatabaseResponse::Timeout(format!(
                            "transaction timed out after {} ms",
                            context.request_timeout.as_millis()
                        )),
                    };
                finish_request(&context, owner, session, response);
            }
            DatabaseRequest::Close => {
                // Prevent any later sends, but still process everything which
                // raced with close and was already accepted by the channel.
                rx.close();
            }
        }
    }

    pool.close().await;
    DATABASE_CONNECTIONS.remove_if(&context.connection_name, |_, current| {
        Arc::ptr_eq(&current.identity, &context.identity)
    });
    let _ = context.closed.send(true);
}

extern "C-unwind" fn connect(state: LuaState) -> i32 {
    let protocol_type: u8 = laux::lua_get(state, 1);
    let owner = laux::lua_get(state, 2);
    let session: i64 = laux::lua_get(state, 3);

    // The async task outlives this Lua call, so never retain references into
    // the Lua stack here.
    let database_url = checked_string(state, 4);
    let name = checked_string(state, 5);
    let connect_timeout = positive_option(state, 6, 5000, u32::MAX as i64) as u64;
    let request_timeout_ms = positive_option(state, 7, 30000, u32::MAX as i64) as u64;
    let max_rows = positive_option(state, 8, 100000, i32::MAX as i64) as usize;
    let queue_capacity = positive_option(state, 9, 100, i32::MAX as i64) as usize;

    CONTEXT.tokio_runtime.spawn(async move {
        match DatabasePool::connect(&database_url, Duration::from_millis(connect_timeout)).await {
            Ok(pool) => {
                let (tx, rx) = mpsc::channel(queue_capacity);
                let counter = Arc::new(AtomicI64::new(0));
                let identity = Arc::new(());
                let closing = Arc::new(AtomicBool::new(false));
                let (closed_tx, closed_rx) = watch::channel(false);
                let connection = DatabaseConnection {
                    tx: tx.clone(),
                    counter: counter.clone(),
                    closing: closing.clone(),
                    closed: closed_rx.clone(),
                };
                if let Some(previous) = DATABASE_CONNECTIONS.insert(
                    name.clone(),
                    DatabaseRegistration {
                        identity: identity.clone(),
                        tx: tx.downgrade(),
                        counter: counter.clone(),
                        closing,
                        closed: closed_rx,
                    },
                ) && !previous.closing.swap(true, Ordering::AcqRel)
                    && let Some(previous_tx) = previous.tx.upgrade()
                {
                    tokio::spawn(async move {
                        let _ = previous_tx.send(DatabaseRequest::Close).await;
                    });
                }
                send_response(
                    protocol_type,
                    owner,
                    session,
                    DatabaseResponse::Connect(connection),
                );
                drop(tx);
                database_handler(
                    &pool,
                    rx,
                    DatabaseHandlerContext {
                        protocol_type,
                        connection_name: name,
                        identity,
                        counter,
                        request_timeout: Duration::from_millis(request_timeout_ms),
                        max_rows,
                        closed: closed_tx,
                    },
                )
                .await;
            }
            Err(err) => {
                send_response(protocol_type, owner, session, DatabaseResponse::Error(err));
            }
        };
    });

    laux::lua_push(state, session);
    1
}

fn utf8_string(bytes: &[u8], context: &str) -> Result<String, String> {
    String::from_utf8(bytes.to_vec()).map_err(|err| format!("{context} must be valid UTF-8: {err}"))
}

fn parse_null_type(type_name: &str) -> Result<NullType, String> {
    match type_name.trim().to_ascii_lowercase().as_str() {
        "bool" | "boolean" => Ok(NullType::Bool),
        "int2" | "smallint" => Ok(NullType::Int16),
        "int4" | "int" | "integer" => Ok(NullType::Int32),
        "int8" | "bigint" => Ok(NullType::Int64),
        "float4" | "real" => Ok(NullType::Float32),
        "float8" | "double" | "double precision" => Ok(NullType::Float64),
        "text" | "string" | "varchar" | "char" => Ok(NullType::Text),
        "bytes" | "bytea" | "blob" | "binary" => Ok(NullType::Bytes),
        "json" | "jsonb" => Ok(NullType::Json),
        "uuid" => Ok(NullType::Uuid),
        "date" => Ok(NullType::Date),
        "timestamp" | "datetime" => Ok(NullType::Timestamp),
        "timestamptz" | "timestamp with time zone" => Ok(NullType::TimestampTz),
        "time" => Ok(NullType::Time),
        "timetz" | "time with time zone" => Ok(NullType::TimeTz),
        _ => Err(format!("sqlx.null unsupported SQL type: {type_name}")),
    }
}

fn get_typed_query_param(
    wrapper: &LuaTable,
    options: &JsonOptions,
) -> Result<Option<QueryParams>, String> {
    let kind = {
        let marker = wrapper.rawget("__sqlx_param");
        match &marker.value {
            LuaValue::Nil | LuaValue::None => return Ok(None),
            LuaValue::String(value) => utf8_string(value, "sqlx parameter kind")?,
            _ => return Err("sqlx parameter kind must be a string".to_string()),
        }
    };

    match kind.as_str() {
        "null" => {
            let type_name = {
                let field = wrapper.rawget("type");
                match &field.value {
                    LuaValue::Nil | LuaValue::None => "text".to_string(),
                    LuaValue::String(value) => utf8_string(value, "sqlx.null type")?,
                    _ => return Err("sqlx.null type must be a string".to_string()),
                }
            };
            Ok(Some(QueryParams::Null(parse_null_type(&type_name)?)))
        }
        "text" => {
            let field = wrapper.rawget("value");
            match &field.value {
                LuaValue::String(value) => Ok(Some(QueryParams::Text(utf8_string(
                    value,
                    "sqlx.text value",
                )?))),
                _ => Err("sqlx.text value must be a string".to_string()),
            }
        }
        "bytes" => {
            let field = wrapper.rawget("value");
            match &field.value {
                LuaValue::String(value) => Ok(Some(QueryParams::Bytes(value.to_vec()))),
                _ => Err("sqlx.bytes value must be a string".to_string()),
            }
        }
        "json" => {
            let field = wrapper.rawget("value");
            lua_value_to_json(&field.value, options)
                .map(QueryParams::Json)
                .map(Some)
                .ok_or_else(|| "sqlx.json value cannot be encoded as JSON".to_string())
        }
        _ => Err(format!("unknown sqlx parameter kind: {kind}")),
    }
}

fn get_query_param(state: LuaState, i: i32) -> Result<QueryParams, String> {
    let options = JsonOptions::default();

    let res = match LuaValue::from_stack(state, i) {
        LuaValue::Nil => QueryParams::Null(NullType::Text),
        LuaValue::LightUserData(ptr) if ptr.is_null() => QueryParams::Null(NullType::Text),
        LuaValue::Boolean(val) => QueryParams::Bool(val),
        LuaValue::Number(val) => QueryParams::Float(val),
        LuaValue::Integer(val) => QueryParams::Int(val),
        LuaValue::String(val) => QueryParams::Text(utf8_string(val, "SQL text parameter")?),
        LuaValue::Table(val) => {
            if let Some(param) = get_typed_query_param(&val, &options)? {
                return Ok(param);
            }

            let is_array_wrapper = {
                let marker = val.rawget("__sqlx_array");
                matches!(&marker.value, LuaValue::Boolean(true))
            } || val.getmetafield(cstr!("__sqlx_array")).is_some();
            if is_array_wrapper {
                return get_pg_array_param(&val, &options);
            }

            let mut buffer = Vec::new();
            encode_table(&mut buffer, &val, 0, false, &options)
                .map_err(|err| format!("SQL JSON parameter encode error: {err}"))?;
            QueryParams::Json(
                serde_json::from_slice(buffer.as_slice())
                    .map_err(|err| format!("SQL JSON parameter decode error: {err}"))?,
            )
        }
        _t => {
            return Err(format!(
                "get_query_param: unsupport value type :{}",
                laux::type_name(state, i)
            ));
        }
    };
    Ok(res)
}

fn collect_pg_array<T, F>(
    values: &LuaTable,
    expected: &str,
    mut convert: F,
) -> Result<Vec<Option<T>>, String>
where
    F: for<'a> FnMut(LuaValue<'a>) -> Option<T>,
{
    let (is_array, len) = values.array_len();
    if !is_array && values.iter().next().is_some() {
        return Err("sqlx.array values must be a Lua sequence".to_string());
    }

    let mut result = Vec::with_capacity(len);
    for (index, value) in values.array_iter().enumerate() {
        match value {
            LuaValue::LightUserData(ptr) if ptr.is_null() => result.push(None),
            value => match convert(value) {
                Some(value) => result.push(Some(value)),
                None => {
                    return Err(format!(
                        "sqlx.array element {} must be {}",
                        index + 1,
                        expected
                    ));
                }
            },
        }
    }
    Ok(result)
}

fn lua_value_to_json(value: &LuaValue<'_>, options: &JsonOptions) -> Option<serde_json::Value> {
    match value {
        LuaValue::Nil => Some(serde_json::Value::Null),
        LuaValue::LightUserData(ptr) if ptr.is_null() => Some(serde_json::Value::Null),
        LuaValue::Boolean(value) => Some(serde_json::Value::Bool(*value)),
        LuaValue::Integer(value) => Some(serde_json::Value::Number((*value).into())),
        LuaValue::Number(value) => {
            serde_json::Number::from_f64(*value).map(serde_json::Value::Number)
        }
        LuaValue::String(value) => String::from_utf8(value.to_vec())
            .ok()
            .map(serde_json::Value::String),
        LuaValue::Table(value) => {
            let mut buffer = Vec::new();
            encode_table(&mut buffer, value, 0, false, options).ok()?;
            serde_json::from_slice(&buffer).ok()
        }
        _ => None,
    }
}

fn get_pg_array_param(wrapper: &LuaTable, options: &JsonOptions) -> Result<QueryParams, String> {
    let type_name = {
        let field = wrapper.rawget("type");
        match &field.value {
            LuaValue::String(value) => utf8_string(value, "sqlx.array type")?,
            _ => return Err("sqlx.array type must be a string".to_string()),
        }
    };

    let values_field = wrapper.rawget("values");
    let values = match &values_field.value {
        LuaValue::Table(value) => value,
        _ => return Err("sqlx.array values must be a table".to_string()),
    };

    let normalized = type_name.trim().to_ascii_lowercase();
    let element_type = normalized.strip_suffix("[]").unwrap_or(&normalized);

    match element_type {
        "bool" | "boolean" => Ok(QueryParams::PgBoolArray(collect_pg_array(
            values,
            "a boolean",
            |value| match value {
                LuaValue::Boolean(value) => Some(value),
                _ => None,
            },
        )?)),
        "int2" | "smallint" => Ok(QueryParams::PgInt2Array(collect_pg_array(
            values,
            "an int2 integer",
            |value| match value {
                LuaValue::Integer(value) => i16::try_from(value).ok(),
                _ => None,
            },
        )?)),
        "int4" | "integer" | "int" => Ok(QueryParams::PgInt4Array(collect_pg_array(
            values,
            "an int4 integer",
            |value| match value {
                LuaValue::Integer(value) => i32::try_from(value).ok(),
                _ => None,
            },
        )?)),
        "int8" | "bigint" => Ok(QueryParams::PgInt8Array(collect_pg_array(
            values,
            "an int8 integer",
            |value| match value {
                LuaValue::Integer(value) => Some(value),
                _ => None,
            },
        )?)),
        "float4" | "real" => Ok(QueryParams::PgFloat4Array(collect_pg_array(
            values,
            "a number",
            |value| match value {
                LuaValue::Integer(value) => Some(value as f32),
                LuaValue::Number(value) => Some(value as f32),
                _ => None,
            },
        )?)),
        "float8" | "double" | "double precision" => Ok(QueryParams::PgFloat8Array(
            collect_pg_array(values, "a number", |value| match value {
                LuaValue::Integer(value) => Some(value as f64),
                LuaValue::Number(value) => Some(value),
                _ => None,
            })?,
        )),
        "text" | "varchar" | "char" | "character varying" | "name" => Ok(QueryParams::PgTextArray(
            collect_pg_array(values, "a string", |value| match value {
                LuaValue::String(value) => String::from_utf8(value.to_vec()).ok(),
                _ => None,
            })?,
        )),
        "bytea" | "bytes" => Ok(QueryParams::PgBytesArray(collect_pg_array(
            values,
            "a string",
            |value| match value {
                LuaValue::String(value) => Some(value.to_vec()),
                _ => None,
            },
        )?)),
        "uuid" => Ok(QueryParams::PgUuidArray(collect_pg_array(
            values,
            "a UUID string",
            |value| match value {
                LuaValue::String(value) => std::str::from_utf8(value)
                    .ok()
                    .and_then(|value| Uuid::parse_str(value).ok()),
                _ => None,
            },
        )?)),
        "json" | "jsonb" => {
            let values = collect_pg_array(values, "a JSON value", |value| {
                lua_value_to_json(&value, options).map(Json)
            })?;
            Ok(QueryParams::PgJsonArray(values))
        }
        _ => Err(format!(
            "sqlx.array unsupported PostgreSQL element type: {}",
            type_name
        )),
    }
}

fn enqueue_query(state: LuaState, execute_only: bool) -> i32 {
    let mut args = LuaArgs::new(1);
    let conn = connection_arg(state, args.iter_arg());

    let owner = laux::lua_get(state, args.iter_arg());
    let session = laux::lua_get(state, args.iter_arg());

    if conn.closing.load(Ordering::Acquire) {
        push_lua_table!(
            state,
            "kind" => "CLOSED",
            "message" => "database connection is closing"
        );
        return 1;
    }

    let sql = checked_string(state, args.iter_arg());
    let mut params = Vec::new();
    let top = laux::lua_top(state);
    for i in args.iter_arg()..=top {
        let param = get_query_param(state, i);
        match param {
            Ok(value) => {
                params.push(value);
            }
            Err(err) => {
                push_lua_table!(
                    state,
                    "kind" => "ERROR",
                    "message" => err
                );
                return 1;
            }
        }
    }

    let query = DatabaseQuery { sql, binds: params };
    let request = if execute_only {
        DatabaseRequest::Execute(owner, session, query)
    } else {
        DatabaseRequest::Query(owner, session, query)
    };

    conn.counter.fetch_add(1, Ordering::AcqRel);
    match conn.tx.try_send(request) {
        Ok(_) => {
            laux::lua_push(state, session);
            1
        }
        Err(err) => {
            conn.counter.fetch_sub(1, Ordering::AcqRel);
            let kind = if matches!(&err, mpsc::error::TrySendError::Full(_)) {
                "BUSY"
            } else {
                "CLOSED"
            };
            push_lua_table!(
                state,
                "kind" => kind,
                "message" => err.to_string()
            );
            1
        }
    }
}

extern "C-unwind" fn query(state: LuaState) -> i32 {
    enqueue_query(state, false)
}

extern "C-unwind" fn execute(state: LuaState) -> i32 {
    enqueue_query(state, true)
}

extern "C-unwind" fn batch(state: LuaState) -> i32 {
    let mut args = LuaArgs::new(1);
    let conn = connection_arg(state, args.iter_arg());
    let owner = laux::lua_get(state, args.iter_arg());
    let session = laux::lua_get(state, args.iter_arg());
    let sql = checked_string(state, args.iter_arg());

    if conn.closing.load(Ordering::Acquire) {
        push_lua_table!(
            state,
            "kind" => "CLOSED",
            "message" => "database connection is closing"
        );
        return 1;
    }

    conn.counter.fetch_add(1, Ordering::AcqRel);
    match conn
        .tx
        .try_send(DatabaseRequest::Batch(owner, session, sql))
    {
        Ok(_) => laux::lua_push(state, session),
        Err(err) => {
            conn.counter.fetch_sub(1, Ordering::AcqRel);
            let kind = if matches!(&err, mpsc::error::TrySendError::Full(_)) {
                "BUSY"
            } else {
                "CLOSED"
            };
            push_lua_table!(
                state,
                "kind" => kind,
                "message" => err.to_string()
            );
        }
    }
    1
}

struct TransactionQuerys {
    querys: Vec<DatabaseQuery>,
}

impl SqlxUserdata for TransactionQuerys {
    const METATABLE: &'static CStr = c"sqlx_transaction_metatable";
}

extern "C-unwind" fn push_transaction_query(state: LuaState) -> i32 {
    // Validate now, but don't hold a mutable borrow across parameter encoding
    // (Lua metamethods can re-enter this function).
    userdata_ptr::<TransactionQuerys>(state, 1);
    let sql = checked_string(state, 2);
    let mut params = Vec::new();
    let top = laux::lua_top(state);
    for i in 3..=top {
        let param = get_query_param(state, i);
        match param {
            Ok(value) => {
                params.push(value);
            }
            Err(err) => {
                drop(params);
                laux::lua_error(state, err);
            }
        }
    }

    let querys = unsafe { &mut *userdata_ptr::<TransactionQuerys>(state, 1) };
    let querys = querys.as_mut().unwrap_or_else(|| {
        laux::lua_error(state, "SQLx transaction has been collected".to_string())
    });
    querys.querys.push(DatabaseQuery { sql, binds: params });

    0
}

extern "C-unwind" fn make_transaction(state: LuaState) -> i32 {
    push_userdata(
        state,
        TransactionQuerys { querys: Vec::new() },
        &[lreg!("push", push_transaction_query), lreg_null!()],
    );
    1
}

extern "C-unwind" fn transaction(state: LuaState) -> i32 {
    let mut args = LuaArgs::new(1);
    let conn = connection_arg(state, args.iter_arg());

    let owner = laux::lua_get(state, args.iter_arg());
    let session = laux::lua_get(state, args.iter_arg());

    let querys = unsafe { &mut *userdata_ptr::<TransactionQuerys>(state, args.iter_arg()) };
    let querys = querys.as_mut().unwrap_or_else(|| {
        laux::lua_error(state, "SQLx transaction has been collected".to_string())
    });

    if conn.closing.load(Ordering::Acquire) {
        push_lua_table!(
            state,
            "kind" => "CLOSED",
            "message" => "database connection is closing"
        );
        return 1;
    }

    let request = DatabaseRequest::Transaction(owner, session, std::mem::take(&mut querys.querys));
    conn.counter.fetch_add(1, Ordering::AcqRel);
    match conn.tx.try_send(request) {
        Ok(_) => {
            laux::lua_push(state, session);
            1
        }
        Err(err) => {
            conn.counter.fetch_sub(1, Ordering::AcqRel);
            let kind = if matches!(&err, mpsc::error::TrySendError::Full(_)) {
                "BUSY"
            } else {
                "CLOSED"
            };
            let message = err.to_string();
            if let DatabaseRequest::Transaction(_, _, requests) = err.into_inner() {
                querys.querys = requests;
            }
            push_lua_table!(
                state,
                "kind" => kind,
                "message" => message
            );
            1
        }
    }
}

extern "C-unwind" fn close(state: LuaState) -> i32 {
    let conn = connection_arg(state, 1);
    let protocol_type: u8 = laux::lua_get(state, 2);
    let owner: u32 = laux::lua_get(state, 3);
    let session: i64 = laux::lua_get(state, 4);

    let first_close = !conn.closing.swap(true, Ordering::AcqRel);
    let tx = conn.tx.clone();
    let mut closed = conn.closed.clone();
    CONTEXT.tokio_runtime.spawn(async move {
        if first_close {
            let _ = tx.send(DatabaseRequest::Close).await;
        }
        drop(tx);
        // All callers, including repeated close() calls, wait for the pool
        // itself to finish closing. A dropped handler is reported as an error.
        let response = match closed.wait_for(|done| *done).await {
            Ok(_) => DatabaseResponse::Closed,
            Err(_) => DatabaseResponse::Error(sqlx::Error::WorkerCrashed),
        };
        send_response(protocol_type, owner, session, response);
    });

    laux::lua_push(state, session);
    1
}

#[derive(Copy, Clone)]
enum DbType {
    Int8,
    UInt8,
    Int16,
    UInt16,
    Int32,
    UInt32,
    Int64,
    UInt64,
    Float32,
    Float64,
    Text,
    Bool,
    Timestamp,
    TimestampTz,
    Date,
    Time,
    Uuid,
    Bytes,
    Json,
    Null,
    UnsupportedDecimal,
    TimeTz,
    Unknown,
}

static DB_TYPE_MAP: phf::Map<&'static str, DbType> = phf::phf_map! {
    // Int32 types
    "INT4" => DbType::Int32,
    "INT" => DbType::Int32,
    "INTEGER" => DbType::Int32,
    "MEDIUMINT" => DbType::Int32,
    // Int64 types
    "INT8" => DbType::Int64,
    "BIGINT" => DbType::Int64,
    // Int16 types
    "INT2" => DbType::Int16,
    "SMALLINT" => DbType::Int16,
    // Int8 type
    "TINYINT" => DbType::Int8,
    // Float64 types
    "FLOAT8" => DbType::Float64,
    "DOUBLE" => DbType::Float64,
    // Float32 types
    "FLOAT4" => DbType::Float32,
    "FLOAT" => DbType::Float32,
    "REAL" => DbType::Float32,
    // Text types
    "TEXT" => DbType::Text,
    "VARCHAR" => DbType::Text,
    "CHAR" => DbType::Text,
    "BPCHAR" => DbType::Text,
    "NAME" => DbType::Text,
    "TINYTEXT" => DbType::Text,
    "MEDIUMTEXT" => DbType::Text,
    "LONGTEXT" => DbType::Text,
    "NVARCHAR" => DbType::Text,
    "NCHAR" => DbType::Text,
    // Bool types
    "BOOL" => DbType::Bool,
    "BOOLEAN" => DbType::Bool,
    // Timestamp types
    "TIMESTAMP" => DbType::Timestamp,
    "TIMESTAMPTZ" => DbType::TimestampTz,
    "DATETIME" => DbType::Timestamp,
    // Date type
    "DATE" => DbType::Date,
    // Time type
    "TIME" => DbType::Time,
    // UUID type
    "UUID" => DbType::Uuid,
    // Bytes types
    "BYTEA" => DbType::Bytes,
    "BLOB" => DbType::Bytes,
    "VARBINARY" => DbType::Bytes,
    "BINARY" => DbType::Bytes,
    "TINYBLOB" => DbType::Bytes,
    "MEDIUMBLOB" => DbType::Bytes,
    "LONGBLOB" => DbType::Bytes,
    // Json types
    "JSON" => DbType::Json,
    "JSONB" => DbType::Json,
    // Null type
    "NULL" => DbType::Null,
    // Unsupported decimal types
    "DECIMAL" => DbType::UnsupportedDecimal,
    "NUMERIC" => DbType::UnsupportedDecimal,
    "MONEY" => DbType::UnsupportedDecimal,
    "TIMETZ" => DbType::TimeTz,
    // Unsigned types
    "TINYINT UNSIGNED" => DbType::UInt8,
    "SMALLINT UNSIGNED" => DbType::UInt16,
    "INT UNSIGNED" => DbType::UInt32,
    "MEDIUMINT UNSIGNED" => DbType::UInt32,
    "BIGINT UNSIGNED" => DbType::UInt64,
};

impl DbType {
    #[inline]
    fn from_name(name: &str) -> Self {
        DB_TYPE_MAP.get(name).copied().unwrap_or(Self::Unknown)
    }
}

fn push_sql_null(state: LuaState) {
    laux::lua_pushlightuserdata(state, std::ptr::null_mut());
}

fn push_u64(state: LuaState, value: u64) {
    if let Ok(value) = i64::try_from(value) {
        laux::lua_push(state, value);
    } else {
        laux::lua_push(state, value.to_string());
    }
}

fn ensure_unique_columns<'a, I>(columns: I) -> Result<(), String>
where
    I: IntoIterator<Item = &'a str>,
{
    let mut names = HashSet::new();
    for name in columns {
        if !names.insert(name) {
            return Err(format!(
                "duplicate result column '{name}'; use an explicit SQL alias"
            ));
        }
    }
    Ok(())
}

fn process_rows<'a, DB>(state: LuaState, rows: &'a [<DB as Database>::Row]) -> Result<i32, String>
where
    DB: sqlx::Database + 'static,
    usize: ColumnIndex<<DB as Database>::Row>,
    i8: sqlx::Decode<'a, DB>,
    u8: sqlx::Decode<'a, DB>,
    i16: sqlx::Decode<'a, DB>,
    u16: sqlx::Decode<'a, DB>,
    i32: sqlx::Decode<'a, DB>,
    u32: sqlx::Decode<'a, DB>,
    i64: sqlx::Decode<'a, DB>,
    u64: sqlx::Decode<'a, DB>,
    f32: sqlx::Decode<'a, DB>,
    f64: sqlx::Decode<'a, DB>,
    bool: sqlx::Decode<'a, DB>,
    &'a str: sqlx::Decode<'a, DB>,
    &'a [u8]: sqlx::Decode<'a, DB>,
    NaiveDate: sqlx::Decode<'a, DB>,
    NaiveDateTime: sqlx::Decode<'a, DB>,
    NaiveTime: sqlx::Decode<'a, DB>,
    Uuid: sqlx::Decode<'a, DB>,
{
    let table = LuaTable::new(state, rows.len(), 0);
    if rows.is_empty() {
        return Ok(1);
    }

    let column_info: Vec<(usize, &str, DbType)> = rows
        .first()
        .unwrap()
        .columns()
        .iter()
        .enumerate()
        .map(|(index, column)| {
            let name = column.name();
            let db_type = DbType::from_name(column.type_info().name());
            (index, name, db_type)
        })
        .collect();
    ensure_unique_columns(column_info.iter().map(|(_, name, _)| *name))?;

    let is_sqlite = std::any::TypeId::of::<DB>() == std::any::TypeId::of::<Sqlite>();

    for (i, row) in rows.iter().enumerate() {
        let row_table = LuaTable::new(state, 0, row.len());
        for (index, column_name, db_type) in column_info.iter() {
            let value = row
                .try_get_raw(*index)
                .map_err(|error| format!("{} decode error: {}", column_name, error))?;
            if value.is_null() {
                row_table.insert_x(*column_name, || push_sql_null(state));
                continue;
            }

            // SQLite storage types can differ for every row in one column.
            // First-row metadata must not coerce later text/blob values to 0.
            let db_type = if is_sqlite {
                DbType::from_name(value.type_info().name())
            } else {
                *db_type
            };

            macro_rules! decode_value {
                ($value_type:ty) => {{
                    <$value_type as sqlx::Decode<DB>>::decode(value)
                        .map_err(|error| format!("{} decode error: {}", column_name, error))?
                }};
            }

            match db_type {
                DbType::Int8 => {
                    if is_sqlite {
                        row_table.insert(*column_name, decode_value!(i64));
                    } else {
                        row_table.insert(*column_name, decode_value!(i8));
                    }
                }
                DbType::Int16 => {
                    if is_sqlite {
                        row_table.insert(*column_name, decode_value!(i64));
                    } else {
                        row_table.insert(*column_name, decode_value!(i16));
                    }
                }
                DbType::Int32 => {
                    if is_sqlite {
                        row_table.insert(*column_name, decode_value!(i64));
                    } else {
                        row_table.insert(*column_name, decode_value!(i32));
                    }
                }
                DbType::Int64 => {
                    row_table.insert(*column_name, decode_value!(i64));
                }
                DbType::UInt8 => {
                    row_table.insert(*column_name, decode_value!(u8) as i64);
                }
                DbType::UInt16 => {
                    row_table.insert(*column_name, decode_value!(u16) as i64);
                }
                DbType::UInt32 => {
                    row_table.insert(*column_name, decode_value!(u32) as i64);
                }
                DbType::UInt64 => {
                    let decoded = decode_value!(u64);
                    row_table.insert_x(*column_name, || push_u64(state, decoded));
                }
                DbType::Float32 => {
                    if is_sqlite {
                        row_table.insert(*column_name, decode_value!(f64));
                    } else {
                        row_table.insert(*column_name, decode_value!(f32));
                    }
                }
                DbType::Float64 => {
                    row_table.insert(*column_name, decode_value!(f64));
                }
                DbType::Text => {
                    row_table.insert(*column_name, decode_value!(&str));
                }
                DbType::Bool => {
                    row_table.insert(*column_name, decode_value!(bool));
                }
                DbType::Timestamp => {
                    let decoded = decode_value!(NaiveDateTime);
                    row_table.insert(
                        *column_name,
                        decoded.format("%Y-%m-%d %H:%M:%S%.f").to_string(),
                    );
                }
                DbType::TimestampTz => {
                    return Err(format!(
                        "unexpected timezone-aware timestamp for column '{}'",
                        column_name
                    ));
                }
                DbType::Date => {
                    let decoded = decode_value!(NaiveDate);
                    row_table.insert(*column_name, decoded.format("%Y-%m-%d").to_string());
                }
                DbType::Time => {
                    let decoded = decode_value!(NaiveTime);
                    row_table.insert(*column_name, decoded.format("%H:%M:%S%.f").to_string());
                }
                DbType::Uuid => {
                    row_table.insert(*column_name, decode_value!(Uuid).to_string());
                }
                DbType::Bytes => {
                    row_table.insert(*column_name, decode_value!(&[u8]));
                }
                DbType::Json => {
                    let decoded = decode_value!(&str);
                    let json = serde_json::from_str(decoded)
                        .map_err(|error| format!("{} JSON decode error: {}", column_name, error))?;
                    row_table.insert_x(*column_name, || push_json_value(state, json));
                }
                DbType::Null => {
                    row_table.insert_x(*column_name, || push_sql_null(state));
                }
                DbType::UnsupportedDecimal => {
                    return Err(format!(
                        "unsupported decimal type for column '{}'",
                        column_name
                    ));
                }
                DbType::TimeTz => {
                    return Err(format!(
                        "unexpected time with time zone type for column '{}'",
                        column_name
                    ));
                }
                DbType::Unknown => {
                    let decoded = decode_value!(&[u8]);
                    row_table.insert(*column_name, decoded);
                }
            }
        }
        table.rawseti(i + 1);
    }
    Ok(1)
}

fn push_optional_array<T, F>(state: LuaState, values: Vec<Option<T>>, mut push_value: F)
where
    F: FnMut(LuaState, T),
{
    let table = LuaTable::new(state, values.len(), 0);
    for (index, value) in values.into_iter().enumerate() {
        match value {
            Some(value) => push_value(state, value),
            None => laux::lua_pushlightuserdata(state, std::ptr::null_mut()),
        }
        table.rawseti(index + 1);
    }
}

fn push_json_value(state: LuaState, value: serde_json::Value) {
    laux::lua_checkstack(state, 4, std::ptr::null());
    match value {
        serde_json::Value::Null => {
            laux::lua_pushlightuserdata(state, std::ptr::null_mut());
        }
        serde_json::Value::Bool(value) => laux::lua_push(state, value),
        serde_json::Value::Number(value) => {
            if let Some(value) = value.as_i64() {
                laux::lua_push(state, value);
            } else if let Some(value) = value.as_u64() {
                push_u64(state, value);
            } else {
                laux::lua_push(state, value.as_f64().unwrap_or_default());
            }
        }
        serde_json::Value::String(value) => laux::lua_push(state, value),
        serde_json::Value::Array(values) => {
            let table = LuaTable::new(state, values.len(), 0);
            for (index, value) in values.into_iter().enumerate() {
                push_json_value(state, value);
                table.rawseti(index + 1);
            }
        }
        serde_json::Value::Object(values) => {
            let table = LuaTable::new(state, 0, values.len());
            for (key, value) in values {
                table.insert_x(key.as_str(), || push_json_value(state, value));
            }
        }
    }
}

fn process_pg_array_value(
    state: LuaState,
    row_table: &LuaTable,
    column_name: &str,
    type_name: &str,
    value: PgValueRef<'_>,
) -> Result<bool, String> {
    macro_rules! decode_array {
        ($value_type:ty) => {{
            <Vec<Option<$value_type>> as sqlx::decode::Decode<Postgres>>::decode(value)
                .map_err(|err| format!("{} decode error: {}", column_name, err))?
        }};
    }

    match type_name {
        "BOOL[]" => {
            let decoded = decode_array!(bool);
            row_table.insert_x(column_name, || {
                push_optional_array(state, decoded, laux::lua_push)
            });
        }
        "INT2[]" => {
            let decoded = decode_array!(i16);
            row_table.insert_x(column_name, || {
                push_optional_array(state, decoded, |state, value| {
                    laux::lua_push(state, value as i64)
                })
            });
        }
        "INT4[]" => {
            let decoded = decode_array!(i32);
            row_table.insert_x(column_name, || {
                push_optional_array(state, decoded, |state, value| {
                    laux::lua_push(state, value as i64)
                })
            });
        }
        "INT8[]" => {
            let decoded = decode_array!(i64);
            row_table.insert_x(column_name, || {
                push_optional_array(state, decoded, laux::lua_push)
            });
        }
        "FLOAT4[]" => {
            let decoded = decode_array!(f32);
            row_table.insert_x(column_name, || {
                push_optional_array(state, decoded, |state, value| {
                    laux::lua_push(state, value as f64)
                })
            });
        }
        "FLOAT8[]" => {
            let decoded = decode_array!(f64);
            row_table.insert_x(column_name, || {
                push_optional_array(state, decoded, laux::lua_push)
            });
        }
        "TEXT[]" | "VARCHAR[]" | "CHAR[]" | "NAME[]" => {
            let decoded = decode_array!(String);
            row_table.insert_x(column_name, || {
                push_optional_array(state, decoded, laux::lua_push)
            });
        }
        "BYTEA[]" => {
            let decoded = decode_array!(Vec<u8>);
            row_table.insert_x(column_name, || {
                push_optional_array(state, decoded, |state, value| {
                    laux::lua_push(state, value.as_slice())
                })
            });
        }
        "UUID[]" => {
            let decoded = decode_array!(Uuid);
            row_table.insert_x(column_name, || {
                push_optional_array(state, decoded, |state, value| {
                    laux::lua_push(state, value.to_string())
                })
            });
        }
        "JSON[]" | "JSONB[]" => {
            let decoded = decode_array!(Json<serde_json::Value>);
            row_table.insert_x(column_name, || {
                push_optional_array(state, decoded, |state, value| {
                    push_json_value(state, value.0)
                })
            });
        }
        _ => return Ok(false),
    }

    Ok(true)
}

fn insert_pg_scalar_value(
    state: LuaState,
    row_table: &LuaTable,
    column_name: &str,
    db_type: DbType,
    value: PgValueRef<'_>,
) -> Result<(), String> {
    macro_rules! decode_value {
        ($value_type:ty) => {{
            <$value_type as sqlx::decode::Decode<Postgres>>::decode(value)
                .map_err(|err| format!("{} decode error: {}", column_name, err))?
        }};
    }

    match db_type {
        DbType::Int8 => row_table.insert(column_name, decode_value!(i8)),
        DbType::UInt8 => return Err("PostgreSQL has no unsigned integer type".to_string()),
        DbType::Int16 => row_table.insert(column_name, decode_value!(i16)),
        DbType::UInt16 => return Err("PostgreSQL has no unsigned integer type".to_string()),
        DbType::Int32 => row_table.insert(column_name, decode_value!(i32)),
        DbType::UInt32 => return Err("PostgreSQL has no unsigned integer type".to_string()),
        DbType::Int64 => row_table.insert(column_name, decode_value!(i64)),
        DbType::UInt64 => return Err("PostgreSQL has no unsigned integer type".to_string()),
        DbType::Float32 => row_table.insert(column_name, decode_value!(f32)),
        DbType::Float64 => row_table.insert(column_name, decode_value!(f64)),
        DbType::Text => row_table.insert(column_name, decode_value!(&str)),
        DbType::Bool => row_table.insert(column_name, decode_value!(bool)),
        DbType::Timestamp => {
            let value = decode_value!(NaiveDateTime);
            row_table.insert(
                column_name,
                value.format("%Y-%m-%d %H:%M:%S%.f").to_string(),
            )
        }
        DbType::TimestampTz => {
            let value = decode_value!(DateTime<Utc>);
            row_table.insert(
                column_name,
                value.to_rfc3339_opts(SecondsFormat::AutoSi, true),
            )
        }
        DbType::Date => {
            let value = decode_value!(NaiveDate);
            row_table.insert(column_name, value.format("%Y-%m-%d").to_string())
        }
        DbType::Time => {
            let value = decode_value!(NaiveTime);
            row_table.insert(column_name, value.format("%H:%M:%S%.f").to_string())
        }
        DbType::Uuid => row_table.insert(column_name, decode_value!(Uuid).to_string()),
        DbType::Bytes => row_table.insert(column_name, decode_value!(&[u8])),
        DbType::Json => {
            let value = decode_value!(serde_json::Value);
            row_table.insert_x(column_name, || push_json_value(state, value))
        }
        DbType::Null => row_table.insert_x(column_name, || push_sql_null(state)),
        DbType::UnsupportedDecimal => {
            return Err(format!(
                "Unsupported decimal type for column '{}'",
                column_name
            ));
        }
        DbType::TimeTz => {
            let value = decode_value!(PgTimeTz<NaiveTime, FixedOffset>);
            row_table.insert(
                column_name,
                format!("{}{}", value.time.format("%H:%M:%S%.f"), value.offset),
            )
        }
        DbType::Unknown => {
            let bytes = <&[u8] as sqlx::decode::Decode<Postgres>>::decode(value)
                .map_err(|err| format!("{} decode error: {}", column_name, err))?;
            row_table.insert(column_name, bytes)
        }
    };

    Ok(())
}

fn process_pg_rows(state: LuaState, rows: &[PgRow]) -> Result<i32, String> {
    let table = LuaTable::new(state, rows.len(), 0);
    if rows.is_empty() {
        return Ok(1);
    }

    let column_info: Vec<_> = rows[0]
        .columns()
        .iter()
        .enumerate()
        .map(|(index, column)| {
            let type_name = column.type_info().name();
            (
                index,
                column.name(),
                type_name,
                DbType::from_name(type_name),
            )
        })
        .collect();
    ensure_unique_columns(column_info.iter().map(|(_, name, _, _)| *name))?;

    for (row_index, row) in rows.iter().enumerate() {
        let row_table = LuaTable::new(state, 0, row.len());
        for (index, column_name, type_name, db_type) in &column_info {
            let value = row
                .try_get_raw(*index)
                .map_err(|err| format!("{} decode error: {}", column_name, err))?;

            if value.is_null() {
                row_table.insert_x(*column_name, || push_sql_null(state));
                continue;
            }

            if process_pg_array_value(state, &row_table, column_name, type_name, value.clone())? {
                continue;
            }

            insert_pg_scalar_value(state, &row_table, column_name, *db_type, value)?;
        }
        table.rawseti(row_index + 1);
    }

    Ok(1)
}

fn push_connection(state: LuaState, connection: DatabaseConnection) -> i32 {
    let l = [
        lreg!("query", query),
        lreg!("execute", execute),
        lreg!("batch", batch),
        lreg!("transaction", transaction),
        lreg!("close", close),
        lreg_null!(),
    ];
    push_userdata(state, connection, l.as_ref());
    1
}

extern "C-unwind" fn find_connection(state: LuaState) -> i32 {
    let name = checked_string(state, 1);
    let registration = DATABASE_CONNECTIONS
        .get(&name)
        .map(|pair| pair.value().clone());

    if let Some(registration) = registration {
        if !registration.closing.load(Ordering::Acquire)
            && let Some(tx) = registration.tx.upgrade()
        {
            return push_connection(
                state,
                DatabaseConnection {
                    tx,
                    counter: registration.counter,
                    closing: registration.closing,
                    closed: registration.closed,
                },
            );
        }
        DATABASE_CONNECTIONS.remove_if(&name, |_, current| {
            Arc::ptr_eq(&current.identity, &registration.identity)
        });
    }

    laux::lua_pushnil(state);
    1
}

extern "C-unwind" fn decode(state: LuaState) -> i32 {
    laux::lua_checkstack(state, 6, std::ptr::null());
    let response_id = laux::lua_get::<isize>(state, 1);
    let owner = laux::lua_get::<u32>(state, 2);
    let Some((_, result)) = PENDING_RESPONSES.remove_if(&response_id, |_, res| res.owner == owner)
    else {
        push_lua_table!(state,
            "kind" => "ERROR",
            "message" => "invalid, already consumed, or expired SQLx response id"
        );
        return 1;
    };

    match result.value {
        DatabaseResponse::PgRows(rows) => {
            return process_pg_rows(state, &rows)
                .map_err(|e| {
                    push_lua_table!(
                        state,
                        "kind" => "ERROR",
                        "message" => e
                    );
                })
                .unwrap_or(1);
        }
        DatabaseResponse::MysqlRows(rows) => {
            return process_rows::<MySql>(state, &rows)
                .map_err(|e| {
                    push_lua_table!(
                        state,
                        "kind" => "ERROR",
                        "message" => e
                    );
                })
                .unwrap_or(1);
        }
        DatabaseResponse::SqliteRows(rows) => {
            return process_rows::<Sqlite>(state, &rows)
                .map_err(|e| {
                    push_lua_table!(
                        state,
                        "kind" => "ERROR",
                        "message" => e
                    );
                })
                .unwrap_or(1);
        }
        DatabaseResponse::Execute {
            rows_affected,
            last_insert_id,
        } => {
            let table = LuaTable::new(state, 0, 3);
            table.insert("message", "ok");
            table.insert_x("rows_affected", || push_u64(state, rows_affected));
            if let Some(last_insert_id) = last_insert_id {
                table.insert_x("last_insert_id", || push_u64(state, last_insert_id));
            }
            return 1;
        }
        DatabaseResponse::Transaction { rows_affected } => {
            let table = LuaTable::new(state, 0, 2);
            table.insert("message", "ok");
            table.insert_x("rows_affected", || push_u64(state, rows_affected));
            return 1;
        }
        DatabaseResponse::Connect(connection) => return push_connection(state, connection),
        DatabaseResponse::Closed => {
            laux::lua_push(state, true);
            return 1;
        }
        DatabaseResponse::Timeout(message) => {
            push_lua_table!(
                state,
                "kind" => "TIMEOUT",
                "message" => message
            );
        }
        DatabaseResponse::Error(err) => match err.as_database_error() {
            Some(db_err) => {
                let table = LuaTable::new(state, 0, 6);
                table.insert("kind", "DB");
                table.insert("message", db_err.message());
                table.insert("error_kind", format!("{:?}", db_err.kind()));
                if let Some(code) = db_err.code() {
                    table.insert("sqlstate", code.as_ref());
                }
                if let Some(constraint) = db_err.constraint() {
                    table.insert("constraint", constraint);
                }
                if let Some(table_name) = db_err.table() {
                    table.insert("table", table_name);
                }
            }
            None => {
                let kind = match &err {
                    sqlx::Error::Io(_)
                    | sqlx::Error::Tls(_)
                    | sqlx::Error::Protocol(_)
                    | sqlx::Error::PoolTimedOut
                    | sqlx::Error::PoolClosed
                    | sqlx::Error::WorkerCrashed => "SOCKET",
                    _ => "ERROR",
                };
                push_lua_table!(
                    state,
                    "kind" => kind,
                    "message" => err.to_string()
                );
            }
        },
    }

    1
}

extern "C-unwind" fn stats(state: LuaState) -> i32 {
    let table = LuaTable::new(state, 0, DATABASE_CONNECTIONS.len());
    DATABASE_CONNECTIONS.iter().for_each(|pair| {
        table.insert(
            pair.key().as_str(),
            pair.value().counter.load(Ordering::Acquire),
        );
    });
    1
}

#[cfg(feature = "sqlx")]
#[unsafe(no_mangle)]
#[allow(clippy::not_unsafe_ptr_arg_deref)]
pub extern "C-unwind" fn luaopen_rust_sqlx(state: LuaState) -> i32 {
    let l = [
        lreg!("connect", connect),
        lreg!("find_connection", find_connection),
        lreg!("decode", decode),
        lreg!("stats", stats),
        lreg!("make_transaction", make_transaction),
        lreg_null!(),
    ];

    luaL_newlib!(state, l);

    1
}
