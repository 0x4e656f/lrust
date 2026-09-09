use std::collections::HashSet;
use std::ffi::CStr;
use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicI64, Ordering},
};
use std::time::{Duration, Instant};

use dashmap::DashMap;
use futures::TryStreamExt;
use lazy_static::lazy_static;
use sqlx::types::{Json, Uuid};
use sqlx::{
    Column, ColumnIndex, Connection, Database, Executor, MySql, MySqlConnection, PgConnection,
    Postgres, Row, Sqlite, SqliteConnection, TypeInfo, ValueRef,
    mysql::MySqlRow,
    postgres::{PgRow, PgValueFormat, PgValueRef, types::PgTimeTz},
    sqlite::{SqliteConnectOptions, SqliteRow},
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
    lreg, lreg_null, luaL_newlib,
};

use crate::{LOG_LEVEL_ERROR, moon_log};

#[path = "sqlx_retry.rs"]
mod retry;
use retry::{ConnectionLogGate, ReconnectBackoff};

#[path = "sqlx_lua.rs"]
mod lua_safe;
#[path = "sqlx_pg_datetime.rs"]
mod pg_datetime;
#[path = "sqlx_response.rs"]
mod response;
use lua_safe::{OutputTable, TableRead};

// SQLx must not use the common unprotected Lua output helpers while Rust owns
// requests, rows or errors. Keep the error-table macro local for the same reason.
macro_rules! push_lua_table {
    ($state:expr, $( $key:expr => $value:expr ),* ) => {{
        let table = OutputTable::new($state, 0, 6);
        $(table.insert($key, $value);)*
    }};
}

lazy_static! {
    static ref DATABASE_CONNECTIONS: DashMap<String, DatabaseRegistration> = DashMap::new();
    static ref RESPONSES: Arc<response::Registry<DatabaseResponse>> =
        Arc::new(response::Registry::new());
}

// The lease returned with each async submission owns the reply. Its __close
// and __gc abandon it immediately; a late completion never retains row buffers.
// Session IDs are opaque tokens scoped by owner. The Moon host ABI is unchanged.
fn send_response(protocol_type: u8, owner: u32, session: i64, value: DatabaseResponse) {
    if session == 0 || !RESPONSES.publish((owner, session), value) {
        return;
    }
    unsafe extern "C-unwind" {
        fn send_integer_message(type_: u8, receiver: u32, session: i64, val: isize);
    }
    unsafe { send_integer_message(protocol_type, owner, session, session as isize) };
}

type LuaResult = Result<i32, String>;

fn run_entry(state: LuaState, entry: fn(LuaState) -> LuaResult) -> i32 {
    let top = laux::lua_top(state);
    // Do this before acquiring any Rust-owned request resources. Parameter JSON
    // depth is already limited to 64; this reserves its bounded traversal stack.
    laux::lua_checkstack(state, 512, cstr!("sqlx entry"));
    let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| match entry(state) {
        Ok(results) => results,
        Err(error) => {
            unsafe {
                ffi::lua_settop(state.as_ptr(), top);
            }
            push_lua_table!(state, "kind" => "ERROR", "message" => error);
            1
        }
    }));
    let outcome = match outcome {
        Err(payload) if !payload.is::<lua_safe::LuaFailure>() => {
            drop(payload);
            unsafe {
                ffi::lua_settop(state.as_ptr(), top);
            }
            // Rendering a Rust error can itself exhaust the Lua allocator.
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                push_lua_table!(state,
                    "kind" => "ERROR",
                    "message" => "SQLx internal panic; request was not replayed, write outcome may be unknown"
                );
                1
            }))
        }
        outcome => outcome,
    };
    match outcome {
        Ok(results) => results,
        Err(payload) => {
            let on_stack = payload
                .downcast_ref::<lua_safe::LuaFailure>()
                .is_some_and(|failure| failure.error_on_stack);
            drop(payload);
            // All Rust resources (including the panic box) have now dropped.
            // The original Lua memory-error object preserves LUA_ERRMEM.
            unsafe {
                if !on_stack {
                    ffi::lua_pushstring(state.as_ptr(), c"SQLx Lua stack exhausted".as_ptr());
                }
                ffi::lua_error(state.as_ptr())
            }
        }
    }
}

macro_rules! entry_points {
    ($($name:ident => $implementation:ident),* $(,)?) => {
        $(extern "C-unwind" fn $name(state: LuaState) -> i32 {
            run_entry(state, $implementation)
        })*
    };
}

entry_points! {
    connect => connect_impl, query => query_impl, execute => execute_impl,
    batch => batch_impl, transaction => transaction_impl, close => close_impl,
    make_transaction => make_transaction_impl, push_transaction_query => push_transaction_query_impl,
    find_connection => find_connection_impl, decode => decode_impl, stats => stats_impl,
    response_stats => response_stats_impl,
}

// Checked Lua APIs longjmp over Rust destructors on Linux. Validate with the
// non-throwing APIs and propagate Result until all request-owned values drop.
fn checked_integer<T: TryFrom<i64>>(state: LuaState, index: i32) -> Result<T, String> {
    let mut valid = 0;
    let value = unsafe { ffi::lua_tointegerx(state.as_ptr(), index, &mut valid) };
    if valid == 0 {
        return Err(format!("SQLx argument #{index} must be an integer"));
    }
    T::try_from(value).map_err(|_| format!("SQLx argument #{index} is out of range"))
}

fn checked_string(state: LuaState, index: i32) -> Result<String, String> {
    if unsafe { ffi::lua_type(state.as_ptr(), index) } != ffi::LUA_TSTRING {
        return Err(format!("SQLx argument #{index} must be a string"));
    }
    let bytes = laux::lua_to::<&[u8]>(state, index);
    utf8_string(bytes, "SQLx string")
}

fn positive_option(state: LuaState, index: i32, default: i64, max: i64) -> Result<i64, String> {
    bounded_option(state, index, default, 1, max)
}

fn bounded_option(
    state: LuaState,
    index: i32,
    default: i64,
    min: i64,
    max: i64,
) -> Result<i64, String> {
    let value = if unsafe { ffi::lua_isnoneornil(state.as_ptr(), index) } != 0 {
        default
    } else {
        checked_integer::<i64>(state, index)?
    };
    if value < min || value > max {
        return Err(format!(
            "SQLx option #{index} must be between {min} and {max}"
        ));
    }
    Ok(value)
}

trait SqlxUserdata {
    const METATABLE: &'static CStr;
    const CLOSE: bool = false;
    fn metatable_key() -> *const std::ffi::c_void;
}

// Validate type and size before any cast; a Lua caller can pass another
// userdata to a method. Option also makes repeated __gc calls harmless.
fn userdata_ptr<T: SqlxUserdata>(state: LuaState, index: i32) -> Result<*mut Option<T>, String> {
    unsafe { lua_safe::test_userdata::<T>(state, index) }
        .ok_or_else(|| "invalid SQLx userdata type or size".to_string())
}

fn connection_arg(state: LuaState, index: i32) -> Result<DatabaseConnection, String> {
    unsafe { &*userdata_ptr::<DatabaseConnection>(state, index)? }
        .as_ref()
        .cloned()
        .ok_or_else(|| "SQLx connection has been collected".to_string())
}

fn push_userdata<T: SqlxUserdata>(state: LuaState, value: T, lib: &[laux::LuaReg]) {
    lua_safe::userdata(state, value, lib);
}

impl SqlxUserdata for response::Lease<DatabaseResponse> {
    const METATABLE: &'static CStr = c"sqlx_response_lease";
    const CLOSE: bool = true;
    fn metatable_key() -> *const std::ffi::c_void {
        static KEY: u8 = 0;
        (&KEY as *const u8).cast()
    }
}

fn response_guard(state: LuaState, owner: u32, session: i64) -> Result<Option<i32>, String> {
    if session == 0 {
        return Ok(None);
    }
    if session < 0 || isize::try_from(session).is_err() {
        return Err("SQLx session must be a positive native integer".to_string());
    }
    let lease = RESPONSES.register((owner, session))?;
    push_userdata(state, lease, &[lreg_null!()]);
    Ok(Some(laux::lua_top(state)))
}

fn return_session(state: LuaState, session: i64, guard: Option<i32>) -> LuaResult {
    lua_safe::push(state, session);
    if let Some(index) = guard {
        unsafe {
            ffi::lua_pushvalue(state.as_ptr(), index);
        }
        Ok(2)
    } else {
        Ok(1)
    }
}

fn abandon_guard(state: LuaState, guard: Option<i32>) {
    if let Some(index) = guard {
        // This is our own freshly created userdata, not a caller-provided cast.
        unsafe {
            let ptr = ffi::lua_touserdata(state.as_ptr(), index)
                .cast::<Option<response::Lease<DatabaseResponse>>>();
            (*ptr).take();
        }
    }
}

// The FIFO actor exclusively owns a physical connection. Executing via a
// one-slot Pool adds acquire/release health checks to every operation, and a
// cancelled query can leave a pool-return task waiting on the old query.
enum DatabaseBackend {
    MySql(MySqlConnection),
    Postgres(PgConnection),
    Sqlite(SqliteConnection),
}

impl DatabaseBackend {
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
            let connection =
                connect_with_timeout(timeout_duration, MySqlConnection::connect(database_url))
                    .await?;
            Ok(Self::MySql(connection))
        } else if database_url.starts_with("postgres://")
            || database_url.starts_with("postgresql://")
        {
            let connection =
                connect_with_timeout(timeout_duration, PgConnection::connect(database_url)).await?;
            Ok(Self::Postgres(connection))
        } else if database_url.starts_with("sqlite:") {
            let connection = connect_with_timeout(timeout_duration, async {
                let options = database_url
                    .parse::<SqliteConnectOptions>()?
                    .create_if_missing(true);
                SqliteConnection::connect_with(&options).await
            })
            .await?;
            Ok(Self::Sqlite(connection))
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
        &mut self,
        request: &DatabaseQuery,
        max_rows: usize,
    ) -> Result<DatabaseResponse, sqlx::Error> {
        match self {
            Self::MySql(connection) => {
                let query = Self::make_query(&request.sql, &request.binds)?;
                let mut stream = query.fetch(connection);
                let mut rows = Vec::new();
                while let Some(row) = stream.try_next().await? {
                    if rows.len() >= max_rows {
                        return Ok(DatabaseResponse::RowLimitExceeded(max_rows));
                    }
                    rows.push(row);
                }
                Ok(DatabaseResponse::MysqlRows(rows))
            }
            Self::Postgres(connection) => {
                let query = Self::make_pg_query(&request.sql, &request.binds)?;
                let mut stream = query.fetch(connection);
                let mut rows = Vec::new();
                while let Some(row) = stream.try_next().await? {
                    if rows.len() >= max_rows {
                        return Ok(DatabaseResponse::RowLimitExceeded(max_rows));
                    }
                    rows.push(row);
                }
                Ok(DatabaseResponse::PgRows(rows))
            }
            Self::Sqlite(connection) => {
                let query = Self::make_query(&request.sql, &request.binds)?;
                let mut stream = query.fetch(connection);
                let mut rows = Vec::new();
                while let Some(row) = stream.try_next().await? {
                    if rows.len() >= max_rows {
                        return Ok(DatabaseResponse::RowLimitExceeded(max_rows));
                    }
                    rows.push(row);
                }
                Ok(DatabaseResponse::SqliteRows(rows))
            }
        }
    }

    async fn execute(&mut self, request: &DatabaseQuery) -> Result<DatabaseResponse, sqlx::Error> {
        match self {
            Self::MySql(connection) => {
                let result = Self::make_query(&request.sql, &request.binds)?
                    .execute(connection)
                    .await?;
                Ok(DatabaseResponse::Execute {
                    rows_affected: result.rows_affected(),
                    last_insert_id: Some(result.last_insert_id()),
                })
            }
            Self::Postgres(connection) => {
                let result = Self::make_pg_query(&request.sql, &request.binds)?
                    .execute(connection)
                    .await?;
                Ok(DatabaseResponse::Execute {
                    rows_affected: result.rows_affected(),
                    last_insert_id: None,
                })
            }
            Self::Sqlite(connection) => {
                let result = Self::make_query(&request.sql, &request.binds)?
                    .execute(connection)
                    .await?;
                Ok(DatabaseResponse::Execute {
                    rows_affected: result.rows_affected(),
                    last_insert_id: u64::try_from(result.last_insert_rowid()).ok(),
                })
            }
        }
    }

    async fn batch(&mut self, sql: &str) -> Result<DatabaseResponse, sqlx::Error> {
        match self {
            Self::MySql(connection) => {
                let result = connection.execute(sqlx::raw_sql(sql)).await?;
                Ok(DatabaseResponse::Execute {
                    rows_affected: result.rows_affected(),
                    last_insert_id: Some(result.last_insert_id()),
                })
            }
            Self::Postgres(connection) => {
                let result = connection.execute(sqlx::raw_sql(sql)).await?;
                Ok(DatabaseResponse::Execute {
                    rows_affected: result.rows_affected(),
                    last_insert_id: None,
                })
            }
            Self::Sqlite(connection) => {
                let result = connection.execute(sqlx::raw_sql(sql)).await?;
                Ok(DatabaseResponse::Execute {
                    rows_affected: result.rows_affected(),
                    last_insert_id: u64::try_from(result.last_insert_rowid()).ok(),
                })
            }
        }
    }

    async fn transaction(
        &mut self,
        requests: &[DatabaseQuery],
    ) -> Result<DatabaseResponse, sqlx::Error> {
        match self {
            Self::MySql(connection) => {
                let mut transaction = connection.begin().await?;
                let executed = async {
                    let mut rows_affected = 0u64;
                    for request in requests {
                        let query = Self::make_query(&request.sql, &request.binds)?;
                        rows_affected = rows_affected.saturating_add(
                            query.execute(&mut *transaction).await?.rows_affected(),
                        );
                    }
                    Ok::<_, sqlx::Error>(rows_affected)
                }
                .await;
                let rows_affected = match executed {
                    Ok(rows) => rows,
                    Err(error) => {
                        // Drop only queues ROLLBACK. Flush and await it before
                        // reporting failure, or an idle actor would retain locks.
                        if let Err(error) = transaction.rollback().await {
                            return Ok(DatabaseResponse::DisconnectedError(error));
                        }
                        return Err(error);
                    }
                };
                if let Err(error) = transaction.commit().await {
                    return Ok(DatabaseResponse::DisconnectedError(error));
                }
                Ok(DatabaseResponse::Transaction { rows_affected })
            }
            Self::Postgres(connection) => {
                let mut transaction = connection.begin().await?;
                let executed = async {
                    let mut rows_affected = 0u64;
                    for request in requests {
                        let query = Self::make_pg_query(&request.sql, &request.binds)?;
                        rows_affected = rows_affected.saturating_add(
                            query.execute(&mut *transaction).await?.rows_affected(),
                        );
                    }
                    Ok::<_, sqlx::Error>(rows_affected)
                }
                .await;
                let rows_affected = match executed {
                    Ok(rows) => rows,
                    Err(error) => {
                        // Drop only queues ROLLBACK. Flush and await it before
                        // reporting failure, or an idle actor would retain locks.
                        if let Err(error) = transaction.rollback().await {
                            return Ok(DatabaseResponse::DisconnectedError(error));
                        }
                        return Err(error);
                    }
                };
                if let Err(error) = transaction.commit().await {
                    return Ok(DatabaseResponse::DisconnectedError(error));
                }
                Ok(DatabaseResponse::Transaction { rows_affected })
            }
            Self::Sqlite(connection) => {
                let mut transaction = connection.begin().await?;
                let executed = async {
                    let mut rows_affected = 0u64;
                    for request in requests {
                        let query = Self::make_query(&request.sql, &request.binds)?;
                        rows_affected = rows_affected.saturating_add(
                            query.execute(&mut *transaction).await?.rows_affected(),
                        );
                    }
                    Ok::<_, sqlx::Error>(rows_affected)
                }
                .await;
                let rows_affected = match executed {
                    Ok(rows) => rows,
                    Err(error) => {
                        // Drop only queues ROLLBACK. Flush and await it before
                        // reporting failure, or an idle actor would retain locks.
                        if let Err(error) = transaction.rollback().await {
                            return Ok(DatabaseResponse::DisconnectedError(error));
                        }
                        return Err(error);
                    }
                };
                if let Err(error) = transaction.commit().await {
                    return Ok(DatabaseResponse::DisconnectedError(error));
                }
                Ok(DatabaseResponse::Transaction { rows_affected })
            }
        }
    }

    async fn close(self) {
        // No Pool or detached pool-return tasks survive this future. Dropping
        // the timed-out close future drops the owned transport as well.
        let _ = timeout(Duration::from_secs(1), async move {
            match self {
                Self::MySql(connection) => connection.close().await,
                Self::Postgres(connection) => connection.close().await,
                Self::Sqlite(connection) => connection.close().await,
            }
        })
        .await;
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
    fn metatable_key() -> *const std::ffi::c_void {
        static KEY: u8 = 0;
        (&KEY as *const u8).cast()
    }
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
    database_url: String,
    connect_timeout: Duration,
    identity: Arc<()>,
    counter: Arc<AtomicI64>,
    request_timeout: Duration,
    max_rows: usize,
    reconnect_initial_delay: Duration,
    reconnect_max_delay: Duration,
    reconnect_log_interval: Duration,
    closed: watch::Sender<bool>,
}

enum DatabaseResponse {
    Connect(DatabaseConnection),
    Closed,
    PgRows(Vec<PgRow>),
    MysqlRows(Vec<MySqlRow>),
    SqliteRows(Vec<SqliteRow>),
    Error(sqlx::Error),
    ConnectFailed {
        error: sqlx::Error,
        retry_after_ms: u64,
    },
    ReconnectCooldown(u64),
    // Preserve the original error metadata while retiring a connection whose
    // transaction commit/rollback did not complete successfully.
    DisconnectedError(sqlx::Error),
    Timeout(String),
    RowLimitExceeded(usize),
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

fn connection_error(error: &sqlx::Error) -> bool {
    matches!(
        error,
        sqlx::Error::Io(_)
            | sqlx::Error::Tls(_)
            | sqlx::Error::Protocol(_)
            | sqlx::Error::PoolTimedOut
            | sqlx::Error::PoolClosed
            | sqlx::Error::WorkerCrashed
    )
}

impl DatabaseResponse {
    fn requires_disconnect(&self) -> bool {
        match self {
            // A cancelled/partially consumed operation must never reuse its
            // connection or defer draining its result to the next request.
            Self::Timeout(_) | Self::RowLimitExceeded(_) | Self::DisconnectedError(_) => true,
            Self::Error(error) => {
                connection_error(error)
                    || error
                        .as_database_error()
                        .and_then(|error| error.code())
                        .is_some_and(|code| {
                            code.starts_with("08")
                                || matches!(code.as_ref(), "57P01" | "57P02" | "57P03")
                        })
            }
            _ => false,
        }
    }
}

fn finish_request(
    context: &DatabaseHandlerContext,
    owner: u32,
    session: i64,
    response: DatabaseResponse,
    connection_logs: &mut ConnectionLogGate,
) {
    context.counter.fetch_sub(1, Ordering::AcqRel);

    if session == 0 {
        let suppressed = if matches!(
            response,
            DatabaseResponse::ConnectFailed { .. } | DatabaseResponse::ReconnectCooldown(_)
        ) {
            let Some(count) = connection_logs.take(Instant::now()) else {
                return;
            };
            count
        } else {
            0
        };
        let message = match &response {
            DatabaseResponse::Error(err) | DatabaseResponse::DisconnectedError(err) => {
                Some(err.to_string())
            }
            DatabaseResponse::Timeout(message) => Some(message.clone()),
            DatabaseResponse::ConnectFailed {
                error,
                retry_after_ms,
            } => Some(format!(
                "connection failed: {error}; retry after {retry_after_ms} ms (SQL not submitted)"
            )),
            DatabaseResponse::ReconnectCooldown(ms) => Some(format!(
                "reconnect cooldown: retry after {ms} ms (SQL not submitted)"
            )),
            DatabaseResponse::RowLimitExceeded(max_rows) => {
                Some(format!("query result exceeded max_rows ({max_rows})"))
            }
            _ => None,
        };
        if let Some(message) = message {
            moon_log(
                owner,
                LOG_LEVEL_ERROR,
                format!(
                    "Database '{}' error: {message}; suppressed connection errors: {suppressed}",
                    context.connection_name
                ),
            );
        }
        return;
    }

    send_response(context.protocol_type, owner, session, response);
}

async fn database_handler(
    backend: DatabaseBackend,
    mut rx: mpsc::Receiver<DatabaseRequest>,
    context: DatabaseHandlerContext,
) {
    let mut backend = Some(backend);
    let mut reconnect =
        ReconnectBackoff::new(context.reconnect_initial_delay, context.reconnect_max_delay);
    let mut connection_logs = ConnectionLogGate::new(context.reconnect_log_interval);
    while let Some(op) = rx.recv().await {
        let (owner, session, operation) = match &op {
            DatabaseRequest::Query(owner, session, _) => (*owner, *session, "query"),
            DatabaseRequest::Execute(owner, session, _) => (*owner, *session, "execute"),
            DatabaseRequest::Batch(owner, session, _) => (*owner, *session, "batch"),
            DatabaseRequest::Transaction(owner, session, _) => (*owner, *session, "transaction"),
            DatabaseRequest::Close => {
                // Stop later sends, but drain every already accepted request.
                rx.close();
                continue;
            }
        };
        let retry_after_ms = reconnect.retry_after_ms(Instant::now());
        if backend.is_none() && retry_after_ms > 0 {
            finish_request(
                &context,
                owner,
                session,
                DatabaseResponse::ReconnectCooldown(retry_after_ms),
                &mut connection_logs,
            );
            continue;
        }
        // This flag also catches the outer request deadline expiring during
        // a handshake. Ordinary SQL timeouts must not activate connect backoff.
        let mut connecting = backend.is_none();
        let result = timeout(context.request_timeout, async {
            let connection = match backend {
                Some(ref mut connection) => connection,
                None => backend.insert(
                    DatabaseBackend::connect(&context.database_url, context.connect_timeout)
                        .await?,
                ),
            };
            connecting = false;
            reconnect.reset();
            match &op {
                DatabaseRequest::Query(_, _, query) => {
                    connection.query(query, context.max_rows).await
                }
                DatabaseRequest::Execute(_, _, query) => connection.execute(query).await,
                DatabaseRequest::Batch(_, _, sql) => connection.batch(sql).await,
                DatabaseRequest::Transaction(_, _, queries) => {
                    connection.transaction(queries).await
                }
                DatabaseRequest::Close => unreachable!("close is handled before execution"),
            }
        })
        .await;
        let response = if connecting {
            reconnect.failed(Instant::now());
            let error = match result {
                Ok(Err(error)) => error,
                Err(_) => sqlx::Error::Io(std::io::Error::new(
                    std::io::ErrorKind::TimedOut,
                    "request timed out while reconnecting",
                )),
                Ok(Ok(_)) => unreachable!("successful reconnect clears connecting"),
            };
            DatabaseResponse::ConnectFailed {
                error,
                retry_after_ms: reconnect.retry_after_ms(Instant::now()),
            }
        } else {
            match result {
                Ok(Ok(response)) => response,
                Ok(Err(error)) => DatabaseResponse::Error(error),
                Err(_) => DatabaseResponse::Timeout(format!(
                    "{operation} timed out after {} ms",
                    context.request_timeout.as_millis(),
                )),
            }
        };
        if response.requires_disconnect() {
            // Drop the raw transport without awaiting protocol cleanup. Do not
            // retry this operation: a lost response may hide a committed write.
            // Only a later FIFO request may open a new connection.
            drop(backend.take());
        }
        finish_request(&context, owner, session, response, &mut connection_logs);
    }

    if let Some(backend) = backend {
        backend.close().await;
    }
    DATABASE_CONNECTIONS.remove_if(&context.connection_name, |_, current| {
        Arc::ptr_eq(&current.identity, &context.identity)
    });
    let _ = context.closed.send(true);
}

fn connect_impl(state: LuaState) -> LuaResult {
    let protocol_type: u8 = checked_integer(state, 1)?;
    let owner = checked_integer(state, 2)?;
    let session: i64 = checked_integer(state, 3)?;

    // The async task outlives this Lua call, so never retain references into
    // the Lua stack here.
    let database_url = checked_string(state, 4)?;
    let name = checked_string(state, 5)?;
    let connect_timeout = positive_option(state, 6, 5000, u32::MAX as i64)? as u64;
    let request_timeout_ms = positive_option(state, 7, 30000, u32::MAX as i64)? as u64;
    let max_rows = positive_option(state, 8, 100000, i32::MAX as i64)? as usize;
    let queue_capacity = positive_option(state, 9, 100, i32::MAX as i64)? as usize;
    let reconnect_initial_delay = bounded_option(state, 10, 250, 0, u32::MAX as i64)? as u64;
    let reconnect_max_delay = positive_option(state, 11, 5000, u32::MAX as i64)? as u64;
    let reconnect_log_interval = bounded_option(state, 12, 5000, 0, u32::MAX as i64)? as u64;
    if reconnect_max_delay < reconnect_initial_delay {
        return Err("SQLx reconnect_max_delay must be >= reconnect_initial_delay".to_string());
    }

    let guard = response_guard(state, owner, session)?;
    CONTEXT.tokio_runtime.spawn(async move {
        match DatabaseBackend::connect(&database_url, Duration::from_millis(connect_timeout)).await
        {
            Ok(backend) => {
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
                    backend,
                    rx,
                    DatabaseHandlerContext {
                        protocol_type,
                        connection_name: name,
                        database_url,
                        connect_timeout: Duration::from_millis(connect_timeout),
                        identity,
                        counter,
                        request_timeout: Duration::from_millis(request_timeout_ms),
                        max_rows,
                        reconnect_initial_delay: Duration::from_millis(reconnect_initial_delay),
                        reconnect_max_delay: Duration::from_millis(reconnect_max_delay),
                        reconnect_log_interval: Duration::from_millis(reconnect_log_interval),
                        closed: closed_tx,
                    },
                )
                .await;
            }
            Err(err) => {
                // Explicit connect is always a single attempt; no global/name
                // cooldown map. Only an existing actor remembers failures.
                send_response(
                    protocol_type,
                    owner,
                    session,
                    DatabaseResponse::ConnectFailed {
                        error: err,
                        retry_after_ms: 0,
                    },
                );
            }
        };
    });

    return_session(state, session, guard)
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

fn get_typed_query_param(wrapper: &LuaTable) -> Result<Option<QueryParams>, String> {
    let kind = {
        let marker = wrapper.raw_get("__sqlx_param");
        match &marker.value {
            LuaValue::Nil | LuaValue::None => return Ok(None),
            LuaValue::String(value) => utf8_string(value, "sqlx parameter kind")?,
            _ => return Err("sqlx parameter kind must be a string".to_string()),
        }
    };

    match kind.as_str() {
        "null" => {
            let type_name = {
                let field = wrapper.raw_get("type");
                match &field.value {
                    LuaValue::Nil | LuaValue::None => "text".to_string(),
                    LuaValue::String(value) => utf8_string(value, "sqlx.null type")?,
                    _ => return Err("sqlx.null type must be a string".to_string()),
                }
            };
            Ok(Some(QueryParams::Null(parse_null_type(&type_name)?)))
        }
        "text" => {
            let field = wrapper.raw_get("value");
            match &field.value {
                LuaValue::String(value) => Ok(Some(QueryParams::Text(utf8_string(
                    value,
                    "sqlx.text value",
                )?))),
                _ => Err("sqlx.text value must be a string".to_string()),
            }
        }
        "bytes" => {
            let field = wrapper.raw_get("value");
            match &field.value {
                LuaValue::String(value) => Ok(Some(QueryParams::Bytes(value.to_vec()))),
                _ => Err("sqlx.bytes value must be a string".to_string()),
            }
        }
        "json" => {
            let field = wrapper.raw_get("value");
            lua_value_to_json(&field.value, 0)
                .map(QueryParams::Json)
                .map(Some)
        }
        _ => Err(format!("unknown sqlx parameter kind: {kind}")),
    }
}

fn get_query_param(state: LuaState, i: i32) -> Result<QueryParams, String> {
    let res = match LuaValue::from_stack(state, i) {
        LuaValue::Nil => QueryParams::Null(NullType::Text),
        LuaValue::LightUserData(ptr) if ptr.is_null() => QueryParams::Null(NullType::Text),
        LuaValue::Boolean(val) => QueryParams::Bool(val),
        LuaValue::Number(val) => QueryParams::Float(val),
        LuaValue::Integer(val) => QueryParams::Int(val),
        LuaValue::String(val) => QueryParams::Text(utf8_string(val, "SQL text parameter")?),
        LuaValue::Table(val) => {
            if let Some(param) = get_typed_query_param(&val)? {
                return Ok(param);
            }

            let is_array_wrapper = {
                let marker = val.raw_get("__sqlx_array");
                matches!(&marker.value, LuaValue::Boolean(true))
            } || val.meta_field(c"__sqlx_array").is_some();
            if is_array_wrapper {
                return get_pg_array_param(&val);
            }

            QueryParams::Json(lua_table_to_json(&val, 0)?)
        }
        _t => {
            return Err(format!(
                "get_query_param: unsupport value type :{}",
                laux::type_name(state, unsafe { ffi::lua_type(state.as_ptr(), i) })
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
    let (is_array, len) = values.array_shape();
    if !is_array && values.pairs().next().is_some() {
        return Err("sqlx.array values must be a Lua sequence".to_string());
    }

    let mut result = Vec::with_capacity(len);
    for (index, value) in values.values(len).enumerate() {
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

// Build owned JSON while the Lua stack is valid. SQLx performs the only text
// serialization later, without an intermediate JSON buffer and parsing pass.
fn lua_value_to_json(value: &LuaValue<'_>, depth: usize) -> Result<serde_json::Value, String> {
    match value {
        LuaValue::Nil => Ok(serde_json::Value::Null),
        LuaValue::LightUserData(ptr) if ptr.is_null() => Ok(serde_json::Value::Null),
        LuaValue::Boolean(value) => Ok(serde_json::Value::Bool(*value)),
        LuaValue::Integer(value) => Ok(serde_json::Value::Number((*value).into())),
        LuaValue::Number(value) => serde_json::Number::from_f64(*value)
            .map(serde_json::Value::Number)
            .ok_or_else(|| "SQL JSON number must be finite".to_string()),
        LuaValue::String(value) => {
            utf8_string(value, "SQL JSON string").map(serde_json::Value::String)
        }
        LuaValue::Table(value) => lua_table_to_json(value, depth),
        _ => Err(format!("SQL JSON unsupported value type: {}", value.name())),
    }
}

fn lua_table_to_json(table: &LuaTable, depth: usize) -> Result<serde_json::Value, String> {
    if depth >= 64 {
        return Err("SQL JSON table nesting exceeds 64 levels (possibly cyclic)".to_string());
    }
    if unsafe { ffi::lua_checkstack(table.lua_state().as_ptr(), 6) } == 0 {
        return Err("SQLx JSON traversal exhausted Lua stack".to_string());
    }
    let (is_array, len) = table.array_shape();
    if is_array {
        let mut values = Vec::with_capacity(len);
        for value in table.values(len) {
            values.push(lua_value_to_json(&value, depth + 1)?);
        }
        return Ok(serde_json::Value::Array(values));
    }
    let mut values = serde_json::Map::new();
    for (key, value) in table.pairs() {
        let key = match key {
            LuaValue::String(key) => utf8_string(key, "SQL JSON object key")?,
            LuaValue::Integer(key) => key.to_string(),
            _ => return Err("SQL JSON object keys must be strings or integers".to_string()),
        };
        // Match the former parser: stringified duplicate keys keep the last
        // value in Lua iteration order. Serde safely escapes object keys.
        values.insert(key, lua_value_to_json(&value, depth + 1)?);
    }
    // Preserve SQLx's fixed defaults: empty tables are arrays and sparse array
    // slots become JSON null, independent of the public JSON module's options.
    if values.is_empty() {
        Ok(serde_json::Value::Array(Vec::new()))
    } else {
        Ok(serde_json::Value::Object(values))
    }
}

fn get_pg_array_param(wrapper: &LuaTable) -> Result<QueryParams, String> {
    let type_name = {
        let field = wrapper.raw_get("type");
        match &field.value {
            LuaValue::String(value) => utf8_string(value, "sqlx.array type")?,
            _ => return Err("sqlx.array type must be a string".to_string()),
        }
    };

    let values_field = wrapper.raw_get("values");
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
                lua_value_to_json(&value, 0).ok().map(Json)
            })?;
            Ok(QueryParams::PgJsonArray(values))
        }
        _ => Err(format!(
            "sqlx.array unsupported PostgreSQL element type: {}",
            type_name
        )),
    }
}

fn enqueue_query(state: LuaState, execute_only: bool) -> LuaResult {
    let mut args = LuaArgs::new(1);
    let conn = connection_arg(state, args.iter_arg())?;

    let owner = checked_integer(state, args.iter_arg())?;
    let session = checked_integer(state, args.iter_arg())?;

    if conn.closing.load(Ordering::Acquire) {
        push_lua_table!(
            state,
            "kind" => "CLOSED",
            "message" => "database connection is closing"
        );
        return Ok(1);
    }

    let sql = checked_string(state, args.iter_arg())?;
    let mut params = Vec::new();
    let top = laux::lua_top(state);
    for i in args.iter_arg()..=top {
        let param = get_query_param(state, i);
        match param {
            Ok(value) => {
                params.push(value);
            }
            Err(err) => {
                return Err(err);
            }
        }
    }

    let query = DatabaseQuery { sql, binds: params };
    let request = if execute_only {
        DatabaseRequest::Execute(owner, session, query)
    } else {
        DatabaseRequest::Query(owner, session, query)
    };

    let guard = response_guard(state, owner, session)?;
    conn.counter.fetch_add(1, Ordering::AcqRel);
    match conn.tx.try_send(request) {
        Ok(_) => return_session(state, session, guard),
        Err(err) => {
            abandon_guard(state, guard);
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
            Ok(1)
        }
    }
}

fn query_impl(state: LuaState) -> LuaResult {
    enqueue_query(state, false)
}

fn execute_impl(state: LuaState) -> LuaResult {
    enqueue_query(state, true)
}

fn batch_impl(state: LuaState) -> LuaResult {
    let mut args = LuaArgs::new(1);
    let conn = connection_arg(state, args.iter_arg())?;
    let owner = checked_integer(state, args.iter_arg())?;
    let session = checked_integer(state, args.iter_arg())?;
    let sql = checked_string(state, args.iter_arg())?;

    if conn.closing.load(Ordering::Acquire) {
        push_lua_table!(
            state,
            "kind" => "CLOSED",
            "message" => "database connection is closing"
        );
        return Ok(1);
    }

    let guard = response_guard(state, owner, session)?;
    conn.counter.fetch_add(1, Ordering::AcqRel);
    match conn
        .tx
        .try_send(DatabaseRequest::Batch(owner, session, sql))
    {
        Ok(_) => return return_session(state, session, guard),
        Err(err) => {
            abandon_guard(state, guard);
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
    Ok(1)
}

struct TransactionQuerys {
    querys: Vec<DatabaseQuery>,
}

impl SqlxUserdata for TransactionQuerys {
    const METATABLE: &'static CStr = c"sqlx_transaction_metatable";
    fn metatable_key() -> *const std::ffi::c_void {
        static KEY: u8 = 0;
        (&KEY as *const u8).cast()
    }
}

fn push_transaction_query_impl(state: LuaState) -> LuaResult {
    // Validate now, but don't hold a mutable borrow across parameter encoding
    // (Lua metamethods can re-enter this function).
    userdata_ptr::<TransactionQuerys>(state, 1)?;
    let sql = checked_string(state, 2)?;
    let mut params = Vec::new();
    let top = laux::lua_top(state);
    for i in 3..=top {
        let param = get_query_param(state, i);
        match param {
            Ok(value) => {
                params.push(value);
            }
            Err(err) => {
                return Err(err);
            }
        }
    }

    let querys = unsafe { &mut *userdata_ptr::<TransactionQuerys>(state, 1)? };
    let querys = querys
        .as_mut()
        .ok_or_else(|| "SQLx transaction has been collected".to_string())?;
    querys.querys.push(DatabaseQuery { sql, binds: params });

    Ok(0)
}

fn make_transaction_impl(state: LuaState) -> LuaResult {
    push_userdata(
        state,
        TransactionQuerys { querys: Vec::new() },
        &[lreg!("push", push_transaction_query), lreg_null!()],
    );
    Ok(1)
}

fn transaction_impl(state: LuaState) -> LuaResult {
    let mut args = LuaArgs::new(1);
    let conn = connection_arg(state, args.iter_arg())?;

    let owner = checked_integer(state, args.iter_arg())?;
    let session = checked_integer(state, args.iter_arg())?;

    let querys_ptr = userdata_ptr::<TransactionQuerys>(state, args.iter_arg())?;
    if unsafe { &*querys_ptr }.is_none() {
        return Err("SQLx transaction has been collected".to_string());
    }

    if conn.closing.load(Ordering::Acquire) {
        push_lua_table!(
            state,
            "kind" => "CLOSED",
            "message" => "database connection is closing"
        );
        return Ok(1);
    }

    let guard = response_guard(state, owner, session)?;
    // Allocating the response lease can run Lua GC/finalizers. Never hold a
    // mutable userdata reference across it, and recheck after that allocation.
    let requests = match unsafe { &mut *querys_ptr } {
        Some(querys) => std::mem::take(&mut querys.querys),
        None => {
            abandon_guard(state, guard);
            return Err("SQLx transaction has been collected".to_string());
        }
    };
    let request = DatabaseRequest::Transaction(owner, session, requests);
    conn.counter.fetch_add(1, Ordering::AcqRel);
    match conn.tx.try_send(request) {
        Ok(_) => return_session(state, session, guard),
        Err(err) => {
            abandon_guard(state, guard);
            conn.counter.fetch_sub(1, Ordering::AcqRel);
            let kind = if matches!(&err, mpsc::error::TrySendError::Full(_)) {
                "BUSY"
            } else {
                "CLOSED"
            };
            let message = err.to_string();
            if let DatabaseRequest::Transaction(_, _, requests) = err.into_inner() {
                // No Lua operation between take(), try_send() and this restore.
                if let Some(querys) = unsafe { &mut *querys_ptr } {
                    querys.querys = requests;
                }
            }
            push_lua_table!(
                state,
                "kind" => kind,
                "message" => message
            );
            Ok(1)
        }
    }
}

fn close_impl(state: LuaState) -> LuaResult {
    let conn = connection_arg(state, 1)?;
    let protocol_type: u8 = checked_integer(state, 2)?;
    let owner: u32 = checked_integer(state, 3)?;
    let session: i64 = checked_integer(state, 4)?;

    let guard = response_guard(state, owner, session)?;
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

    return_session(state, session, guard)
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
        lua_safe::push(state, value);
    } else {
        lua_safe::push(state, value.to_string());
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
    let table = OutputTable::new(state, rows.len(), 0);
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
        let row_table = OutputTable::new(state, 0, row.len());
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
    let table = OutputTable::new(state, values.len(), 0);
    for (index, value) in values.into_iter().enumerate() {
        match value {
            Some(value) => push_value(state, value),
            None => laux::lua_pushlightuserdata(state, std::ptr::null_mut()),
        }
        table.rawseti(index + 1);
    }
}

fn push_json_value(state: LuaState, value: serde_json::Value) {
    lua_safe::checkstack(state, 4);
    match value {
        serde_json::Value::Null => {
            laux::lua_pushlightuserdata(state, std::ptr::null_mut());
        }
        serde_json::Value::Bool(value) => lua_safe::push(state, value),
        serde_json::Value::Number(value) => {
            if let Some(value) = value.as_i64() {
                lua_safe::push(state, value);
            } else if let Some(value) = value.as_u64() {
                push_u64(state, value);
            } else {
                lua_safe::push(state, value.as_f64().unwrap_or_default());
            }
        }
        serde_json::Value::String(value) => lua_safe::push(state, value),
        serde_json::Value::Array(values) => {
            let table = OutputTable::new(state, values.len(), 0);
            for (index, value) in values.into_iter().enumerate() {
                push_json_value(state, value);
                table.rawseti(index + 1);
            }
        }
        serde_json::Value::Object(values) => {
            let table = OutputTable::new(state, 0, values.len());
            for (key, value) in values {
                table.insert_x(key.as_str(), || push_json_value(state, value));
            }
        }
    }
}

fn process_pg_array_value(
    state: LuaState,
    row_table: &OutputTable,
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
                push_optional_array(state, decoded, lua_safe::push)
            });
        }
        "INT2[]" => {
            let decoded = decode_array!(i16);
            row_table.insert_x(column_name, || {
                push_optional_array(state, decoded, |state, value| {
                    lua_safe::push(state, value as i64)
                })
            });
        }
        "INT4[]" => {
            let decoded = decode_array!(i32);
            row_table.insert_x(column_name, || {
                push_optional_array(state, decoded, |state, value| {
                    lua_safe::push(state, value as i64)
                })
            });
        }
        "INT8[]" => {
            let decoded = decode_array!(i64);
            row_table.insert_x(column_name, || {
                push_optional_array(state, decoded, lua_safe::push)
            });
        }
        "FLOAT4[]" => {
            let decoded = decode_array!(f32);
            row_table.insert_x(column_name, || {
                push_optional_array(state, decoded, |state, value| {
                    lua_safe::push(state, value as f64)
                })
            });
        }
        "FLOAT8[]" => {
            let decoded = decode_array!(f64);
            row_table.insert_x(column_name, || {
                push_optional_array(state, decoded, lua_safe::push)
            });
        }
        "TEXT[]" | "VARCHAR[]" | "CHAR[]" | "NAME[]" => {
            let decoded = decode_array!(String);
            row_table.insert_x(column_name, || {
                push_optional_array(state, decoded, lua_safe::push)
            });
        }
        "BYTEA[]" => {
            let decoded = decode_array!(Vec<u8>);
            row_table.insert_x(column_name, || {
                push_optional_array(state, decoded, |state, value| {
                    lua_safe::push(state, value.as_slice())
                })
            });
        }
        "UUID[]" => {
            let decoded = decode_array!(Uuid);
            row_table.insert_x(column_name, || {
                push_optional_array(state, decoded, |state, value| {
                    lua_safe::push(state, value.to_string())
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

fn decode_pg_temporal(value: PgValueRef<'_>, db_type: DbType) -> Result<String, String> {
    let bytes = <&[u8] as sqlx::decode::Decode<Postgres>>::decode(value.clone())
        .map_err(|err| err.to_string())?;
    if value.format() == PgValueFormat::Text {
        return utf8_string(bytes, "PostgreSQL temporal value");
    }
    let int8 = |bytes: &[u8]| -> Result<i64, String> {
        bytes
            .try_into()
            .map(i64::from_be_bytes)
            .map_err(|_| "invalid PostgreSQL temporal int8 length".to_string())
    };
    let int4 = |bytes: &[u8]| -> Result<i32, String> {
        bytes
            .try_into()
            .map(i32::from_be_bytes)
            .map_err(|_| "invalid PostgreSQL temporal int4 length".to_string())
    };
    match db_type {
        DbType::Date => Ok(pg_datetime::date(int4(bytes)?)),
        DbType::Timestamp => pg_datetime::timestamp(int8(bytes)?, false),
        DbType::TimestampTz => pg_datetime::timestamp(int8(bytes)?, true),
        DbType::Time => pg_datetime::time(int8(bytes)?),
        DbType::TimeTz if bytes.len() == 12 => {
            pg_datetime::timetz(int8(&bytes[..8])?, int4(&bytes[8..])?)
        }
        _ => Err("invalid PostgreSQL temporal type or length".to_string()),
    }
}

fn insert_pg_scalar_value(
    state: LuaState,
    row_table: &OutputTable,
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
        DbType::Timestamp | DbType::TimestampTz | DbType::Date | DbType::Time | DbType::TimeTz => {
            let value = decode_pg_temporal(value, db_type)
                .map_err(|err| format!("{column_name} decode error: {err}"))?;
            row_table.insert(column_name, value)
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
        DbType::Unknown => {
            let bytes = <&[u8] as sqlx::decode::Decode<Postgres>>::decode(value)
                .map_err(|err| format!("{} decode error: {}", column_name, err))?;
            row_table.insert(column_name, bytes)
        }
    };

    Ok(())
}

fn process_pg_rows(state: LuaState, rows: &[PgRow]) -> Result<i32, String> {
    let table = OutputTable::new(state, rows.len(), 0);
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
        let row_table = OutputTable::new(state, 0, row.len());
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

fn find_connection_impl(state: LuaState) -> LuaResult {
    let name = checked_string(state, 1)?;
    let registration = DATABASE_CONNECTIONS
        .get(&name)
        .map(|pair| pair.value().clone());

    if let Some(registration) = registration {
        if !registration.closing.load(Ordering::Acquire)
            && let Some(tx) = registration.tx.upgrade()
        {
            return Ok(push_connection(
                state,
                DatabaseConnection {
                    tx,
                    counter: registration.counter,
                    closing: registration.closing,
                    closed: registration.closed,
                },
            ));
        }
        DATABASE_CONNECTIONS.remove_if(&name, |_, current| {
            Arc::ptr_eq(&current.identity, &registration.identity)
        });
    }

    laux::lua_pushnil(state);
    Ok(1)
}

fn decode_impl(state: LuaState) -> LuaResult {
    lua_safe::checkstack(state, 6);
    let response_id = checked_integer::<i64>(state, 1)?;
    let owner = checked_integer::<u32>(state, 2)?;
    let Some(result) = RESPONSES.take((owner, response_id)) else {
        push_lua_table!(state,
            "kind" => "ERROR",
            "message" => "invalid, already consumed, or expired SQLx response id"
        );
        return Ok(1);
    };

    match result {
        DatabaseResponse::PgRows(rows) => {
            return Ok(process_pg_rows(state, &rows)
                .map_err(|e| {
                    push_lua_table!(
                        state,
                        "kind" => "ERROR",
                        "message" => e
                    );
                })
                .unwrap_or(1));
        }
        DatabaseResponse::MysqlRows(rows) => {
            return Ok(process_rows::<MySql>(state, &rows)
                .map_err(|e| {
                    push_lua_table!(
                        state,
                        "kind" => "ERROR",
                        "message" => e
                    );
                })
                .unwrap_or(1));
        }
        DatabaseResponse::SqliteRows(rows) => {
            return Ok(process_rows::<Sqlite>(state, &rows)
                .map_err(|e| {
                    push_lua_table!(
                        state,
                        "kind" => "ERROR",
                        "message" => e
                    );
                })
                .unwrap_or(1));
        }
        DatabaseResponse::Execute {
            rows_affected,
            last_insert_id,
        } => {
            let table = OutputTable::new(state, 0, 3);
            table.insert("message", "ok");
            table.insert_x("rows_affected", || push_u64(state, rows_affected));
            if let Some(last_insert_id) = last_insert_id {
                table.insert_x("last_insert_id", || push_u64(state, last_insert_id));
            }
            return Ok(1);
        }
        DatabaseResponse::Transaction { rows_affected } => {
            let table = OutputTable::new(state, 0, 2);
            table.insert("message", "ok");
            table.insert_x("rows_affected", || push_u64(state, rows_affected));
            return Ok(1);
        }
        DatabaseResponse::Connect(connection) => return Ok(push_connection(state, connection)),
        DatabaseResponse::Closed => {
            lua_safe::push(state, true);
            return Ok(1);
        }
        DatabaseResponse::Timeout(message) => {
            push_lua_table!(
                state,
                "kind" => "TIMEOUT",
                "message" => message
            );
        }
        DatabaseResponse::RowLimitExceeded(max_rows) => {
            push_lua_table!(state,
                "kind" => "ERROR",
                "message" => format!("query result exceeded max_rows ({max_rows})")
            );
        }
        DatabaseResponse::ReconnectCooldown(ms) => {
            push_lua_table!(state,
                "kind" => "SOCKET",
                "message" => "SQLx reconnect cooldown; SQL was not submitted",
                "connect_failed" => true,
                "retry_after_ms" => ms
            );
        }
        response @ (DatabaseResponse::Error(_)
        | DatabaseResponse::DisconnectedError(_)
        | DatabaseResponse::ConnectFailed { .. }) => {
            let (err, retry_after_ms) = match response {
                DatabaseResponse::ConnectFailed {
                    error,
                    retry_after_ms,
                } => (error, Some(retry_after_ms)),
                DatabaseResponse::Error(error) | DatabaseResponse::DisconnectedError(error) => {
                    (error, None)
                }
                _ => unreachable!(),
            };
            match err.as_database_error() {
                Some(db_err) => {
                    let table = OutputTable::new(state, 0, 6);
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
                    let kind = if connection_error(&err) {
                        "SOCKET"
                    } else {
                        "ERROR"
                    };
                    push_lua_table!(
                        state,
                        "kind" => kind,
                        "message" => err.to_string()
                    );
                }
            }
            if let Some(ms) = retry_after_ms {
                let table = OutputTable::from_stack(state, -1);
                table.insert("connect_failed", true);
                table.insert("retry_after_ms", ms);
            }
        }
    }

    Ok(1)
}

fn stats_impl(state: LuaState) -> LuaResult {
    // Never retain a DashMap shard guard while allocating a Lua table.
    let snapshot: Vec<_> = DATABASE_CONNECTIONS
        .iter()
        .map(|pair| {
            (
                pair.key().clone(),
                pair.value().counter.load(Ordering::Acquire),
            )
        })
        .collect();
    let table = OutputTable::new(state, 0, snapshot.len());
    for (name, count) in snapshot {
        table.insert(name, count);
    }
    Ok(1)
}

fn response_stats_impl(state: LuaState) -> LuaResult {
    let (waiting, ready) = RESPONSES.counts();
    push_lua_table!(state, "waiting" => waiting, "ready" => ready);
    Ok(1)
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
        lreg!("response_stats", response_stats),
        lreg!("make_transaction", make_transaction),
        lreg_null!(),
    ];

    luaL_newlib!(state, l);

    1
}

#[cfg(test)]
#[path = "../../../../test/sqlx_native_cases.rs"]
mod native_tests;
