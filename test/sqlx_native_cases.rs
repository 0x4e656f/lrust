use super::*;
use std::sync::Mutex;

#[path = "sqlx_allocation_cases.rs"]
mod allocation;

static SERIAL: Mutex<()> = Mutex::new(());
struct Lua(LuaState);
impl Lua {
    fn new() -> Self {
        let state = std::ptr::NonNull::new(unsafe { ffi::luaL_newstate() }).unwrap();
        unsafe {
            ffi::luaL_openlibs(state.as_ptr());
        }
        luaopen_rust_sqlx(state);
        unsafe {
            ffi::lua_setglobal(state.as_ptr(), c"sqlx".as_ptr());
        }
        Self(state)
    }
    fn run(&self, code: &str) {
        unsafe {
            let status = ffi::luaL_loadbufferx(
                self.0.as_ptr(),
                code.as_ptr().cast(),
                code.len(),
                c"sqlx-test".as_ptr(),
                std::ptr::null(),
            );
            assert_eq!(status, ffi::LUA_OK);
            let status = ffi::lua_pcall(self.0.as_ptr(), 0, 0, 0);
            if status != ffi::LUA_OK {
                let error = laux::lua_to::<&[u8]>(self.0, -1);
                panic!("{}", String::from_utf8_lossy(error));
            }
        }
    }
}
impl Drop for Lua {
    fn drop(&mut self) {
        unsafe {
            ffi::lua_close(self.0.as_ptr());
        }
    }
}

#[test]
fn native_invalid_arguments_release_connection_clones() {
    let _serial = SERIAL.lock().unwrap();
    let lua = Lua::new();
    let (tx, _rx) = mpsc::channel(8);
    let counter = Arc::new(AtomicI64::new(0));
    let (_closed, closed) = watch::channel(false);
    push_connection(
        lua.0,
        DatabaseConnection {
            tx: tx.clone(),
            counter: counter.clone(),
            closing: Arc::new(AtomicBool::new(false)),
            closed,
        },
    );
    unsafe {
        ffi::lua_setglobal(lua.0.as_ptr(), c"db".as_ptr());
    }
    lua.run(
        r#"
        for i = 1, 10000 do
            assert(db:query(42, i, {}).kind == 'ERROR')
            assert(db:query(42, i, string.char(255)).kind == 'ERROR')
            assert(db:query({}, i, 'SELECT 1').kind == 'ERROR')
            assert(db:batch(42, i, {}).kind == 'ERROR')
            assert(db:transaction(42, i, {}).kind == 'ERROR')
            assert(db:close(23, {}, i).kind == 'ERROR')
            assert(sqlx.connect(23, 42, i, 'postgresql://not-opened', 'invalid', 0).kind == 'ERROR')
        end
        local trans = sqlx.make_transaction()
        for i = 1, 10000 do
            assert(trans:push('SELECT $1', function() end).kind == 'ERROR')
            local cycle = {}; cycle.self = cycle
            assert(trans:push('SELECT $1', cycle).kind == 'ERROR')
        end
        assert(trans:push('SELECT 1') == nil)
        for i = 1, 8 do
            local session, lease = db:query(42, 20000+i, 'SELECT 1')
            assert(session == 20000+i)
            do local guard <close> = lease end
        end
        -- Failed enqueue must not leave a response registration behind.
        assert(db:query(42, 30000, 'SELECT 1').kind == 'BUSY')
        assert(sqlx.response_stats().waiting == 0)
        assert(sqlx.response_stats().ready == 0)
    "#,
    );
    assert_eq!(
        tx.strong_count(),
        2,
        "invalid input leaked a channel sender"
    );
    assert_eq!(
        Arc::strong_count(&counter),
        2,
        "invalid input leaked counter Arc"
    );
    assert_eq!(
        _rx.len(),
        8,
        "closing response leases must not cancel accepted SQL"
    );
    drop(lua);
    assert_eq!(tx.strong_count(), 1);
    assert_eq!(Arc::strong_count(&counter), 1);
    assert_eq!(RESPONSES.counts(), (0, 0));
}

#[test]
fn response_leases_close_gc_and_late_completion() {
    let _serial = SERIAL.lock().unwrap();
    let lua = Lua::new();
    let guard = response_guard(lua.0, 42, 101).unwrap();
    assert!(guard.is_some());
    unsafe {
        ffi::lua_setglobal(lua.0.as_ptr(), c"lease".as_ptr());
    }
    send_response(
        23,
        42,
        101,
        DatabaseResponse::Execute {
            rows_affected: 7,
            last_insert_id: None,
        },
    );
    lua.run(
        r#"
        assert(sqlx.response_stats().ready == 1)
        assert(sqlx.decode(101, 43).kind == 'ERROR')
        assert(sqlx.decode(101, 42).rows_affected == 7)
        assert(sqlx.decode(101, 42).kind == 'ERROR')
        do local guard <close> = lease end
        lease = nil
        collectgarbage('collect')
    "#,
    );
    assert_eq!(RESPONSES.counts(), (0, 0));
    let guard = response_guard(lua.0, 42, 102).unwrap();
    abandon_guard(lua.0, guard);
    send_response(23, 42, 102, DatabaseResponse::Closed);
    assert_eq!(RESPONSES.counts(), (0, 0));
    response_guard(lua.0, 42, 103).unwrap();
    send_response(23, 42, 103, DatabaseResponse::Closed);
    assert_eq!(RESPONSES.counts(), (0, 1));
    drop(lua); // Closing a service's Lua state must reclaim unread responses.
    assert_eq!(RESPONSES.counts(), (0, 0));
}

#[test]
fn boundary_contains_rust_panic() {
    let _serial = SERIAL.lock().unwrap();
    let lua = Lua::new();
    fn fail(_: LuaState) -> LuaResult {
        panic!("injected SQLx panic");
    }
    assert_eq!(run_entry(lua.0, fail), 1);
    unsafe {
        ffi::lua_setglobal(lua.0.as_ptr(), c"failure".as_ptr());
    }
    lua.run("assert(failure.kind == 'ERROR'); assert(sqlx.response_stats().ready == 0)");
}

#[test]
fn temporal_format_matches_existing_finite_values() {
    use chrono::{Duration, NaiveDate};
    let epoch = NaiveDate::from_ymd_opt(2000, 1, 1).unwrap();
    for days in (-95_000_000_i32..95_000_000).step_by(10_003) {
        if let Some(date) = epoch.checked_add_signed(Duration::days(days.into())) {
            assert_eq!(pg_datetime::date(days), date.format("%Y-%m-%d").to_string());
        }
    }
    for micros in [
        0,
        1,
        -1,
        1000,
        123456,
        -123456,
        757_425_599_123_000,
        757_425_599_123_456,
    ] {
        let value = epoch
            .and_hms_opt(0, 0, 0)
            .unwrap()
            .checked_add_signed(Duration::microseconds(micros))
            .unwrap();
        assert_eq!(
            pg_datetime::timestamp(micros, false).unwrap(),
            value.format("%Y-%m-%d %H:%M:%S%.f").to_string()
        );
        assert_eq!(
            pg_datetime::timestamp(micros, true).unwrap(),
            value
                .and_utc()
                .to_rfc3339_opts(chrono::SecondsFormat::AutoSi, true)
        );
    }
}
