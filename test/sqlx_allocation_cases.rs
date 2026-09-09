//! Fault-injection tests use the production Lua boundary with a failing C Lua
//! allocator. No Moon, SQL server, network operation or runtime polling.
use super::*;
use std::{
    alloc::{GlobalAlloc, Layout, System},
    cell::Cell,
    ffi::c_void,
};

// Track native allocations on this test thread only. This catches leaked Rust
// strings/vectors/JSON trees, not just entries visible in response_stats().
thread_local! {
    static TRACK: Cell<bool> = const { Cell::new(false) };
    static DELTA: Cell<isize> = const { Cell::new(0) };
}
struct CountedSystem;
fn record(bytes: isize) {
    let _ = TRACK.try_with(|enabled| {
        if enabled.get() {
            let _ = DELTA.try_with(|delta| delta.set(delta.get() + bytes));
        }
    });
}
unsafe impl GlobalAlloc for CountedSystem {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let ptr = unsafe { System.alloc(layout) };
        if !ptr.is_null() {
            record(layout.size() as isize);
        }
        ptr
    }
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        let ptr = unsafe { System.alloc_zeroed(layout) };
        if !ptr.is_null() {
            record(layout.size() as isize);
        }
        ptr
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        record(-(layout.size() as isize));
        unsafe {
            System.dealloc(ptr, layout);
        }
    }
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        let result = unsafe { System.realloc(ptr, layout, size) };
        if !result.is_null() {
            record(size as isize - layout.size() as isize);
        }
        result
    }
}
#[global_allocator]
static ALLOCATOR: CountedSystem = CountedSystem;

struct Fault {
    original: ffi::lua_Alloc,
    original_ud: *mut c_void,
    remaining: Option<usize>,
    rejected: usize,
}
unsafe extern "C-unwind" fn allocate(
    ud: *mut c_void,
    ptr: *mut c_void,
    old: usize,
    new: usize,
) -> *mut c_void {
    let fault = unsafe { &mut *ud.cast::<Fault>() };
    if new > 0 && (ptr.is_null() || new > old) {
        if let Some(remaining) = &mut fault.remaining {
            if *remaining == 0 {
                fault.rejected += 1;
                return std::ptr::null_mut(); // also fail Lua's emergency-GC retry
            }
            *remaining -= 1;
        }
    }
    unsafe { (fault.original)(fault.original_ud, ptr, old, new) }
}
struct FaultLua {
    lua: Lua,
    fault: Box<Fault>,
}
impl FaultLua {
    fn new() -> Self {
        let lua = Lua::new();
        lua.run("sqlx.response_stats()"); // prewarm entry/call stack, not userdata metatables
        unsafe {
            assert_ne!(ffi::lua_checkstack(lua.0.as_ptr(), 4096), 0);
            let mut original_ud = std::ptr::null_mut();
            let original = ffi::lua_getallocf(lua.0.as_ptr(), &mut original_ud);
            let mut fault = Box::new(Fault {
                original,
                original_ud,
                remaining: None,
                rejected: 0,
            });
            ffi::lua_setallocf(lua.0.as_ptr(), allocate, (&mut *fault as *mut Fault).cast());
            Self { lua, fault }
        }
    }
    fn call(&mut self, budget: usize, args: i32) -> (i32, isize) {
        self.fault.rejected = 0;
        self.fault.remaining = Some(budget);
        DELTA.set(0);
        TRACK.set(true);
        let status = unsafe { ffi::lua_pcall(self.lua.0.as_ptr(), args, ffi::LUA_MULTRET, 0) };
        self.fault.remaining = None;
        unsafe {
            ffi::lua_settop(self.lua.0.as_ptr(), 0);
            ffi::lua_gc(self.lua.0.as_ptr(), ffi::LUA_GCCOLLECT);
        }
        TRACK.set(false);
        (status, DELTA.get())
    }
}
impl Drop for FaultLua {
    fn drop(&mut self) {
        // Restore before the boxed allocator context or Lua state can drop.
        unsafe {
            ffi::lua_setallocf(
                self.lua.0.as_ptr(),
                self.fault.original,
                self.fault.original_ud,
            );
        }
    }
}

fn exercise(state: LuaState) -> LuaResult {
    match checked_integer::<i64>(state, 1)? {
        0 => {
            response_guard(state, 123, 456)?;
        }
        1 => {
            let (tx, _rx) = mpsc::channel(8);
            let (_closed, closed) = watch::channel(false);
            push_connection(
                state,
                DatabaseConnection {
                    tx,
                    counter: Arc::new(AtomicI64::new(0)),
                    closing: Arc::new(AtomicBool::new(false)),
                    closed,
                },
            );
        }
        2 => {
            let querys = (0..8)
                .map(|i| DatabaseQuery {
                    sql: format!("SELECT {i}"),
                    binds: vec![QueryParams::Text("owned transaction parameter".repeat(16))],
                })
                .collect();
            push_userdata(
                state,
                TransactionQuerys { querys },
                &[lreg!("push", push_transaction_query), lreg_null!()],
            );
        }
        3 => {
            let values: Vec<_> = (0..12)
                .map(|i| {
                    serde_json::json!({
                        "index": i, "payload": "large owned JSON string".repeat(16),
                        "nested": [null, true, {"unicode": "中文", "empty": []}]
                    })
                })
                .collect();
            push_json_value(state, serde_json::Value::Array(values));
        }
        4 => {
            let values = (0..16)
                .map(|i| {
                    if i % 3 == 0 {
                        None
                    } else {
                        Some(format!("array {i}: {}", "x".repeat(128)))
                    }
                })
                .collect();
            push_optional_array(state, values, lua_safe::push);
        }
        5 => return Err("native error owns this message: ".repeat(128)),
        6 => {
            // The result owns a real SQLx error object and is consumed by the
            // actual decode path. Allocating the error table must drop it too.
            let _lease = RESPONSES.register((123, 456))?;
            RESPONSES.publish(
                (123, 456),
                DatabaseResponse::Error(sqlx::Error::Protocol("owned database error".repeat(256))),
            );
            unsafe {
                ffi::lua_pushinteger(state.as_ptr(), 456);
                ffi::lua_replace(state.as_ptr(), 1);
                ffi::lua_pushinteger(state.as_ptr(), 123);
            }
            return decode_impl(state);
        }
        7 => {
            // Convert input recursively while owning earlier query parameters.
            let sql = "SELECT $1".repeat(256);
            let parameter = get_query_param(state, 2)?;
            let _query = DatabaseQuery {
                sql,
                binds: vec![parameter],
            };
            lua_safe::push(state, true);
        }
        8 => {
            let table = OutputTable::new(state, 0, 8);
            table.insert("timestamp", pg_datetime::timestamp(123456, true)?);
            table.insert("bytes", &[0_u8, 255, 0][..]);
            table.insert_x("unsigned", || push_u64(state, u64::MAX));
            table.insert_x("null", || push_sql_null(state));
            table.insert_x("json", || {
                push_json_value(state, serde_json::json!({"message": "test"}))
            });
        }
        9 => std::panic::resume_unwind(Box::new("owned panic payload".repeat(64))),
        _ => unreachable!(),
    }
    Ok(1)
}
extern "C-unwind" fn exercise_entry(state: *mut ffi::lua_State) -> i32 {
    run_entry(std::ptr::NonNull::new(state).unwrap(), exercise)
}
extern "C-unwind" fn connect_entry(state: *mut ffi::lua_State) -> i32 {
    connect(std::ptr::NonNull::new(state).unwrap())
}

#[test]
fn every_lua_allocation_failure_releases_native_resources_and_recovers() {
    let _serial = SERIAL.lock().unwrap();
    // Registry shard capacities are intentional process-lifetime storage.
    // Prewarm the one test key so only request-owned allocations are measured.
    drop(RESPONSES.register((123, 456)).unwrap());
    for mode in 0..=9 {
        let mut failures = 0;
        for budget in 0..512 {
            let mut lua = FaultLua::new();
            if mode == 7 {
                lua.lua
                    .run(r#"input = {nested = {1, {value = 'input'}, false}, text = 'owned'}"#);
            }
            unsafe {
                ffi::lua_pushcfunction(lua.lua.0.as_ptr(), exercise_entry);
                ffi::lua_pushinteger(lua.lua.0.as_ptr(), mode);
                if mode == 7 {
                    ffi::lua_getglobal(lua.lua.0.as_ptr(), c"input".as_ptr());
                }
            }
            let (status, native_bytes) = lua.call(budget, if mode == 7 { 2 } else { 1 });
            assert_eq!(
                native_bytes, 0,
                "mode={mode}, budget={budget}, status={status}: leaked Rust bytes"
            );
            assert_eq!(RESPONSES.counts(), (0, 0), "mode={mode}, budget={budget}");
            // On the SAME Lua state, partial metatable creation must not poison
            // later userdata, close handlers, builders or connection methods.
            lua.lua.run(
                r#"
                local t = sqlx.make_transaction()
                assert(t:push('SELECT $1', 'after allocation failure') == nil)
                assert(sqlx.response_stats().waiting == 0)
                assert(sqlx.response_stats().ready == 0)
            "#,
            );
            if status == ffi::LUA_OK {
                assert!(
                    failures > 0 || mode == 7,
                    "mode={mode}: allocator was not exercised"
                );
                println!(
                    "allocation case {mode}: {failures} failure points, recovery and zero native-byte balance"
                );
                break;
            }
            assert_eq!(status, ffi::LUA_ERRMEM, "mode={mode}, budget={budget}");
            assert!(lua.fault.rejected > 0);
            failures += 1;
            assert!(
                budget < 511,
                "case did not reach a successful allocation budget"
            );
        }
    }
}

#[test]
fn repeated_connect_allocation_failures_do_not_register_or_start_requests() {
    let _serial = SERIAL.lock().unwrap();
    let mut lua = FaultLua::new();
    // No successful connect is attempted; the first allocation in the lease
    // builder always fails before CONTEXT or its runtime can be accessed.
    for session in 1..=100 {
        drop(RESPONSES.register((42, session)).unwrap()); // exclude shard growth
        unsafe {
            ffi::lua_pushcfunction(lua.lua.0.as_ptr(), connect_entry);
            ffi::lua_pushinteger(lua.lua.0.as_ptr(), 23);
            ffi::lua_pushinteger(lua.lua.0.as_ptr(), 42);
            ffi::lua_pushinteger(lua.lua.0.as_ptr(), session);
            ffi::lua_pushstring(
                lua.lua.0.as_ptr(),
                c"postgresql://never-opened.invalid/test".as_ptr(),
            );
            ffi::lua_pushstring(lua.lua.0.as_ptr(), c"allocation-test".as_ptr());
        }
        let (status, native_bytes) = lua.call(0, 5);
        assert_eq!(status, ffi::LUA_ERRMEM);
        assert_eq!(native_bytes, 0);
        assert_eq!(RESPONSES.counts(), (0, 0));
    }
    drop(lua);
    assert_eq!(RESPONSES.counts(), (0, 0));
}

#[test]
fn failed_input_traversal_restores_stack_and_does_not_pollute_later_parameters() {
    let _serial = SERIAL.lock().unwrap();
    let lua = Lua::new();
    lua.run(r#"
        local t = sqlx.make_transaction()
        for i = 1, 1000 do
            assert(t:push('SELECT $1', {nested = {unsupported = function() end}}).kind == 'ERROR')
        end
        assert(t:push('SELECT $1, $2', {nested = {answer = 42}}, {__sqlx_array = true, type = 'int8', values = {1, 2, 3}}) == nil)
    "#);
    assert_eq!(laux::lua_top(lua.0), 0);
}

extern "C-unwind" fn find_entry(state: *mut ffi::lua_State) -> i32 {
    find_connection(std::ptr::NonNull::new(state).unwrap())
}
extern "C-unwind" fn stats_entry(state: *mut ffi::lua_State) -> i32 {
    stats(std::ptr::NonNull::new(state).unwrap())
}

#[test]
fn allocation_failures_release_named_connection_clones_and_stats_snapshots() {
    let _serial = SERIAL.lock().unwrap();
    let (tx, _rx) = mpsc::channel(8);
    let counter = Arc::new(AtomicI64::new(0));
    let (_closed, closed) = watch::channel(false);
    DATABASE_CONNECTIONS.insert(
        "allocation-fixture".into(),
        DatabaseRegistration {
            identity: Arc::new(()),
            tx: tx.downgrade(),
            counter: counter.clone(),
            closing: Arc::new(AtomicBool::new(false)),
            closed,
        },
    );
    for mode in 0..2 {
        for budget in 0..64 {
            let mut lua = FaultLua::new();
            unsafe {
                ffi::lua_pushcfunction(
                    lua.lua.0.as_ptr(),
                    if mode == 0 { find_entry } else { stats_entry },
                );
                if mode == 0 {
                    ffi::lua_pushstring(lua.lua.0.as_ptr(), c"allocation-fixture".as_ptr());
                }
            }
            let (status, native_bytes) = lua.call(budget, if mode == 0 { 1 } else { 0 });
            assert_eq!(native_bytes, 0, "mode={mode}, budget={budget}");
            assert_eq!(tx.strong_count(), 1, "named lookup leaked sender");
            assert_eq!(Arc::strong_count(&counter), 2, "named lookup leaked Arc");
            // Same-state retry checks the *connection* metatable, not only the
            // transaction metatable exercised by the generic allocation sweep.
            lua.lua.run(
                r#"
                local db = assert(sqlx.find_connection('allocation-fixture'))
                assert(type(db.query) == 'function' and type(db.close) == 'function')
            "#,
            );
            drop(lua);
            assert_eq!(tx.strong_count(), 1);
            if status == ffi::LUA_OK {
                break;
            }
            assert_eq!(status, ffi::LUA_ERRMEM);
            assert!(budget < 63);
        }
    }
    DATABASE_CONNECTIONS.remove("allocation-fixture");
    assert_eq!(Arc::strong_count(&counter), 1);
}
