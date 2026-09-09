//! SQLx-local Lua allocation boundary. Never let a Lua longjmp cross Rust owners.
//!
//! All potentially throwing Lua operations run in the small `dispatch` frame,
//! which owns no Rust resources. A failed lua_pcall returns normally first; a
//! private Rust unwind then runs Drop up to run_entry, which rethrows the Lua
//! error only AFTER dropping its panic payload. Do not use with panic=abort.
//! Keep this private: callbacks must not call arbitrary owning Rust code.

#[cfg(panic = "abort")]
compile_error!("SQLx Lua allocation recovery requires panic=unwind (the workspace default)");

use lib_lua::{
    ffi,
    laux::{self, LuaState, LuaValue},
};
use std::{
    ffi::{c_char, c_void},
    panic::resume_unwind,
};

pub(super) struct LuaFailure {
    pub error_on_stack: bool,
}

pub(super) fn checkstack(state: LuaState, extra: i32) {
    if unsafe { ffi::lua_checkstack(state.as_ptr(), extra) } == 0 {
        resume_unwind(Box::new(LuaFailure {
            error_on_stack: false,
        }));
    }
}

// Borrowed values only: their owners stay ABOVE lua_pcall, not in dispatch.
#[derive(Clone, Copy)]
pub(super) enum Value<'a> {
    Integer(i64),
    Number(f64),
    Boolean(bool),
    Bytes(&'a [u8]),
}

pub(super) trait PushValue {
    fn value(&self) -> Value<'_>;
}
macro_rules! integers {
    ($($ty:ty),*) => {$(impl PushValue for $ty {
        fn value(&self) -> Value<'_> { Value::Integer(*self as i64) }
    })*};
}
integers!(i8, u8, i16, u16, i32, u32, i64, u64, usize, isize);
impl PushValue for bool {
    fn value(&self) -> Value<'_> {
        Value::Boolean(*self)
    }
}
impl PushValue for f32 {
    fn value(&self) -> Value<'_> {
        Value::Number(*self as f64)
    }
}
impl PushValue for f64 {
    fn value(&self) -> Value<'_> {
        Value::Number(*self)
    }
}
impl PushValue for str {
    fn value(&self) -> Value<'_> {
        Value::Bytes(self.as_bytes())
    }
}
impl PushValue for String {
    fn value(&self) -> Value<'_> {
        Value::Bytes(self.as_bytes())
    }
}
impl PushValue for [u8] {
    fn value(&self) -> Value<'_> {
        Value::Bytes(self)
    }
}
impl<T: PushValue + ?Sized> PushValue for &T {
    fn value(&self) -> Value<'_> {
        (*self).value()
    }
}

enum Op<'a> {
    Push(Value<'a>),
    Table(i32, i32),
    Insert(Value<'a>, Value<'a>),
    SetValue(Value<'a>),
    SetIndex(i64),
    Get(Value<'a>),
    Meta(*const c_char),
    Shape(bool, usize),
    Next(bool),
    Userdata {
        name: *const c_char,
        key: *const c_void,
        size: usize,
        lib: *const ffi::luaL_Reg,
        gc: ffi::lua_CFunction,
        close: bool,
        init: unsafe fn(*mut c_void),
    },
}

// Adding an owning field here would reintroduce the Linux longjmp leak.
const _: () = assert!(!std::mem::needs_drop::<Op<'static>>());

// SAFETY: no owned Rust temporaries, destructors, user closures or panics here.
// Lua references are explicit pcall arguments, never outer-frame stack indices.
unsafe fn push_raw(state: *mut ffi::lua_State, value: Value<'_>) {
    unsafe {
        match value {
            Value::Integer(v) => ffi::lua_pushinteger(state, v),
            Value::Number(v) => ffi::lua_pushnumber(state, v),
            Value::Boolean(v) => ffi::lua_pushboolean(state, v as i32),
            Value::Bytes(v) => {
                ffi::lua_pushlstring(state, v.as_ptr().cast(), v.len());
            }
        }
    }
}

extern "C-unwind" fn dispatch(state: *mut ffi::lua_State) -> i32 {
    unsafe {
        let op = &mut *ffi::lua_touserdata(state, 1).cast::<Op<'_>>();
        match op {
            Op::Push(value) => {
                push_raw(state, *value);
                1
            }
            Op::Table(array, record) => {
                ffi::lua_createtable(state, *array, *record);
                1
            }
            Op::Insert(key, value) => {
                push_raw(state, *key);
                push_raw(state, *value);
                ffi::lua_rawset(state, 2);
                0
            }
            Op::SetValue(key) => {
                push_raw(state, *key);
                ffi::lua_pushvalue(state, 3);
                ffi::lua_rawset(state, 2);
                0
            }
            Op::SetIndex(index) => {
                ffi::lua_pushvalue(state, 3);
                ffi::lua_rawseti(state, 2, *index);
                0
            }
            Op::Get(key) => {
                push_raw(state, *key);
                ffi::lua_rawget(state, 2);
                1
            }
            Op::Meta(key) => {
                if ffi::luaL_getmetafield(state, 2, *key) == ffi::LUA_TNIL {
                    ffi::lua_pushnil(state);
                }
                1
            }
            Op::Shape(array, len) => {
                // The common shape detector contains only primitive Lua calls
                // and Copy locals; protect its interned metatable-key lookups.
                (*array, *len) = laux::lua_array_size(std::ptr::NonNull::new_unchecked(state), 2);
                0
            }
            Op::Next(more) => {
                ffi::lua_pushvalue(state, 3);
                *more = ffi::lua_next(state, 2) != 0;
                if *more { 2 } else { 0 }
            }
            Op::Userdata {
                name,
                key,
                size,
                lib,
                gc,
                close,
                init,
            } => {
                ffi::lua_rawgetp(state, ffi::LUA_REGISTRYINDEX, *key);
                if ffi::lua_isnil(state, -1) != 0 {
                    ffi::lua_pop(state, 1);
                    // Publish only a COMPLETE metatable. An allocation failure
                    // must not leave a cached table without __gc for a retry.
                    ffi::lua_createtable(state, 0, 5);
                    ffi::lua_pushstring(state, *name);
                    ffi::lua_setfield(state, -2, c"__name".as_ptr());
                    ffi::lua_createtable(state, 0, 6);
                    ffi::luaL_setfuncs(state, *lib, 0);
                    ffi::lua_setfield(state, -2, c"__index".as_ptr());
                    ffi::lua_pushcfunction(state, *gc);
                    ffi::lua_setfield(state, -2, c"__gc".as_ptr());
                    if *close {
                        ffi::lua_pushcfunction(state, *gc);
                        ffi::lua_setfield(state, -2, c"__close".as_ptr());
                    }
                    ffi::lua_pushboolean(state, 0);
                    ffi::lua_setfield(state, -2, c"__metatable".as_ptr());
                    ffi::lua_pushvalue(state, -1);
                    ffi::lua_rawsetp(state, ffi::LUA_REGISTRYINDEX, *key);
                }
                let ptr = ffi::lua_newuserdatauv(state, *size, 0);
                init(ptr); // None<T>; GC is safe even before ownership transfer.
                ffi::lua_pushvalue(state, -2);
                ffi::lua_setmetatable(state, -2);
                1
            }
        }
    }
}

fn protect(state: LuaState, op: &mut Op<'_>, inputs: &[i32], results: i32) {
    checkstack(state, inputs.len() as i32 + 4);
    unsafe {
        let top = ffi::lua_gettop(state.as_ptr());
        ffi::lua_pushcfunction(state.as_ptr(), dispatch); // zero upvalues: no allocation
        ffi::lua_pushlightuserdata(state.as_ptr(), (op as *mut Op<'_>).cast());
        for &index in inputs {
            let absolute = if index < 0 && index > ffi::LUA_REGISTRYINDEX {
                top + index + 1
            } else {
                index
            };
            ffi::lua_pushvalue(state.as_ptr(), absolute);
        }
        if ffi::lua_pcall(state.as_ptr(), inputs.len() as i32 + 1, results, 0) != ffi::LUA_OK {
            // Preserve the original Lua error on top. SQLx stack guards skip
            // stack cleanup during this unwind; Rust-owned values still Drop.
            resume_unwind(Box::new(LuaFailure {
                error_on_stack: true,
            }));
        }
    }
}

pub(super) fn push<T: PushValue>(state: LuaState, value: T) {
    match value.value() {
        Value::Bytes(bytes) => protect(state, &mut Op::Push(Value::Bytes(bytes)), &[], 1),
        scalar => {
            checkstack(state, 1);
            unsafe {
                push_raw(state.as_ptr(), scalar);
            }
        }
    }
}

/// Output tables contain no metamethods. Fuse key/value insertion in one pcall
/// instead of protecting each push separately; no intermediate row/JSON copies.
pub(super) struct OutputTable {
    state: LuaState,
    index: i32,
}
impl OutputTable {
    pub fn new(state: LuaState, array: usize, record: usize) -> Self {
        protect(
            state,
            &mut Op::Table(
                array.min(i32::MAX as usize) as i32,
                record.min(i32::MAX as usize) as i32,
            ),
            &[],
            1,
        );
        Self::from_stack(state, -1)
    }
    pub fn from_stack(state: LuaState, index: i32) -> Self {
        Self {
            state,
            index: unsafe { ffi::lua_absindex(state.as_ptr(), index) },
        }
    }
    pub fn insert<K: PushValue, V: PushValue>(&self, key: K, value: V) -> &Self {
        protect(
            self.state,
            &mut Op::Insert(key.value(), value.value()),
            &[self.index],
            0,
        );
        self
    }
    pub fn insert_x<K: PushValue, F: FnOnce()>(&self, key: K, push: F) -> &Self {
        push(); // Rust closure and its captured owners stay outside dispatch.
        protect(
            self.state,
            &mut Op::SetValue(key.value()),
            &[self.index, -1],
            0,
        );
        unsafe {
            ffi::lua_pop(self.state.as_ptr(), 1);
        }
        self
    }
    pub fn rawseti(&self, index: usize) {
        protect(
            self.state,
            &mut Op::SetIndex(index as i64),
            &[self.index, -1],
            0,
        );
        unsafe {
            ffi::lua_pop(self.state.as_ptr(), 1);
        }
    }
}

pub(super) fn userdata<T: super::SqlxUserdata>(state: LuaState, value: T, lib: &[laux::LuaReg]) {
    extern "C-unwind" fn gc<T: super::SqlxUserdata>(state: *mut ffi::lua_State) -> i32 {
        unsafe {
            if let Some(ptr) = test_userdata::<T>(std::ptr::NonNull::new_unchecked(state), 1) {
                (*ptr).take();
            }
        }
        0
    }
    unsafe fn init<T>(ptr: *mut c_void) {
        unsafe {
            ptr.cast::<Option<T>>().write(None);
        }
    }
    protect(
        state,
        &mut Op::Userdata {
            name: T::METATABLE.as_ptr(),
            key: T::metatable_key(),
            size: std::mem::size_of::<Option<T>>(),
            lib: lib.as_ptr().cast(),
            gc: gc::<T>,
            close: T::CLOSE,
            init: init::<T>,
        },
        &[],
        1,
    );
    // The userdata is now rooted and has a complete __gc metatable. No Lua
    // operation (and thus no longjmp/GC) occurs during this ownership transfer.
    unsafe {
        ffi::lua_touserdata(state.as_ptr(), -1)
            .cast::<Option<T>>()
            .write(Some(value));
    }
}

// Non-allocating validation, also safe inside __gc. Avoid luaL_testudata's
// allocating string lookup. The registry key is private to this SQLx module.
pub(super) unsafe fn test_userdata<T: super::SqlxUserdata>(
    state: LuaState,
    index: i32,
) -> Option<*mut Option<T>> {
    unsafe {
        if ffi::lua_type(state.as_ptr(), index) != ffi::LUA_TUSERDATA
            || ffi::lua_rawlen(state.as_ptr(), index) != std::mem::size_of::<Option<T>>()
        {
            return None;
        }
        let ptr = ffi::lua_touserdata(state.as_ptr(), index).cast();
        if ffi::lua_getmetatable(state.as_ptr(), index) == 0 {
            return None;
        }
        ffi::lua_rawgetp(state.as_ptr(), ffi::LUA_REGISTRYINDEX, T::metatable_key());
        let equal = ffi::lua_rawequal(state.as_ptr(), -1, -2) != 0;
        ffi::lua_pop(state.as_ptr(), 2);
        equal.then_some(ptr)
    }
}

// Input views reuse the existing non-allocating LuaValue decoder. Scope guards
// restore exact stack heights (including iterator keys) on ordinary exits.
// During an unwind, leave the Lua error on top for run_entry to rethrow.
struct StackScope {
    state: LuaState,
    top: i32,
}
impl StackScope {
    fn new(state: LuaState) -> Self {
        Self {
            state,
            top: laux::lua_top(state),
        }
    }
}
impl Drop for StackScope {
    fn drop(&mut self) {
        if !std::thread::panicking() {
            unsafe {
                ffi::lua_settop(self.state.as_ptr(), self.top);
            }
        }
    }
}
pub(super) struct ScopeValue<'a> {
    pub value: LuaValue<'a>,
    _scope: StackScope,
}

pub(super) trait TableRead {
    fn raw_get(&self, key: &str) -> ScopeValue<'_>;
    fn meta_field(&self, key: &'static std::ffi::CStr) -> Option<ScopeValue<'_>>;
    fn array_shape(&self) -> (bool, usize);
    fn pairs(&self) -> Pairs<'_>;
    fn values(&self, len: usize) -> Values<'_>;
}
impl TableRead for laux::LuaTable {
    fn raw_get(&self, key: &str) -> ScopeValue<'_> {
        let scope = StackScope::new(self.lua_state());
        protect(
            self.lua_state(),
            &mut Op::Get(key.value()),
            &[self.index()],
            1,
        );
        ScopeValue {
            value: LuaValue::from_stack(self.lua_state(), -1),
            _scope: scope,
        }
    }
    fn meta_field(&self, key: &'static std::ffi::CStr) -> Option<ScopeValue<'_>> {
        let scope = StackScope::new(self.lua_state());
        protect(
            self.lua_state(),
            &mut Op::Meta(key.as_ptr()),
            &[self.index()],
            1,
        );
        let value = LuaValue::from_stack(self.lua_state(), -1);
        if matches!(value, LuaValue::Nil) {
            None
        } else {
            Some(ScopeValue {
                value,
                _scope: scope,
            })
        }
    }
    fn array_shape(&self) -> (bool, usize) {
        let mut op = Op::Shape(false, 0);
        protect(self.lua_state(), &mut op, &[self.index()], 0);
        match op {
            Op::Shape(array, len) => (array, len),
            _ => unreachable!(),
        }
    }
    fn pairs(&self) -> Pairs<'_> {
        checkstack(self.lua_state(), 3);
        let scope = StackScope::new(self.lua_state());
        unsafe {
            ffi::lua_pushnil(self.lua_state().as_ptr());
        }
        Pairs {
            table: self,
            scope,
            done: false,
        }
    }
    fn values(&self, len: usize) -> Values<'_> {
        checkstack(self.lua_state(), 1);
        Values {
            table: self,
            scope: StackScope::new(self.lua_state()),
            position: 0,
            len,
        }
    }
}

pub(super) struct Pairs<'a> {
    table: &'a laux::LuaTable,
    scope: StackScope,
    done: bool,
}
impl<'a> Iterator for Pairs<'a> {
    type Item = (LuaValue<'a>, LuaValue<'a>);
    fn next(&mut self) -> Option<Self::Item> {
        if self.done {
            return None;
        }
        let state = self.table.lua_state();
        unsafe {
            ffi::lua_settop(state.as_ptr(), self.scope.top + 1);
        }
        let mut op = Op::Next(false);
        protect(state, &mut op, &[self.table.index(), -1], ffi::LUA_MULTRET);
        self.done = matches!(op, Op::Next(false));
        if self.done {
            return None;
        }
        unsafe {
            ffi::lua_remove(state.as_ptr(), self.scope.top + 1);
        }
        Some((
            LuaValue::from_stack(state, -2),
            LuaValue::from_stack(state, -1),
        ))
    }
}
pub(super) struct Values<'a> {
    table: &'a laux::LuaTable,
    scope: StackScope,
    position: usize,
    len: usize,
}
impl<'a> Iterator for Values<'a> {
    type Item = LuaValue<'a>;
    fn next(&mut self) -> Option<Self::Item> {
        let state = self.table.lua_state();
        unsafe {
            ffi::lua_settop(state.as_ptr(), self.scope.top);
        }
        if self.position == self.len {
            return None;
        }
        self.position += 1;
        // rawgeti is non-allocating; the table and stack slot are validated.
        unsafe {
            ffi::lua_rawgeti(state.as_ptr(), self.table.index(), self.position as i64);
        }
        Some(LuaValue::from_stack(state, -1))
    }
}
