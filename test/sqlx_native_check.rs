//! Standalone native binding tests. Link the existing C Lua test library.
//! No Moon binary or database: the host is stubbed and connections are channels.
extern crate self as lib_core;

pub mod context {
    lazy_static::lazy_static! {
        pub static ref CONTEXT: Context = Context {
            tokio_runtime: tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap()
        };
    }
    pub struct Context {
        pub tokio_runtime: tokio::runtime::Runtime,
    }
}
pub const LOG_LEVEL_ERROR: u8 = 1;
pub fn moon_log(_: u32, _: u8, _: String) {}
#[unsafe(no_mangle)]
pub extern "C-unwind" fn send_integer_message(_: u8, _: u32, _: i64, _: isize) {}

#[path = "../crates/libs/lib-lualib/src/lua_sqlx.rs"]
mod sqlx;
