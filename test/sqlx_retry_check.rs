// Standalone pure-policy tests: no Moon host, database or third-party crates.
// rustc --edition 2024 --test test/sqlx_retry_check.rs -o target/sqlx_retry_check.exe
#[path = "../crates/libs/lib-lualib/src/sqlx_retry.rs"]
mod retry;
