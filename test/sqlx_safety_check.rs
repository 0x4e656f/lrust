//! rustc --edition 2024 --test test/sqlx_safety_check.rs -o target/sqlx_safety_check
//! No host, database, or third-party crates are needed for these safety policies.
#[path = "../crates/libs/lib-lualib/src/sqlx_pg_datetime.rs"]
mod datetime;
#[path = "../crates/libs/lib-lualib/src/sqlx_response.rs"]
mod response;
