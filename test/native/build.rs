use std::path::PathBuf;

fn main() {
    let lua = PathBuf::from(std::env::var_os("CARGO_MANIFEST_DIR").unwrap())
        .join("../../../../third/lua");
    println!("cargo:rerun-if-changed={}", lua.display());
    let mut build = cc::Build::new();
    build.std("c11");
    build
        .file(lua.join("onelua.c"))
        .include(&lua)
        .define("MAKE_LIB", None);
    match std::env::var("CARGO_CFG_TARGET_OS").unwrap().as_str() {
        "linux" => {
            build.define("LUA_USE_LINUX", None);
            println!("cargo:rustc-link-lib=dl");
            println!("cargo:rustc-link-lib=m");
        }
        "windows" => {
            build.flag_if_supported("/experimental:c11atomics");
        }
        _ => (),
    }
    build.compile("sqlx_test_lua_runtime");
}
