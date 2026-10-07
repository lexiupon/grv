fn main() {
    if std::env::var_os("CARGO_FEATURE_NATIVE").is_some() {
        let path = std::env::var("GRV_DUCKDB_NATIVE_LIB_DIR")
            .expect("native feature requires GRV_DUCKDB_NATIVE_LIB_DIR from native/build.py");
        println!("cargo:rustc-link-search=native={path}");
        println!("cargo:rustc-link-lib=dylib=grv_duckdb_guard");
        let target = std::env::var("CARGO_CFG_TARGET_OS").expect("Cargo target OS");
        let runtime = match target.as_str() {
            "macos" => "@loader_path/lib",
            "linux" => "$ORIGIN/lib",
            _ => panic!("native DuckDB supports Linux and macOS"),
        };
        println!("cargo:rustc-link-arg=-Wl,-rpath,{runtime}");
        // Development tests load from the configured build output. Bundles
        // must not retain that fallback: relocation must prove packaged loads.
        if std::env::var("GRV_DUCKDB_BUNDLE_ONLY_RPATH").as_deref() != Ok("1") {
            println!("cargo:rustc-link-arg=-Wl,-rpath,{path}");
        }
        println!("cargo:rerun-if-env-changed=GRV_DUCKDB_NATIVE_LIB_DIR");
        println!("cargo:rerun-if-env-changed=GRV_DUCKDB_BUNDLE_ONLY_RPATH");
    }
}
