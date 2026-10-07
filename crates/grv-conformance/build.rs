fn main() {
    println!("cargo:rerun-if-env-changed=GRV_DUCKDB_NATIVE_LIB_DIR");
    if std::env::var_os("CARGO_FEATURE_NATIVE_DUCKDB").is_some() {
        let path = std::env::var("GRV_DUCKDB_NATIVE_LIB_DIR")
            .expect("native-duckdb requires the pinned native/build.py output");
        // Transitive library link flags do not set executable rpaths. The host
        // clears loader environment variables before supervising this fixture.
        println!("cargo:rustc-link-arg=-Wl,-rpath,{path}");
    }
}
