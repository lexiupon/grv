fn main() {
    if let Err(error) =
        grv_adapter_sdk::run_fd3(grv_adapter_duckdb::process::DuckDbAdapter::default())
    {
        eprintln!("DuckDB adapter channel failed: {error}");
        std::process::exit(6);
    }
}
