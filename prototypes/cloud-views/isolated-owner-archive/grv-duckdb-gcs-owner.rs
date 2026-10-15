//! Dedicated private native GCS owner; never interprets SQL or credentials as CLI arguments.
fn main() {
    if grv_adapter_duckdb::native::run_gcs_owner().is_err() {
        // Diagnostics deliberately contain neither SQL nor native/network errors.
        std::process::exit(1);
    }
}
