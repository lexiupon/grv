fn main() {
    let mut args = std::env::args_os().skip(1);
    if args.next().as_deref()
        == Some(std::ffi::OsStr::new(
            grv_adapter_salesforce::containment::HELPER_ARG,
        ))
    {
        grv_adapter_salesforce::containment::exec(args);
    }
    if let Err(error) =
        grv_adapter_sdk::run_fd3(grv_adapter_salesforce::process::SalesforceAdapter::default())
    {
        eprintln!("Salesforce adapter channel failed: {error}");
        std::process::exit(6);
    }
}
