mod admin;
mod after_publish;
mod backend;
mod build;
mod gc;
mod inspect;
mod pull;
mod recover;
mod skills;
mod transfer;
mod writer_attester;
use grv_adapter_host::{
    Error, discovery, install,
    process::{Deadlines, Session},
};
use grv_types::{CommandOutput, ErrorCode, Name, PublicError};
use serde_json::Value;
pub fn entry() {
    let mut args: Vec<String> = std::env::args().skip(1).collect();
    let json = if args.first().map(String::as_str) == Some("--json") {
        args.remove(0);
        true
    } else {
        false
    };
    if matches!(args.first().map(String::as_str), Some("--version" | "-V")) && !json {
        println!("grv {}", env!("CARGO_PKG_VERSION"));
        std::process::exit(0);
    }
    if args.first().map(String::as_str) == Some("skills") {
        // Local agent tooling, outside the client v1 result envelope.
        skills::main(&args[1..], json);
    }
    let command = if args.first().is_some_and(|command| {
        ["ls", "show", "status", "log", "diff", "verify"].contains(&command.as_str())
    }) {
        args[0].as_str()
    } else if args.first().map(String::as_str) == Some("recover") {
        "recover"
    } else if args.first().map(String::as_str) == Some("session") {
        match args.get(1).map(String::as_str) {
            Some("prepare") => "session prepare",
            Some("show") => "session show",
            Some("renew") => "session renew",
            Some("abort") => "session abort",
            _ => "unknown",
        }
    } else if args.first().map(String::as_str) == Some("gc") {
        "gc"
    } else if args.first().map(String::as_str) == Some("unpin") {
        "unpin"
    } else if args.first().map(String::as_str) == Some("pin") {
        "pin"
    } else if args.first().map(String::as_str) == Some("pull") {
        "pull"
    } else if args.first().map(String::as_str) == Some("push") {
        "push"
    } else if args.first().map(String::as_str) == Some("init") {
        "init"
    } else if args.first().map(String::as_str) == Some("adapter") {
        match args.get(1).map(String::as_str) {
            Some("list") => "adapter list",
            Some("install") => "adapter install",
            _ => {
                if args.get(2).map(String::as_str) == Some("capabilities") {
                    "adapter capabilities"
                } else {
                    "adapter command"
                }
            }
        }
    } else {
        "unknown"
    };
    let outcome = run(&args);
    let output = match outcome {
        Ok(success) => {
            let mut output = CommandOutput::success(command, success.result);
            output.root = success.root;
            output
        }
        Err(failure) => {
            let mut output = CommandOutput::failure(command, failure.error);
            output.result = failure.result;
            output.root = failure.root;
            output
        }
    };
    // Validate the complete envelope before anything reaches stdout.
    let value = serde_json::to_value(&output).unwrap();
    if let Err(e) = grv_adapter_host::validate_output(&value) {
        eprintln!("invalid internal command output: {e}");
        std::process::exit(5);
    }
    if json {
        println!("{}", serde_json::to_string(&output).unwrap());
    } else if output.ok {
        println!("{}", serde_json::to_string_pretty(&output.result).unwrap());
    } else {
        for error in &output.errors {
            eprintln!("{}", error.message);
        }
    }
    std::process::exit(i32::from(output.exit_status));
}
struct Failure {
    error: PublicError,
    result: Option<Value>,
    root: Option<String>,
}
impl From<Error> for Failure {
    fn from(error: Error) -> Self {
        Self {
            error: error.public(),
            result: None,
            root: None,
        }
    }
}
impl From<PublicError> for Failure {
    fn from(error: PublicError) -> Self {
        Self {
            error,
            result: None,
            root: None,
        }
    }
}
struct Success {
    result: Value,
    root: Option<String>,
}
fn run(args: &[String]) -> std::result::Result<Success, Failure> {
    if let Some(command) = args.first().filter(|command| {
        ["ls", "show", "status", "log", "diff", "verify"].contains(&command.as_str())
    }) {
        return inspect::run(command, &args[1..]);
    }
    if args.first().map(String::as_str) == Some("recover") {
        return recover::recover(&args[1..]);
    }
    if args.first().map(String::as_str) == Some("session") {
        return build::session(&args[1..]);
    }
    if args.first().map(String::as_str) == Some("gc") {
        return gc::gc(&args[1..]);
    }
    if args.first().map(String::as_str) == Some("unpin") {
        return admin::unpin(&args[1..]);
    }
    if args.first().map(String::as_str) == Some("pin") {
        return admin::pin(&args[1..]);
    }
    if args.first().map(String::as_str) == Some("pull") {
        return pull::pull(&args[1..]);
    }
    if args.first().map(String::as_str) == Some("push") {
        return transfer::push(&args[1..]);
    }
    if args.first().map(String::as_str) == Some("init") {
        return init(&args[1..]);
    }
    adapter(args).map(|result| Success { result, root: None })
}
fn init(args: &[String]) -> std::result::Result<Success, Failure> {
    use grv_core::store::{InitOptions, Store};
    let mut options = InitOptions::default();
    let mut root = None;
    let mut seen = std::collections::BTreeSet::new();
    let mut index = 0;
    while index < args.len() {
        let key = args[index].as_str();
        if !seen.insert(key) {
            return Err(Error::new(ErrorCode::InvalidArgument, "duplicate init flag").into());
        }
        let value = args
            .get(index + 1)
            .ok_or_else(|| Error::new(ErrorCode::InvalidArgument, "init flag value required"))?;
        let number = |value: &str| {
            value.parse::<u64>().map_err(|_| {
                Error::new(
                    ErrorCode::InvalidArgument,
                    "init parameters must be integer seconds",
                )
            })
        };
        match key {
            "--grv" => root = Some(value),
            "--max-clock-skew" => options.max_clock_skew = Some(number(value)?),
            "--max-lease-ttl" => options.max_lease_ttl = Some(number(value)?),
            "--pending-grace" => options.pending_grace = Some(number(value)?),
            _ => return Err(Error::new(ErrorCode::InvalidArgument, "unknown init flag").into()),
        }
        index += 2;
    }
    let root =
        root.ok_or_else(|| Error::new(ErrorCode::InvalidArgument, "init requires --grv <root>"))?;
    let selected = backend::Root::parse(root)?;
    let canonical = selected.canonical.clone();
    let backend = selected.open(true)?;
    let (_, result) = Store::initialize(backend, options).map_err(|error| Failure {
        error,
        result: None,
        root: Some(canonical.clone()),
    })?;
    Ok(Success {
        result: serde_json::to_value(result).unwrap(),
        root: Some(canonical),
    })
}
fn adapter(args: &[String]) -> std::result::Result<Value, Failure> {
    if args.first().map(String::as_str) != Some("adapter") {
        return Err(Error::new(ErrorCode::InvalidArgument,"usage: grv [--json] adapter list | install <path|tarball> [--replace] | <name> capabilities | <name> <command> [args]").into());
    }
    let roots = discovery::search_roots(None)?;
    let verb = args.get(1).ok_or_else(|| {
        Error::new(
            ErrorCode::InvalidArgument,
            "adapter name or action required",
        )
    })?;
    if verb == "list" {
        if args.len() != 2 {
            return Err(Error::new(
                ErrorCode::InvalidArgument,
                "adapter list takes no arguments",
            )
            .into());
        }
        return Ok(serde_json::to_value(discovery::list(&roots)?).unwrap());
    }
    if verb == "install" {
        let path = args
            .get(2)
            .ok_or_else(|| Error::new(ErrorCode::InvalidArgument, "installation path required"))?;
        let replace = args.get(3).map(String::as_str) == Some("--replace");
        if args.len() > if replace { 4 } else { 3 } {
            return Err(
                Error::new(ErrorCode::InvalidArgument, "unknown installation flags").into(),
            );
        }
        return Ok(serde_json::to_value(install::install(
            std::path::Path::new(path),
            &roots[0],
            replace,
            Deadlines::default(),
        )?)
        .unwrap());
    }
    let name =
        Name::new(verb).map_err(|e| Error::new(ErrorCode::InvalidArgument, e.to_string()))?;
    let installed = discovery::discover(&roots)?
        .into_iter()
        .find(|i| i.manifest.name == name)
        .ok_or_else(|| Error::new(ErrorCode::NotFound, "adapter is not installed"))?;
    let sub = args
        .get(2)
        .ok_or_else(|| Error::new(ErrorCode::InvalidArgument, "adapter command required"))?;
    let mut session = Session::spawn(&installed, Deadlines::default())?;
    let result = if sub == "capabilities" {
        if args.len() != 3 {
            return Err(Error::new(
                ErrorCode::InvalidArgument,
                "capabilities takes no arguments",
            )
            .into());
        }
        serde_json::to_value(grv_adapter_api::AdapterCapabilitiesResult {
            adapter: session.descriptor.clone(),
        })
        .unwrap()
    } else {
        let command =
            Name::new(sub).map_err(|e| Error::new(ErrorCode::InvalidArgument, e.to_string()))?;
        serde_json::to_value(session.command(command, args[3..].to_vec())?).unwrap()
    };
    if let Err(error) = session.close() {
        return Err(Failure {
            error: error.public(),
            result: Some(result),
            root: None,
        });
    }
    Ok(result)
}
