//! Adversarial process peer for supervision tests, never a product adapter.
use grv_adapter_api::*;
use grv_adapter_sdk::{Adapter, PreparedCommand, Registration, Result, StopToken};
use serde_json::{Value, json};
use std::{
    process::{Child, Command, Stdio},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    thread,
    time::Duration,
};
struct Adversary {
    scenario: String,
    child: Option<Child>,
    stop: Arc<AtomicBool>,
    flood: Option<thread::JoinHandle<()>>,
}
impl Adapter for Adversary {
    fn registration(&self) -> Registration {
        Registration {
            name: Name::new("adversary").unwrap(),
            package_version: "0.1.0".into(),
            interface_versions: vec![Req::new(1).unwrap()],
            binding_schema_version: Req::new(1).unwrap(),
            capabilities: Capabilities::default(),
            registry: Registry {
                schema_bundle: json!({"$schema":"https://json-schema.org/draft/2020-12/schema","$defs":{"args":{"type":"object","properties":{"message":{"type":"string"}},"required":["message"],"additionalProperties":false},"result":{"type":"object","properties":{"message":{"type":"string"}},"required":["message"],"additionalProperties":false}}}),
                points: vec![],
            },
            commands: vec![CommandDescriptor {
                name: Name::new("echo").unwrap(),
                requires_connection: false,
                requires_authentication: false,
                args_schema_pointer: "/$defs/args".into(),
                result_schema_pointer: "/$defs/result".into(),
            }],
        }
    }
    fn prepare_command(&self, _name: &Name, argv: &[String]) -> Result<PreparedCommand> {
        Ok(PreparedCommand {
            args: json!({"message":argv.get(1).cloned().unwrap_or_default()}),
            connection: None,
        })
    }
    fn execute_command(&mut self, call: &CommandCall, stop: &StopToken) -> Result<Value> {
        if self.scenario == "child" || self.scenario == "leak" {
            let child = Command::new("/bin/sleep")
                .arg("60")
                .stdin(Stdio::null())
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .spawn()
                .unwrap();
            std::fs::write("child.pid", child.id().to_string()).unwrap();
            self.child = Some(child);
        }
        if self.scenario == "ignore" {
            loop {
                thread::sleep(Duration::from_millis(10));
            }
        }
        if self.scenario == "child" {
            while !stop.is_cancelled() {
                thread::sleep(Duration::from_millis(5));
            }
            stop.check()?;
        }
        if self.scenario == "env" {
            let excluded = [
                "AWS_ACCESS_KEY_ID",
                "AWS_SECRET_ACCESS_KEY",
                "GRV_ROOT",
                "GRV_STATE",
                "GOOGLE_APPLICATION_CREDENTIALS",
                "GRV_ADAPTERS_DIR",
            ];
            let message = if excluded.iter().all(|k| std::env::var_os(k).is_none()) {
                "clean"
            } else {
                "leaked"
            };
            return Ok(json!({"message":message}));
        }
        Ok(call.args.clone())
    }
    fn stop_and_wait(&mut self) -> Result<()> {
        if self.scenario == "abnormal" {
            std::process::exit(7);
        }
        if let Some(mut child) = self.child.take() {
            if self.scenario == "leak" {
                std::mem::forget(child);
            } else {
                let _ = child.kill();
                let _ = child.wait();
            }
        }
        self.stop.store(true, Ordering::Release);
        if let Some(t) = self.flood.take() {
            let _ = t.join();
        }
        Ok(())
    }
}
fn main() {
    let scenario = std::fs::read_to_string("scenario").unwrap_or_default();
    let stop = Arc::new(AtomicBool::new(false));
    let flood = if scenario == "flood" {
        let stop = stop.clone();
        Some(thread::spawn(move || {
            use std::io::Write;
            let data = b"credential-canary-raw-stderr\n".repeat(4096);
            while !stop.load(Ordering::Acquire) {
                if std::io::stderr().write_all(&data).is_err() {
                    break;
                }
            }
        }))
    } else {
        None
    };
    if grv_adapter_sdk::run_fd3(Adversary {
        scenario,
        child: None,
        stop,
        flood,
    })
    .is_err()
    {
        std::process::exit(1);
    }
}
