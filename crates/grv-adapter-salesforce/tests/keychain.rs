#![cfg(unix)]
use grv_adapter_salesforce::runtime::{Cancellation, ProcessSpec, run};
use std::{
    ffi::OsString,
    time::{Duration, Instant},
};
const GETTER: &str = include_str!("../src/keychain_getter.cjs");
const BOOTSTRAP: &str = include_str!("../src/keychain_bootstrap.cjs");
fn node(script: impl Into<OsString>, input: &[u8]) -> ProcessSpec {
    ProcessSpec {
        program: "node".into(),
        supervisor: Some(env!("CARGO_BIN_EXE_grv-adapter-salesforce").into()),
        args: vec!["-e".into(), script.into()],
        env: vec![
            ("NODE_OPTIONS".into(), "".into()),
            ("NODE_PATH".into(), "".into()),
        ],
        stdin: input.to_vec(),
        stdout_limit: 1024,
        timeout: Duration::from_secs(5),
    }
}
#[test]
fn getter_is_exact_and_mutation_is_refused_without_canary_output() {
    let script = format!(
        "{GETTER}\n{}",
        r#"
const key = require('node:fs').readFileSync(0,'utf8');
const implementation = { getPassword() { throw Error('original invoked') }, setPassword() { throw Error('original invoked') } };
installReadOnlyKeychain(implementation,key);
let okay = 0;
implementation.getPassword({ service:'sfdx', account:'local' }, (e,v) => { if (!e && v === key) okay++; });
for (const opts of [{service:'other',account:'local'}, {service:'sfdx',account:'other'}, {service:'sfdx',account:'local',password:key}, {}]) {
  implementation.getPassword(opts,(e,v) => { if (e && v === undefined && !e.message.includes(key)) okay++; });
}

implementation.setPassword({service:'sfdx',account:'local',password:key}, (e,v) => { if (e && v === undefined && !e.message.includes(key)) okay++; });
try { implementation.getPassword = () => {}; } catch (_) { okay++; }
if (okay !== 7) process.exitCode=1; else process.stdout.write('ok');
"#
    );
    let output = run(
        node(script, b"credential-key-canary"),
        &Cancellation::default(),
    )
    .unwrap();
    assert!(output.status.success());
    assert_eq!(output.stdout, b"ok");
}
#[test]
fn bootstrap_eof_missing_key_and_module_mismatch_return_without_leaking_key() {
    let script = format!("{GETTER}\n{BOOTSTRAP}");
    let platform = std::env::consts::OS;
    let invalid = serde_json::json!({"entry":"/invalid/bin/run.js","module":"/invalid/module.js","key":"credential-key-canary","platform":platform,"args":[]});
    let inputs = [
        vec![],
        br#"{"args":[],"entry":"/invalid","module":"/invalid","platform":"macos"}"#.to_vec(),
        serde_json::to_vec(&invalid).unwrap(),
    ];
    for input in inputs {
        let output = run(node(script.clone(), &input), &Cancellation::default()).unwrap();
        assert!(!output.status.success());
        assert!(output.stdout.is_empty());
    }
}
#[test]
fn cancelled_private_input_node_worker_stops_before_return() {
    let cancel = Cancellation::default();
    let worker_cancel = cancel.clone();
    let spec = node(
        "require('node:fs').readFileSync(0); setInterval(()=>{},1000)",
        b"credential-key-canary",
    );
    let start = Instant::now();
    let worker = std::thread::spawn(move || run(spec, &worker_cancel));
    std::thread::sleep(Duration::from_millis(100));
    cancel.cancel();
    let error = worker.join().unwrap().err().unwrap();
    assert_eq!(error.code, "EXTRACTION_INCOMPLETE");
    assert!(!error.to_string().contains("credential-key-canary"));
    assert!(start.elapsed() < Duration::from_secs(2));
}
#[test]
fn escaped_detached_child_creation_is_refused_under_real_helper_containment() {
    let script = r#"
const denied = e => {
  if(e.code === 'EPERM' || e.code === 'EACCES') process.stdout.write('denied');
  else { process.stdout.write('unexpected-code:' + String(e.code)); process.exitCode=1; }
};
try {
  const child = require('node:child_process').spawn('/bin/sleep',['30'], { detached:true, stdio:'ignore' });
  child.on('error', denied);
  child.on('spawn', () => { child.kill('SIGKILL'); process.exitCode=1; });
} catch(e) { denied(e); }
"#;
    let output = run(node(script, b""), &Cancellation::default()).unwrap();
    assert!(
        output.status.success(),
        "native denial probe status {:?}, static code {:?}",
        output.status,
        String::from_utf8_lossy(&output.stdout)
    );
    assert_eq!(output.stdout, b"denied");
}

#[test]
fn mismatched_key_cannot_decode_credentials_or_fall_back_to_key_creation() {
    let script = format!(
        "{GETTER}\n{}",
        r#"
const crypto = require('node:crypto');
const key = require('node:fs').readFileSync(0);
const original = crypto.createCipheriv('aes-256-gcm',Buffer.alloc(32,1),Buffer.alloc(12));
const encrypted = Buffer.concat([original.update('credential-token-canary'),original.final()]);
const implementation = {getPassword(){ throw Error('original getter invoked'); },setPassword(){throw Error('original setter invoked');}};
installReadOnlyKeychain(implementation,key.toString('hex'));
implementation.getPassword({service:'sfdx',account:'local'},(error,secret) => {
  if(error) {process.exitCode=1;return;}
  let refused=false;
  try {
    const decrypt=crypto.createDecipheriv('aes-256-gcm',Buffer.from(secret,'hex'),Buffer.alloc(12));
    decrypt.setAuthTag(original.getAuthTag());
    decrypt.update(encrypted); decrypt.final();
  } catch (_) {refused=true;}
  implementation.setPassword({service:'sfdx',account:'local',password:secret},(error) => {
    if(refused && error && !error.message.includes(secret)) process.stdout.write('refused'); else process.exitCode=1;
  });
});
"#
    );
    let output = run(node(script, &[2; 32]), &Cancellation::default()).unwrap();
    assert!(output.status.success());
    assert_eq!(output.stdout, b"refused");
}
