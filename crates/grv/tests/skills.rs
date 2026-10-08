//! `grv skills` end-to-end behaviour against a temporary directory.
use serde_json::Value;
use std::{fs, path::Path, process::Command};

fn grv(cwd: &Path, home: &Path, args: &[&str]) -> (i32, Value) {
    let output = Command::new(env!("CARGO_BIN_EXE_grv"))
        .arg("--json")
        .arg("skills")
        .args(args)
        .current_dir(cwd)
        .env("HOME", home)
        .output()
        .unwrap();
    let value: Value = serde_json::from_slice(&output.stdout)
        .unwrap_or_else(|_| panic!("not JSON: {}", String::from_utf8_lossy(&output.stdout)));
    let code = output.status.code().unwrap();
    assert_eq!(value["exit_status"], code);
    (code, value)
}

fn action(value: &Value) -> &str {
    value["result"]["skills"][0]["action"].as_str().unwrap()
}

#[test]
fn install_is_idempotent_and_protects_local_edits() {
    let temp = tempfile::tempdir().unwrap();
    let (cwd, home) = (temp.path(), temp.path().join("home"));
    let skill = cwd.join(".claude/skills/grv-cli");

    let (code, v) = grv(cwd, &home, &["install", "--dry-run"]);
    assert_eq!((code, action(&v)), (0, "would install"));
    assert!(!skill.exists(), "dry run must not write");

    let (code, v) = grv(cwd, &home, &["install"]);
    assert_eq!((code, action(&v)), (0, "installed"));
    assert!(skill.join("SKILL.md").is_file());
    assert!(skill.join("references/commands.md").is_file());
    assert!(skill.join(".grv-skill.json").is_file());

    let (_, v) = grv(cwd, &home, &["install", "grv-cli"]);
    assert_eq!(action(&v), "unchanged");

    // A hand edit is never overwritten without --force.
    fs::write(skill.join("SKILL.md"), "edited").unwrap();
    let (code, v) = grv(cwd, &home, &["install"]);
    assert_eq!(
        (code, v["errors"][0]["code"].as_str()),
        (3, Some("STATE_CONFLICT"))
    );
    assert_eq!(
        fs::read_to_string(skill.join("SKILL.md")).unwrap(),
        "edited"
    );
    let (_, v) = grv(cwd, &home, &["list"]);
    assert_eq!(v["result"]["skills"][0]["installed"], "modified");

    let (code, v) = grv(cwd, &home, &["install", "--force"]);
    assert_eq!((code, action(&v)), (0, "updated"));
    assert_ne!(
        fs::read_to_string(skill.join("SKILL.md")).unwrap(),
        "edited"
    );
}

#[test]
fn outdated_copy_from_grv_is_updated_without_force() {
    let temp = tempfile::tempdir().unwrap();
    let (cwd, home) = (temp.path(), temp.path().join("home"));
    let skill = cwd.join(".claude/skills/grv-cli");
    grv(cwd, &home, &["install"]);

    // Simulate a copy written by an older grv: different content whose hash
    // matches the marker it recorded.
    for entry in fs::read_dir(&skill).unwrap() {
        let path = entry.unwrap().path();
        if path.is_dir() {
            fs::remove_dir_all(path).unwrap()
        } else {
            fs::remove_file(path).unwrap()
        }
    }
    fs::write(skill.join("SKILL.md"), "old").unwrap();
    let (_, before) = grv(cwd, &home, &["list"]);
    assert_eq!(before["result"]["skills"][0]["installed"], "modified");
    // Recompute the digest the same way grv does: (len, path, len, bytes).
    use sha2::{Digest, Sha256};
    let mut h = Sha256::new();
    h.update(8u64.to_be_bytes());
    h.update(b"SKILL.md");
    h.update(3u64.to_be_bytes());
    h.update(b"old");
    let digest: String = h.finalize().iter().map(|b| format!("{b:02x}")).collect();
    fs::write(
        skill.join(".grv-skill.json"),
        serde_json::json!({"skill": "grv-cli", "grv_version": "0.0.0", "sha256": digest})
            .to_string(),
    )
    .unwrap();

    let (_, v) = grv(cwd, &home, &["list"]);
    assert_eq!(v["result"]["skills"][0]["installed"], "outdated");
    let (code, v) = grv(cwd, &home, &["install"]);
    assert_eq!((code, action(&v)), (0, "updated"));
}

#[test]
fn targets_and_errors() {
    let temp = tempfile::tempdir().unwrap();
    let (cwd, home) = (temp.path(), temp.path().join("home"));

    grv(cwd, &home, &["install", "-g", "--agent", "agents"]);
    assert!(home.join(".agents/skills/grv-cli/SKILL.md").is_file());

    let custom = cwd.join("custom");
    grv(cwd, &home, &["install", "--dir", custom.to_str().unwrap()]);
    assert!(custom.join("grv-cli/SKILL.md").is_file());

    let (code, _) = grv(cwd, &home, &["install", "--dir", "x", "-g"]);
    assert_eq!(code, 2);
    let (code, _) = grv(cwd, &home, &["install", "--agent", "cursor"]);
    assert_eq!(code, 2);
    let (code, v) = grv(cwd, &home, &["install", "no-such-skill"]);
    assert_eq!(
        (code, v["errors"][0]["code"].as_str()),
        (4, Some("NOT_FOUND"))
    );

    let (code, v) = grv(cwd, &home, &["show", "grv-cli"]);
    assert_eq!(code, 0);
    assert!(
        v["result"]["skill_md"]
            .as_str()
            .unwrap()
            .starts_with("---\nname: grv-cli\n")
    );

    let (code, v) = grv(
        cwd,
        &home,
        &["uninstall", "grv-cli", "-g", "--agent", "agents"],
    );
    assert_eq!((code, action(&v)), (0, "removed"));
    assert!(!home.join(".agents/skills/grv-cli").exists());
}

#[cfg(unix)]
#[test]
fn uninstall_removes_a_symlink_not_its_target() {
    let temp = tempfile::tempdir().unwrap();
    let (cwd, home) = (temp.path(), temp.path().join("home"));
    let elsewhere = cwd.join("elsewhere");
    fs::create_dir_all(&elsewhere).unwrap();
    fs::write(elsewhere.join("keep.txt"), "keep").unwrap();
    fs::create_dir_all(cwd.join(".claude/skills")).unwrap();
    std::os::unix::fs::symlink(&elsewhere, cwd.join(".claude/skills/grv-cli")).unwrap();

    let (code, _) = grv(cwd, &home, &["uninstall", "grv-cli"]);
    assert_eq!(
        code, 3,
        "a foreign symlink is not ours to remove without --force"
    );
    let (code, _) = grv(cwd, &home, &["uninstall", "grv-cli", "--force"]);
    assert_eq!(code, 0);
    assert!(
        elsewhere.join("keep.txt").is_file(),
        "symlink target must survive"
    );
}
