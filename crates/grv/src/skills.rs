//! `grv skills`: install the agent skills bundled into this binary.
//!
//! Skills are local tooling, not part of the client v1 data contract, so this
//! command sits outside the versioned result envelope. With `--json` it prints
//! one `{ok, exit_status, command, result, errors}` object; exit statuses reuse the client
//! meanings (0 success, 2 invalid request, 3 conflict, 4 not found, 6 I/O).
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    fs,
    io::Write,
    path::{Path, PathBuf},
};

mod bundled {
    include!(concat!(env!("OUT_DIR"), "/skills.rs"));
}

const MARKER: &str = ".grv-skill.json";
const USAGE: &str = "usage: grv skills list [--agent claude|agents] [-g|--global] [--dir <path>]
       grv skills show <name>
       grv skills install [<name>...] [--agent claude|agents] [-g|--global] [--dir <path>] [--force] [--dry-run]
       grv skills uninstall <name>... [--agent claude|agents] [-g|--global] [--dir <path>] [--force]";

struct Fail {
    code: &'static str,
    exit: i32,
    message: String,
}
fn fail(code: &'static str, exit: i32, message: impl Into<String>) -> Fail {
    Fail {
        code,
        exit,
        message: message.into(),
    }
}
fn invalid(message: impl Into<String>) -> Fail {
    fail("INVALID_ARGUMENT", 2, message)
}
fn io(context: &Path, error: std::io::Error) -> Fail {
    fail(
        "BACKEND_FAILURE",
        6,
        format!("{}: {error}", context.display()),
    )
}

/// One bundled skill: its name and files relative to the skill directory.
struct Skill {
    name: String,
    files: BTreeMap<String, &'static [u8]>,
}
impl Skill {
    fn digest(&self) -> String {
        digest_files(self.files.iter().map(|(k, v)| (k.as_str(), *v)))
    }
    fn description(&self) -> String {
        frontmatter(self.files.get("SKILL.md").copied().unwrap_or_default())
            .get("description")
            .cloned()
            .unwrap_or_default()
    }
}
fn digest_files<'a>(files: impl Iterator<Item = (&'a str, &'a [u8])>) -> String {
    let mut hasher = Sha256::new();
    for (path, bytes) in files {
        hasher.update((path.len() as u64).to_be_bytes());
        hasher.update(path.as_bytes());
        hasher.update((bytes.len() as u64).to_be_bytes());
        hasher.update(bytes);
    }
    hasher
        .finalize()
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}

fn bundled() -> Vec<Skill> {
    let mut skills: BTreeMap<String, Skill> = BTreeMap::new();
    for (path, bytes) in bundled::FILES {
        let Some((name, rest)) = path.split_once('/') else {
            continue; // files at the root of skills/ (e.g. a README) are not skills
        };
        skills
            .entry(name.to_string())
            .or_insert_with(|| Skill {
                name: name.to_string(),
                files: BTreeMap::new(),
            })
            .files
            .insert(rest.to_string(), bytes);
    }
    skills
        .into_values()
        .filter(|s| s.files.contains_key("SKILL.md"))
        .collect()
}

/// Minimal parser for the YAML frontmatter of a SKILL.md: top-level scalar
/// keys, including `>-`/`|` block scalars. Enough for name and description.
pub(crate) fn frontmatter(bytes: &[u8]) -> BTreeMap<String, String> {
    let mut out = BTreeMap::new();
    let text = String::from_utf8_lossy(bytes);
    let mut lines = text.lines();
    if lines.next() != Some("---") {
        return out;
    }
    let mut key: Option<(String, bool)> = None; // (key, folded)
    let mut buffer: Vec<String> = Vec::new();
    let flush = |out: &mut BTreeMap<String, String>,
                 key: &Option<(String, bool)>,
                 buffer: &mut Vec<String>| {
        if let Some((k, folded)) = key {
            let joined = if *folded {
                buffer.join(" ")
            } else {
                buffer.join("\n")
            };
            out.insert(k.clone(), joined.trim().to_string());
        }
        buffer.clear();
    };
    for line in lines {
        if line == "---" {
            break;
        }
        if key.is_some() && (line.starts_with(' ') || line.is_empty()) {
            buffer.push(line.trim().to_string());
            continue;
        }
        flush(&mut out, &key, &mut buffer);
        key = None;
        if let Some((k, v)) = line.split_once(':') {
            let v = v.trim();
            match v {
                ">" | ">-" | "|" | "|-" => key = Some((k.trim().to_string(), v.starts_with('>'))),
                _ => {
                    out.insert(
                        k.trim().to_string(),
                        v.trim_matches(|c| c == '"' || c == '\'').to_string(),
                    );
                }
            }
        }
    }
    flush(&mut out, &key, &mut buffer);
    out
}

struct Target {
    agent: &'static str,
    global: bool,
    dir: Option<PathBuf>,
}
impl Target {
    fn root(&self) -> Result<PathBuf, Fail> {
        if let Some(dir) = &self.dir {
            return Ok(dir.clone());
        }
        let base = if self.global {
            PathBuf::from(
                std::env::var_os("HOME")
                    .filter(|h| !h.is_empty())
                    .ok_or_else(|| invalid("HOME is not set; use --dir <path>"))?,
            )
        } else {
            std::env::current_dir().map_err(|e| io(Path::new("."), e))?
        };
        Ok(base.join(format!(".{}", self.agent)).join("skills"))
    }
}

struct Options {
    names: Vec<String>,
    target: Target,
    force: bool,
    dry_run: bool,
}
fn parse(args: &[String], allow: &[&str]) -> Result<Options, Fail> {
    let mut options = Options {
        names: vec![],
        target: Target {
            agent: "claude",
            global: false,
            dir: None,
        },
        force: false,
        dry_run: false,
    };
    let mut agent_set = false;
    let mut iter = args.iter();
    while let Some(arg) = iter.next() {
        let flag = arg.split_once('=').map_or(arg.as_str(), |(f, _)| f);
        if arg.starts_with('-') && !allow.contains(&flag) {
            return Err(invalid(format!("unknown flag {arg}\n{USAGE}")));
        }
        let mut value = || -> Result<String, Fail> {
            match arg.split_once('=') {
                Some((_, v)) => Ok(v.to_string()),
                None => iter
                    .next()
                    .cloned()
                    .ok_or_else(|| invalid(format!("{arg} requires a value"))),
            }
        };
        match flag {
            "--agent" => {
                options.target.agent = match value()?.as_str() {
                    "claude" => "claude",
                    "agents" => "agents",
                    other => {
                        return Err(invalid(format!(
                            "unknown agent layout {other:?}; use claude, agents or --dir <path>"
                        )));
                    }
                };
                agent_set = true;
            }
            "-g" | "--global" => options.target.global = true,
            "--dir" => options.target.dir = Some(PathBuf::from(value()?)),
            "--force" => options.force = true,
            "--dry-run" => options.dry_run = true,
            _ => options.names.push(arg.clone()),
        }
    }
    if options.target.dir.is_some() && (agent_set || options.target.global) {
        return Err(invalid("--dir cannot be combined with --agent or --global"));
    }
    Ok(options)
}

fn select<'a>(
    skills: &'a [Skill],
    names: &[String],
    default_all: bool,
) -> Result<Vec<&'a Skill>, Fail> {
    if names.is_empty() {
        if default_all {
            return Ok(skills.iter().collect());
        }
        return Err(invalid(format!("a skill name is required\n{USAGE}")));
    }
    names
        .iter()
        .map(|name| {
            skills.iter().find(|s| &s.name == name).ok_or_else(|| {
                let known: Vec<_> = skills.iter().map(|s| s.name.as_str()).collect();
                fail(
                    "NOT_FOUND",
                    4,
                    format!(
                        "no bundled skill named {name:?} (available: {})",
                        known.join(", ")
                    ),
                )
            })
        })
        .collect()
}

/// State of a skill directory relative to the bundled copy.
#[derive(PartialEq)]
enum State {
    Absent,
    Current,
    /// Installed by grv, unmodified since, but from a different version.
    Stale,
    /// Present, but edited by hand or not installed by grv.
    Modified,
}
impl State {
    fn label(&self) -> &'static str {
        match self {
            State::Absent => "absent",
            State::Current => "current",
            State::Stale => "outdated",
            State::Modified => "modified",
        }
    }
}

fn read_tree(dir: &Path) -> Result<BTreeMap<String, Vec<u8>>, Fail> {
    fn walk(root: &Path, dir: &Path, out: &mut BTreeMap<String, Vec<u8>>) -> Result<(), Fail> {
        for entry in fs::read_dir(dir).map_err(|e| io(dir, e))? {
            let path = entry.map_err(|e| io(dir, e))?.path();
            let meta = fs::symlink_metadata(&path).map_err(|e| io(&path, e))?;
            let rel = path
                .strip_prefix(root)
                .unwrap()
                .to_string_lossy()
                .replace('\\', "/");
            if rel == MARKER {
                continue;
            }
            if meta.file_type().is_symlink() {
                out.insert(rel, b"\0symlink".to_vec()); // never matches real content
            } else if meta.is_dir() {
                walk(root, &path, out)?;
            } else {
                out.insert(rel, fs::read(&path).map_err(|e| io(&path, e))?);
            }
        }
        Ok(())
    }
    let mut out = BTreeMap::new();
    walk(dir, dir, &mut out)?;
    Ok(out)
}

fn state(dir: &Path, skill: &Skill) -> Result<State, Fail> {
    match fs::symlink_metadata(dir) {
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(State::Absent),
        Err(e) => return Err(io(dir, e)),
        Ok(meta) if !meta.is_dir() => return Ok(State::Modified),
        Ok(_) => {}
    }
    let tree = read_tree(dir)?;
    let actual = digest_files(tree.iter().map(|(k, v)| (k.as_str(), v.as_slice())));
    if actual == skill.digest() {
        return Ok(State::Current);
    }
    let recorded = fs::read(dir.join(MARKER))
        .ok()
        .and_then(|b| serde_json::from_slice::<Value>(&b).ok())
        .and_then(|v| v["sha256"].as_str().map(str::to_string));
    Ok(if recorded.as_deref() == Some(actual.as_str()) {
        State::Stale
    } else {
        State::Modified
    })
}

fn write_skill(dir: &Path, skill: &Skill) -> Result<(), Fail> {
    let parent = dir.parent().expect("skill dir has a parent");
    fs::create_dir_all(parent).map_err(|e| io(parent, e))?;
    // Stage beside the destination, then swap it in, so an interrupted install
    // never leaves a half-written skill behind.
    let staging = tempfile::Builder::new()
        .prefix(&format!(".{}.", skill.name))
        .tempdir_in(parent)
        .map_err(|e| io(parent, e))?;
    for (rel, bytes) in &skill.files {
        let path = staging.path().join(rel);
        fs::create_dir_all(path.parent().unwrap()).map_err(|e| io(&path, e))?;
        fs::File::create(&path)
            .and_then(|mut f| f.write_all(bytes))
            .map_err(|e| io(&path, e))?;
    }
    let marker = json!({
        "skill": skill.name,
        "grv_version": env!("CARGO_PKG_VERSION"),
        "sha256": skill.digest(),
    });
    fs::write(
        staging.path().join(MARKER),
        serde_json::to_vec_pretty(&marker).unwrap(),
    )
    .map_err(|e| io(dir, e))?;
    remove(dir)?;
    let staged = staging.keep();
    fs::rename(&staged, dir).map_err(|e| {
        let _ = fs::remove_dir_all(&staged);
        io(dir, e)
    })
}

fn remove(path: &Path) -> Result<(), Fail> {
    match fs::symlink_metadata(path) {
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(io(path, e)),
        // Remove a symlink itself, never what it points to.
        Ok(meta) if meta.is_dir() => fs::remove_dir_all(path).map_err(|e| io(path, e)),
        Ok(_) => fs::remove_file(path).map_err(|e| io(path, e)),
    }
}

fn run(args: &[String]) -> Result<(&'static str, Value, String), Fail> {
    let skills = bundled();
    let verb = args.first().map(String::as_str);
    let rest = args.get(1..).unwrap_or_default();
    match verb {
        Some("list") => {
            let o = parse(rest, &["--agent", "-g", "--global", "--dir"])?;
            if !o.names.is_empty() {
                return Err(invalid("skills list takes no names"));
            }
            let root = o.target.root()?;
            let mut items = vec![];
            let mut text = format!("Skills directory: {}\n", root.display());
            for s in &skills {
                let st = state(&root.join(&s.name), s)?;
                text.push_str(&format!(
                    "  {:<12} {:<9} {}\n",
                    s.name,
                    st.label(),
                    s.description()
                ));
                items.push(json!({"name": s.name, "description": s.description(), "installed": st.label()}));
            }
            Ok((
                "skills list",
                json!({"directory": root, "skills": items}),
                text,
            ))
        }
        Some("show") => {
            let o = parse(rest, &[])?;
            if o.names.len() != 1 {
                return Err(invalid("skills show takes exactly one name"));
            }
            let skill = select(&skills, &o.names, false)?[0];
            let body = String::from_utf8_lossy(skill.files["SKILL.md"]).into_owned();
            Ok((
                "skills show",
                json!({"name": skill.name, "files": skill.files.keys().collect::<Vec<_>>(), "skill_md": body}),
                body,
            ))
        }
        Some("install") => {
            let o = parse(
                rest,
                &["--agent", "-g", "--global", "--dir", "--force", "--dry-run"],
            )?;
            let root = o.target.root()?;
            let chosen = select(&skills, &o.names, true)?;
            // Check every skill before writing any, so a conflict leaves nothing half done.
            let mut plan = vec![];
            for s in chosen {
                let dir = root.join(&s.name);
                let st = state(&dir, s)?;
                if st == State::Modified && !o.force {
                    return Err(fail(
                        "STATE_CONFLICT",
                        3,
                        format!(
                            "{} exists and was modified or not installed by grv; rerun with --force to overwrite it",
                            dir.display()
                        ),
                    ));
                }
                plan.push((s, dir, st));
            }
            let mut items = vec![];
            let mut text = String::new();
            for (s, dir, st) in plan {
                let action = match (&st, o.dry_run) {
                    (State::Current, _) => "unchanged",
                    (_, true) => "would install",
                    _ => {
                        write_skill(&dir, s)?;
                        if st == State::Absent {
                            "installed"
                        } else {
                            "updated"
                        }
                    }
                };
                text.push_str(&format!("{action}: {} -> {}\n", s.name, dir.display()));
                items.push(
                    json!({"name": s.name, "path": dir, "action": action, "previous": st.label()}),
                );
            }
            Ok((
                "skills install",
                json!({"directory": root, "dry_run": o.dry_run, "skills": items}),
                text,
            ))
        }
        Some("uninstall") => {
            let o = parse(rest, &["--agent", "-g", "--global", "--dir", "--force"])?;
            let root = o.target.root()?;
            let chosen = select(&skills, &o.names, false)?;
            let mut plan = vec![];
            for s in chosen {
                let dir = root.join(&s.name);
                let st = state(&dir, s)?;
                if st == State::Modified && !o.force {
                    return Err(fail(
                        "STATE_CONFLICT",
                        3,
                        format!(
                            "{} was modified or not installed by grv; rerun with --force to remove it",
                            dir.display()
                        ),
                    ));
                }
                plan.push((s, dir, st));
            }
            let mut items = vec![];
            let mut text = String::new();
            for (s, dir, st) in plan {
                let action = if st == State::Absent {
                    "absent"
                } else {
                    remove(&dir)?;
                    "removed"
                };
                text.push_str(&format!("{action}: {} ({})\n", s.name, dir.display()));
                items.push(json!({"name": s.name, "path": dir, "action": action}));
            }
            Ok((
                "skills uninstall",
                json!({"directory": root, "skills": items}),
                text,
            ))
        }
        _ => Err(invalid(USAGE)),
    }
}

/// Entry point for `grv [--json] skills ...`; never returns.
pub(crate) fn main(args: &[String], json: bool) -> ! {
    let command = match args.first().map(String::as_str) {
        Some(v @ ("list" | "show" | "install" | "uninstall")) => format!("skills {v}"),
        _ => "skills".to_string(),
    };
    match run(args) {
        Ok((_, result, text)) => {
            if json {
                println!(
                    "{}",
                    json!({"ok": true, "exit_status": 0, "command": command, "result": result, "errors": []})
                );
            } else {
                print!("{text}");
                if !text.ends_with('\n') {
                    println!();
                }
            }
            std::process::exit(0)
        }
        Err(f) => {
            if json {
                println!(
                    "{}",
                    json!({"ok": false, "exit_status": f.exit, "command": command, "result": null,
                           "errors": [{"code": f.code, "message": f.message}]})
                );
            } else {
                eprintln!("{}", f.message);
            }
            std::process::exit(f.exit)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_bundled_skill_has_valid_frontmatter() {
        let skills = bundled();
        assert!(skills.iter().any(|s| s.name == "grv-cli"));
        for s in &skills {
            let fm = frontmatter(s.files["SKILL.md"]);
            assert_eq!(
                fm.get("name"),
                Some(&s.name),
                "{}: name must match directory",
                s.name
            );
            let d = fm.get("description").cloned().unwrap_or_default();
            assert!(
                !d.is_empty() && d.len() <= 1024,
                "{}: description must be 1-1024 chars",
                s.name
            );
        }
    }

    #[test]
    fn bundled_skill_links_resolve() {
        for s in bundled() {
            for (path, bytes) in &s.files {
                if !path.ends_with(".md") {
                    continue;
                }
                let text = String::from_utf8_lossy(bytes);
                let base = Path::new(path).parent().unwrap();
                for link in text.split("](").skip(1).filter_map(|t| t.split(')').next()) {
                    if link.contains("://") || link.starts_with('#') {
                        continue;
                    }
                    let target = base.join(link.split('#').next().unwrap());
                    let norm = target.to_string_lossy().replace('\\', "/");
                    assert!(
                        s.files.contains_key(norm.as_str()),
                        "{}/{path}: broken link {link}",
                        s.name
                    );
                }
            }
        }
    }

    #[test]
    fn frontmatter_parses_folded_description() {
        let fm = frontmatter(b"---\nname: x\ndescription: >-\n  one\n  two\n---\nbody");
        assert_eq!(fm["name"], "x");
        assert_eq!(fm["description"], "one two");
    }
}
