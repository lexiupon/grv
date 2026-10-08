//! Embeds the repository's `skills/` tree into the binary.
use std::{env, fs, path::Path};
fn collect(root: &Path, dir: &Path, out: &mut Vec<String>) {
    let mut entries: Vec<_> = fs::read_dir(dir)
        .unwrap()
        .map(|e| e.unwrap().path())
        .collect();
    entries.sort();
    for path in entries {
        let name = path.file_name().unwrap().to_string_lossy().into_owned();
        if name.starts_with('.') {
            continue;
        }
        if path.is_dir() {
            collect(root, &path, out);
        } else {
            out.push(
                path.strip_prefix(root)
                    .unwrap()
                    .to_string_lossy()
                    .replace('\\', "/"),
            );
        }
    }
}
fn main() {
    let manifest = env::var("CARGO_MANIFEST_DIR").unwrap();
    let root = Path::new(&manifest)
        .join("../../skills")
        .canonicalize()
        .unwrap();
    println!("cargo:rerun-if-changed={}", root.display());
    let mut files = Vec::new();
    collect(&root, &root, &mut files);
    for file in &files {
        println!("cargo:rerun-if-changed={}", root.join(file).display());
    }
    let mut code = String::from("pub(crate) static FILES: &[(&str, &[u8])] = &[\n");
    for file in &files {
        code.push_str(&format!(
            "    ({:?}, include_bytes!({:?})),\n",
            file,
            root.join(file).display().to_string()
        ));
    }
    code.push_str("];\n");
    fs::write(
        Path::new(&env::var("OUT_DIR").unwrap()).join("skills.rs"),
        code,
    )
    .unwrap();
}
