use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::process::Command;

const PROTO_REPO: &str = "https://github.com/dapr/durabletask-protobuf.git";
const PROTO_BRANCH: &str = "main";
/// Subdirectory inside the upstream repo containing the `.proto` files.
const UPSTREAM_PROTO_SUBDIR: &str = "protos";

/// Shallow-clone the upstream proto repository and mirror *every* `.proto`
/// file from `protos/` into the workspace `proto/` directory, replacing any
/// existing copies. Returns the list of mirrored proto file names.
fn fetch_protos(proto_dir: &Path) -> Result<Vec<PathBuf>, Box<dyn std::error::Error>> {
    let tmp_dir = tempfile::tempdir()?;
    let clone_dir = tmp_dir.path().join("durabletask-protobuf");

    println!("Fetching protos from {PROTO_REPO} @ {PROTO_BRANCH}");

    let status = Command::new("git")
        .args([
            "clone",
            "--depth",
            "1",
            "--branch",
            PROTO_BRANCH,
            "--single-branch",
            PROTO_REPO,
            clone_dir.to_str().unwrap(),
        ])
        .status()?;

    if !status.success() {
        return Err(format!("git clone failed with status {status}").into());
    }

    let upstream_dir = clone_dir.join(UPSTREAM_PROTO_SUBDIR);
    if !upstream_dir.is_dir() {
        return Err(format!(
            "upstream proto directory not found: {}",
            upstream_dir.display()
        )
        .into());
    }

    // Clean the destination of any previously-mirrored .proto files so that
    // removals upstream are reflected locally.
    if proto_dir.is_dir() {
        for entry in std::fs::read_dir(proto_dir)? {
            let entry = entry?;
            if entry.path().extension().and_then(|e| e.to_str()) == Some("proto") {
                std::fs::remove_file(entry.path())?;
            }
        }
    }
    std::fs::create_dir_all(proto_dir)?;

    let mut copied = Vec::new();
    for entry in std::fs::read_dir(&upstream_dir)? {
        let entry = entry?;
        let src = entry.path();
        if src.extension().and_then(|e| e.to_str()) != Some("proto") {
            continue;
        }
        let file_name = src
            .file_name()
            .ok_or("proto file has no name")?
            .to_owned();
        let dst = proto_dir.join(&file_name);
        std::fs::copy(&src, &dst)?;
        println!("  {} → {}", src.display(), dst.display());
        copied.push(PathBuf::from(file_name));
    }

    copied.sort();
    Ok(copied)
}

/// Build a `mod.rs` that wires up every per-package file emitted by
/// `tonic-prost-build`. Files are named after their proto package
/// (`a.b.c.rs`), with the default/empty package written to `_.rs`. The
/// default package is `include!`d at the crate root so existing call-sites
/// (e.g. `crate::proto::TaskHubSidecarServiceClient`) keep working, and other
/// packages are exposed under nested `pub mod` paths matching their package
/// names.
fn write_mod_rs(out_dir: &Path) -> Result<(), Box<dyn std::error::Error>> {
    // Collect every generated file: package-name -> filename.
    let mut packages: BTreeMap<String, String> = BTreeMap::new();
    for entry in std::fs::read_dir(out_dir)? {
        let entry = entry?;
        let path = entry.path();
        if path.extension().and_then(|e| e.to_str()) != Some("rs") {
            continue;
        }
        let stem = path
            .file_stem()
            .and_then(|s| s.to_str())
            .ok_or("invalid generated filename")?;
        if stem == "mod" {
            continue;
        }
        let file_name = path
            .file_name()
            .and_then(|s| s.to_str())
            .ok_or("invalid generated filename")?
            .to_owned();
        packages.insert(stem.to_owned(), file_name);
    }

    // Build a nested module tree from the dotted package names. Each node may
    // hold a generated file (if a package with that exact name exists) and
    // any child packages.
    #[derive(Default)]
    struct Node {
        file: Option<String>,
        children: BTreeMap<String, Node>,
    }

    let mut root = Node::default();
    for (pkg, file) in &packages {
        if pkg == "_" {
            // Default/empty package — included at the crate root.
            root.file = Some(file.clone());
            continue;
        }
        let mut cur = &mut root;
        for segment in pkg.split('.') {
            cur = cur.children.entry(segment.to_owned()).or_default();
        }
        cur.file = Some(file.clone());
    }

    fn emit(node: &Node, out: &mut String, depth: usize) {
        let indent = "    ".repeat(depth);
        if let Some(file) = &node.file {
            out.push_str(&format!("{indent}include!(\"{file}\");\n"));
        }
        for (name, child) in &node.children {
            out.push_str(&format!("{indent}pub mod {name} {{\n"));
            emit(child, out, depth + 1);
            out.push_str(&format!("{indent}}}\n"));
        }
    }

    let mut body = String::new();
    body.push_str("// @generated by proto-gen — do not edit by hand.\n");
    emit(&root, &mut body, 0);

    std::fs::write(out_dir.join("mod.rs"), body)?;
    Ok(())
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let workspace_root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("..");
    let proto_dir = workspace_root.join("proto");
    let out_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("src/generated");

    let proto_files = fetch_protos(&proto_dir)?;
    if proto_files.is_empty() {
        return Err("no .proto files found upstream".into());
    }

    // Wipe previously-generated Rust sources so removed packages don't linger.
    if out_dir.is_dir() {
        for entry in std::fs::read_dir(&out_dir)? {
            let entry = entry?;
            if entry.path().extension().and_then(|e| e.to_str()) == Some("rs") {
                std::fs::remove_file(entry.path())?;
            }
        }
    }
    std::fs::create_dir_all(&out_dir)?;

    let protos: Vec<PathBuf> = proto_files.iter().map(|f| proto_dir.join(f)).collect();

    tonic_prost_build::configure()
        .build_server(false)
        .build_transport(true)
        .out_dir(&out_dir)
        .compile_protos(&protos, &[proto_dir])?;

    write_mod_rs(&out_dir)?;

    println!("Proto files regenerated in {}", out_dir.display());
    Ok(())
}
