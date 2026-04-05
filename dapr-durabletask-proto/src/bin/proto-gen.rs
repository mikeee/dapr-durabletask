use std::path::PathBuf;
use std::process::Command;

const PROTO_REPO: &str = "https://github.com/dapr/durabletask-protobuf.git";
const PROTO_BRANCH: &str = "main";

/// Proto files needed from the upstream repository.
const PROTO_FILES: &[&str] = &[
    "orchestrator_service.proto",
    "orchestration.proto",
    "history_events.proto",
    "orchestrator_actions.proto",
];

/// Shallow-clone the upstream proto repository and copy the required `.proto`
/// files into the workspace `proto/` directory, replacing any existing copies.
fn fetch_protos(proto_dir: &PathBuf) -> Result<(), Box<dyn std::error::Error>> {
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

    std::fs::create_dir_all(proto_dir)?;

    for file in PROTO_FILES {
        let src = clone_dir.join("protos").join(file);
        let dst = proto_dir.join(file);
        if !src.exists() {
            return Err(format!("expected proto file not found: {}", src.display()).into());
        }
        std::fs::copy(&src, &dst)?;
        println!("  {} → {}", src.display(), dst.display());
    }

    Ok(())
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let workspace_root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("..");
    let proto_dir = workspace_root.join("proto");
    let out_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("src/generated");

    fetch_protos(&proto_dir)?;

    let protos: Vec<PathBuf> = PROTO_FILES
        .iter()
        .filter(|f| **f == "orchestrator_service.proto")
        .map(|f| proto_dir.join(f))
        .collect();

    std::fs::create_dir_all(&out_dir)?;

    tonic_prost_build::configure()
        .build_server(false)
        .build_transport(true)
        .out_dir(&out_dir)
        .compile_protos(&protos, &[proto_dir])?;

    // Rename the generated file to mod.rs
    let generated = out_dir.join("_.rs");
    let target = out_dir.join("mod.rs");
    if generated.exists() {
        std::fs::rename(&generated, &target)?;
    }

    println!("Proto files regenerated in {}", out_dir.display());
    Ok(())
}
