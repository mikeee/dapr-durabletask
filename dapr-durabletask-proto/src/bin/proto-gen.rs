use std::path::PathBuf;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let workspace_root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("..");
    let proto_dir = workspace_root.join("proto");
    let out_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("src/generated");

    let protos: Vec<PathBuf> = ["orchestrator_service.proto"]
        .iter()
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
