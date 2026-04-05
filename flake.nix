{
  description = "Dapr Durable Task Framework";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixpkgs-unstable";
    rust-overlay = {
      url = "github:oxalica/rust-overlay";
      inputs.nixpkgs.follows = "nixpkgs";
    };
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs = { self, nixpkgs, rust-overlay, flake-utils }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        overlays = [ (import rust-overlay) ];
        pkgs = import nixpkgs { inherit system overlays; };

        rustToolchain = pkgs.rust-bin.stable."1.85.0".default.override {
          extensions = [ "rust-src" "rust-analyzer" "clippy" "rustfmt" ];
        };
      in
      {
        devShells.default = pkgs.mkShell {
          buildInputs = [
            # Rust toolchain
            rustToolchain

            # Protobuf compiler (required by build.rs / tonic-build)
            pkgs.protobuf

            # Go (for building the durabletask-go sidecar)
            pkgs.go

            # Shell utilities
            pkgs.bash
            pkgs.coreutils
            pkgs.git

            # Useful dev tools
            pkgs.cargo-watch
            pkgs.cargo-nextest
          ];

          shellHook = ''
            # Build the durabletask-go sidecar into tmp/ if not already present
            SIDECAR_BIN="$PWD/tmp/durabletask-sidecar"
            if [ ! -f "$SIDECAR_BIN" ]; then
              echo "Building durabletask-go sidecar..."
              mkdir -p "$PWD/tmp"
              GOPATH="$PWD/tmp/go" go install github.com/dapr/durabletask-go@latest
              cp "$PWD/tmp/go/bin/durabletask-go" "$SIDECAR_BIN"
              echo "Sidecar built: $SIDECAR_BIN"
            fi
            export DURABLETASK_SIDECAR_BIN="$SIDECAR_BIN"

            echo "durabletask-rust dev shell"
            echo "  rustc:   $(rustc --version)"
            echo "  cargo:   $(cargo --version)"
            echo "  protoc:  $(protoc --version)"
            echo "  sidecar: $DURABLETASK_SIDECAR_BIN"
            echo ""
            echo "Commands:"
            echo "  cargo nextest run --workspace --all-features"
            echo "  cargo run -p dapr-durabletask-proto --features generate --bin proto-gen"
          '';

          # Ensure protoc is found by tonic-build
          PROTOC = "${pkgs.protobuf}/bin/protoc";
        };
      }
    );
}
