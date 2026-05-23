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

        rustExtensions = [ "rust-src" "rust-analyzer" "clippy" "rustfmt" ];

        # Build a Rust toolchain for a given version selector.
        # Accepted values:
        #   - "nightly"  → latest nightly (with extensions)
        #   - "stable"   → latest stable
        #   - "1.88.0"   → that exact stable release (MSRV)
        mkRustToolchain = rustVersion:
          if rustVersion == "nightly" then
            pkgs.rust-bin.selectLatestNightlyWith (toolchain:
              toolchain.default.override { extensions = rustExtensions; })
          else if rustVersion == "stable" then
            pkgs.rust-bin.stable.latest.default.override {
              extensions = rustExtensions;
            }
          else
            pkgs.rust-bin.stable.${rustVersion}.default.override {
              extensions = rustExtensions;
            };

        mkDevShell = rustVersion: pkgs.mkShell {
          buildInputs = [
            # Rust toolchain
            (mkRustToolchain rustVersion)

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
            # Build the durabletask-go sidecar into tmp/ if not already present.
            # Set DURABLETASK_SKIP_SIDECAR=1 to skip.
            SIDECAR_BIN="$PWD/tmp/durabletask-sidecar"
            if [ "''${DURABLETASK_SKIP_SIDECAR:-}" = "1" ]; then
              echo "Skipping durabletask-go sidecar build (DURABLETASK_SKIP_SIDECAR=1)"
            elif [ ! -f "$SIDECAR_BIN" ]; then
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
      in
      {
        # Named devShells let CI select a Rust toolchain via
        # `nix develop .#<name>` without touching the host's rustup.
        # The default shell pins MSRV (1.88).
        devShells = {
          default = mkDevShell "1.88.0";
          msrv = mkDevShell "1.88.0";
          stable = mkDevShell "stable";
          nightly = mkDevShell "nightly";
        };
      }
    );
}
