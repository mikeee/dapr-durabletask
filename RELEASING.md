# Releasing

Both crates are released from per-line **release branches** so that the
git tag, the `Cargo.toml` version, and what ends up on crates.io are
guaranteed to agree. Pushing a hand-crafted tag against `main` is **not**
the supported path.

## Release branches

| Crate                     | Release branch pattern        | Tag pattern                          |
| ------------------------- | ----------------------------- | ------------------------------------ |
| `dapr-durabletask-proto`  | `release-proto-MAJOR.MINOR`   | `dapr-durabletask-proto-vMAJOR.MINOR.PATCH` |
| `dapr-durabletask`        | `release-MAJOR.MINOR`         | `dapr-durabletask-vMAJOR.MINOR.PATCH`       |

Each release branch is the source of truth for its `MAJOR.MINOR` line.
Its `Cargo.toml` version always reflects the last released patch on that
line. Fixes that need to ship are merged into the release branch
(usually backported from `main`).

## Cutting a release

1. Make sure all fixes for the patch are on the release branch and CI is
   green.
2. In the Actions tab, run the appropriate **Cut release** workflow with
   the release branch selected as the ref:
   - `Cut release (dapr-durabletask-proto)` for `release-proto-*` branches.
   - `Cut release (dapr-durabletask)` for `release-*` branches.
3. The workflow will:
   - Verify the branch name matches `release[-proto]-MAJOR.MINOR`.
   - Verify `Cargo.toml` is on the `MAJOR.MINOR.*` line.
   - Bump the patch component.
   - Commit the bump to the release branch and push.
   - Tag the bump commit `<crate>-vMAJOR.MINOR.PATCH+1` and push the tag.
4. The tag push fires the existing publish workflow
   (`publish-dapr-durabletask[-proto].yml`), which runs `cargo publish`
   and creates the GitHub release.
5. For the proto crate, `proto-dep-bump.yml` additionally opens an
   `auto/bump-proto-dep-<version>-into-<branch>` PR against **every**
   branch whose `dapr-durabletask/Cargo.toml` currently depends on the
   same proto `MAJOR.MINOR` line — `main` plus every matching
   `release-MAJOR.MINOR` branch. Merge each PR into the corresponding
   branch before cutting the next `dapr-durabletask` patch from it.

## Starting a new MAJOR.MINOR line

1. Branch from `main` (or wherever you want the line to start) and push
   it as `release-MAJOR.MINOR` (or `release-proto-MAJOR.MINOR`).
2. Set the crate's `Cargo.toml` version to `MAJOR.MINOR.0` on the new
   branch and commit. The next cut will release `MAJOR.MINOR.1`.
3. Bump the `MAJOR.MINOR` on `main` so it tracks the next development
   line.

## Upstream proto sync

`proto-sync.yml` still targets `main`: when upstream
`dapr/durabletask-protobuf` drifts, it regenerates protos and opens a
patch-bump PR against `main`. Bringing those changes into a release line
is a manual cherry-pick onto the appropriate `release-proto-MAJOR.MINOR`
branch followed by a normal **Cut release** run.