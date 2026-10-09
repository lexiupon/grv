# TODO

Known gaps that are tracked but not yet scheduled.

## Linux: trust Homebrew (Linuxbrew) adapter installs

On macOS, `grv` trusts adapters bundled beside the running binary at
`<prefix>/lib/grv/adapters`. Directories above that root may be writable by the
`admin` group (gid 80), as Homebrew's prefix is (see `Trust` in
`crates/grv-adapter-host/src/discovery.rs` and `check_packaged_ancestors` in
`crates/grv-adapter-duckdb/src/native_extensions.rs`).

There is no matching exception on Linux. Linuxbrew prefixes
(`/home/linuxbrew/.linuxbrew`) are usually owned by a dedicated `linuxbrew`
user and may be writable by a shared group. Group membership there does not
imply root, so the macOS reasoning does not carry over. Until this is designed:

- Linux bundled adapters are trusted only if every ancestor is owned by root,
  the caller or the owner of the running `grv`, and none is group- or
  world-writable.
- The Homebrew formula installs adapters only on Apple silicon macOS, and the
  release workflow builds only the `osx_arm64` adapter bundle.

To close this gap:

- Decide which Linuxbrew owner/group setups are trusted, and why.
- Build and publish `linux_amd64` and `linux_arm64` adapter bundles.
- Install them from the formula (`on_linux`) and test with Linuxbrew.

## Intel macOS adapter bundle

The `osx_amd64` bundle builds and verifies in CI but is not published or
installed by the formula yet. Add it to `.github/workflows/release.yml` and the
formula's `on_intel` block.
