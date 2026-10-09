# Homebrew packaging

`grv.rb` is the source of truth for the formula published in
[lexiupon/homebrew-tap](https://github.com/lexiupon/homebrew-tap).

- The CLI builds from the tagged source archive with Homebrew's `rust`.
- On Apple silicon, the `adapters` resource is the prebuilt
  `grv-adapters-X.Y.Z-osx_arm64.tar.gz` that
  [`.github/workflows/release.yml`](../../.github/workflows/release.yml)
  attaches to the GitHub release. It is installed into
  `<prefix>/lib/grv/adapters`, where `grv` looks for adapters bundled beside
  its own binary.

`grv` normally refuses adapter paths that a group can write to. Homebrew's
prefix is writable by the macOS `admin` group, so bundled adapters get a
narrow exception: only directories above `lib/grv/adapters` may be
admin-writable, and the adapter tree itself must not be. See `Trust` in
`crates/grv-adapter-host/src/discovery.rs`. Linux is not covered yet; see
[TODO.md](../../TODO.md).

## Releasing a new version

1. Bump `version` in the workspace `Cargo.toml`, commit, and tag `vX.Y.Z` on
   `main`. Push the tag. The Release adapters workflow builds the adapter
   bundle and attaches it to the release (creating a draft if needed).
   Then publish the release with notes.
2. In the tap repository, copy this `grv.rb` over `Formula/grv.rb` if it
   changed, then run `./scripts/update-grv-formula.py X.Y.Z`. It rewrites the
   source and adapter URLs and their SHA256s.
3. Check: `brew install --build-from-source lexiupon/tap/grv`,
   `brew test grv`, `brew audit --strict grv`. Commit and push the tap.

If you change the formula's structure, change it here first and copy it into
the tap.
