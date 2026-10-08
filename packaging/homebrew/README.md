# Homebrew packaging

`grv.rb` is the source of truth for the formula published in
[lexiupon/homebrew-tap](https://github.com/lexiupon/homebrew-tap). Like the
other formulae there, it builds the tagged source archive with Homebrew's
`rust`, so no prebuilt release artifacts are needed.

The formula installs the `grv` CLI only. Adapters (DuckDB, Salesforce) are
not packaged yet: the DuckDB adapter needs the pinned native guard and signed
extensions, which need either declared Homebrew resources or a prebuilt
bundle attached to the release.

## Releasing a new version

1. Bump `version` in the workspace `Cargo.toml`, commit, and tag `vX.Y.Z` on
   `main`. Push the tag and create the GitHub release.
2. In the tap repository, run `./scripts/update-grv-formula.py X.Y.Z`. It
   fetches the tag tarball and rewrites the URL and SHA256.
3. Check: `brew install --build-from-source lexiupon/tap/grv`,
   `brew test grv`, `brew audit --strict grv`. Commit and push the tap.

If you change the formula's structure, change it here first and copy it into
the tap.
