# Homebrew packaging

`grv.rb` is a formula **template**. It is not published yet.

To publish:

1. Create a tap repository (for example `OWNER/homebrew-grv`).
2. Attach the release bundle tarball built by `scripts/package.py` to a
   GitHub release.
3. Copy `grv.rb` to `Formula/grv.rb` in the tap, replace `OWNER`, the URL and
   `sha256` (`shasum -a 256 <tarball>`), and commit.
4. Check it with `brew install --build-from-source OWNER/grv/grv` and
   `brew test grv`.

Only macOS ARM64 is covered today, matching the initial release scope. Add
`on_intel`/`on_linux` blocks as those platforms are qualified.

Open question to verify against the first real bundle: whether
`grv adapter install` accepts a source directory under the Homebrew prefix
(it copies the adapter into `~/.config/grv/adapters`, which must itself be
protected).
