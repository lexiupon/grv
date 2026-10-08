class Grv < Formula
  desc "Versioned, auditable Parquet store with declarative push/pull adapters"
  homepage "https://github.com/lexiupon/grv"
  url "https://github.com/lexiupon/grv/archive/refs/tags/v0.1.0.tar.gz"
  sha256 "REPLACE_WITH_TAG_TARBALL_SHA256"
  license "Apache-2.0"
  head "https://github.com/lexiupon/grv.git", branch: "main"

  depends_on "rust" => :build

  def install
    system "cargo", "install", *std_cargo_args(path: "crates/grv")
  end

  def caveats
    <<~EOS
      This formula installs the grv CLI: store commands (init, ls, show, log,
      diff, verify, pin, gc, recover) on local, S3 and GCS roots, and the
      AI agent skill. Data adapters (DuckDB, Salesforce) are not included yet;
      see https://github.com/lexiupon/grv/blob/main/docs/building.md

      To teach your AI coding agent how to use grv:
        grv skills install -g
    EOS
  end

  test do
    assert_match version.to_s, shell_output("#{bin}/grv --version")
    system bin/"grv", "init", "--grv", testpath/"store"
    assert_match "grv-cli", shell_output("#{bin}/grv skills list --dir #{testpath}/skills")
  end
end
