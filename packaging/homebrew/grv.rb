class Grv < Formula
  desc "Versioned, auditable Parquet store with declarative push/pull adapters"
  homepage "https://github.com/lexiupon/grv"
  url "https://github.com/lexiupon/grv/archive/refs/tags/v0.2.0.tar.gz"
  sha256 "REPLACE_WITH_TAG_TARBALL_SHA256"
  license "Apache-2.0"
  head "https://github.com/lexiupon/grv.git", branch: "main"

  depends_on "rust" => :build

  # Prebuilt DuckDB and Salesforce adapters (pinned DuckDB native library and
  # signed extensions) built and verified by CI for this tag.
  on_macos do
    on_arm do
      resource "adapters" do
        url "https://github.com/lexiupon/grv/releases/download/v0.2.0/grv-adapters-0.2.0-osx_arm64.tar.gz"
        sha256 "REPLACE_WITH_ADAPTERS_SHA256"
      end
    end
  end

  def install
    system "cargo", "install", *std_cargo_args(path: "crates/grv")
    return unless OS.mac? && Hardware::CPU.arm?

    # grv discovers adapters at <prefix>/lib/grv/adapters next to bin/grv.
    resource("adapters").stage do
      (lib/"grv").install "adapters"
      (pkgshare/"notices").install Dir["notices/*"]
    end
  end

  def caveats
    <<~EOS
      The DuckDB and Salesforce adapters are installed on Apple silicon Macs;
      `grv adapter list` shows them. On other platforms only the CLI is
      installed; see https://github.com/lexiupon/grv/blob/main/docs/building.md

      To teach your AI coding agent how to use grv:
        grv skills install -g
    EOS
  end

  test do
    assert_match version.to_s, shell_output("#{bin}/grv --version")
    system bin/"grv", "init", "--grv", testpath/"store"
    assert_match "grv-cli", shell_output("#{bin}/grv skills list --dir #{testpath}/skills")
    if OS.mac? && Hardware::CPU.arm?
      ENV["HOME"] = testpath
      assert_match "\"pull\": true", shell_output("#{bin}/grv adapter duckdb capabilities")
    end
  end
end
