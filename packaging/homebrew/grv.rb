# Homebrew formula template for GRV. Copy into the tap repository
# (Formula/grv.rb) and fill in url/sha256 from the published release bundle.
class Grv < Formula
  desc "Versioned, auditable Parquet store with declarative push/pull adapters"
  homepage "https://github.com/OWNER/grv"
  version "0.1.0"
  license "Apache-2.0"

  on_macos do
    on_arm do
      url "https://github.com/OWNER/grv/releases/download/v0.1.0/grv-0.1.0-macos-arm64.tar.gz"
      sha256 "REPLACE_WITH_BUNDLE_SHA256"
    end
  end

  def install
    # The relocatable bundle from scripts/package.py: bin/, adapters/, notices/.
    libexec.install Dir["*"]
    bin.install_symlink libexec/"bin/grv"
  end

  def caveats
    <<~EOS
      Adapters must run from a protected directory, which the Homebrew prefix
      is not. Copy them into your user adapter directory once (and again after
      upgrades):
        grv adapter install #{opt_libexec}/adapters/duckdb --replace
        grv adapter install #{opt_libexec}/adapters/salesforce --replace

      To teach your AI coding agent how to use grv:
        grv skills install -g
    EOS
  end

  test do
    system bin/"grv", "init", "--grv", testpath/"store"
    assert_match "grv-cli", shell_output("#{bin}/grv skills list")
  end
end
