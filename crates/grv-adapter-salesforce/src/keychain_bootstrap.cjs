"use strict";
// This bootstrap has no persistent state or environment credential channel.
// Reject before loading the pinned CLI if any identity or input check fails.
(async () => {
  const fs = require("node:fs");
  const path = require("node:path");
  const crypto = require("node:crypto");
  const { pathToFileURL } = require("node:url");
  const fail = () => {
    throw new Error("private keychain bridge refused");
  };
  const chunks = [];
  let size = 0;
  for (;;) {
    const chunk = Buffer.alloc(4096);
    const n = fs.readSync(0, chunk, 0, chunk.length, null);
    if (!n) break;
    size += n;
    if (size > 65536) fail();
    chunks.push(chunk.subarray(0, n));
  }
  const payload = JSON.parse(Buffer.concat(chunks).toString("utf8"));
  if (
    Object.keys(payload).sort().join(",") !== "args,entry,key,module,platform" ||
    typeof payload.key !== "string" ||
    !payload.key.length ||
    payload.key.length > 512 ||
    /[\x00-\x1f\x7f]/.test(payload.key) ||
    !Array.isArray(payload.args) ||
    payload.args.some((arg) => typeof arg !== "string") ||
    typeof payload.entry !== "string" ||
    typeof payload.module !== "string" ||
    (payload.platform !== "macos" && payload.platform !== "linux") ||
    process.platform !== (payload.platform === "macos" ? "darwin" : "linux")
  )
    fail();
  const read = (file) => {
    const stats = fs.statSync(file);
    if (!stats.isFile() || stats.size > 262144) fail();
    return fs.readFileSync(file);
  };
  if (
    !path.isAbsolute(payload.entry) ||
    fs.realpathSync(payload.entry) !== payload.entry ||
    path.basename(payload.entry) !== "run.js" ||
    path.basename(path.dirname(payload.entry)) !== "bin"
  )
    fail();
  const root = path.dirname(path.dirname(payload.entry));
  const core = path.join(root, "node_modules/@salesforce/core");
  if (
    payload.module !== path.join(core, "lib/crypto/keyChainImpl.js") ||
    fs.realpathSync(payload.module) !== payload.module ||
    require.resolve("@salesforce/core", { paths: [root] }) !== path.join(core, "lib/index.js")
  )
    fail();
  const cliPackage = JSON.parse(read(path.join(root, "package.json")));
  const corePackage = JSON.parse(read(path.join(core, "package.json")));
  if (
    cliPackage.name !== "@salesforce/cli" ||
    cliPackage.version !== "2.152.14" ||
    corePackage.name !== "@salesforce/core" ||
    corePackage.version !== "9.2.2"
  )
    fail();
  const hash = (file) => crypto.createHash("sha256").update(read(file)).digest("hex");
  if (
    hash(payload.entry) !== "fa2dfe2a89e652be067a06cfa2c766d52d3340342d6a19c255a8b9b917befb6e" ||
    hash(payload.module) !== "892e2d137f817c60838011297ea6ed9bd037f1d0108ad79396aca6598fa4a65b"
  )
    fail();
  const implementation = require(payload.module).keyChainImpl[process.platform];
  installReadOnlyKeychain(implementation, payload.key);
  process.argv = [process.execPath, payload.entry, ...payload.args];
  await import(pathToFileURL(payload.entry).href);
})().catch(() => {
  process.stderr.write("Salesforce private keychain bridge failed\n");
  process.exitCode = 126;
});
