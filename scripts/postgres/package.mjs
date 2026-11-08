// Build an unpublished, content-addressed candidate from a clean immutable Git archive.
import { execFileSync } from "node:child_process";
import { createHash } from "node:crypto";
import {
  mkdir,
  mkdtemp,
  readFile,
  writeFile,
  rm,
  access,
} from "node:fs/promises";
import { tmpdir } from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";
const root = path.resolve(
    path.dirname(fileURLToPath(import.meta.url)),
    "../.."
  ),
  destination = path.resolve(
    process.argv[2] ?? "experiments/postgres/.generated/package"
  );
const git = (...args) =>
  execFileSync("git", args, { cwd: root, encoding: "utf8" }).trim();
if (git("status", "--porcelain", "--untracked-files=normal"))
  throw new Error("Candidate builds require a clean committed source tree");
const requestedCommit =
  process.argv[3] === "--commit" ? process.argv[4] : undefined;
if (requestedCommit && !/^[a-f0-9]{40}$/.test(requestedCommit))
  throw new Error("Candidate source commit must be a full SHA");
const commit = git("rev-parse", requestedCommit ?? "HEAD"),
  directory = await mkdtemp(path.join(tmpdir(), "lcm-pg-package-"));
const env = Object.fromEntries(
  ["PATH", "HOME", "LANG", "TMPDIR", "NPM_CONFIG_CACHE"]
    .filter((key) => process.env[key] !== undefined)
    .map((key) => [key, process.env[key]])
);
try {
  await mkdir(destination, { recursive: true });
  const source = path.join(directory, "source");
  await mkdir(source);
  const archive = path.join(directory, "source.tar");
  execFileSync(
    "git",
    ["archive", "--format=tar", `--output=${archive}`, commit],
    { cwd: root }
  );
  execFileSync("tar", ["-xf", archive, "-C", source]);
  execFileSync("npm", ["ci", "--ignore-scripts", "--no-audit", "--no-fund"], {
    cwd: source,
    env,
    stdio: ["ignore", "ignore", "inherit"],
  });
  execFileSync("npm", ["run", "build"], {
    cwd: source,
    env,
    stdio: ["ignore", "ignore", "inherit"],
  });
  for (const file of [
    "dist/index.js",
    "dist/cli.js",
    "dist/postgres-cli.js",
    "dist/experimental/context-storage.js",
    "dist/experimental/postgres-operations.js",
    "doctor-contract-api.js",
  ])
    await access(path.join(source, file));
  const digest = (bytes) => createHash("sha256").update(bytes).digest("hex");
  const metadata = JSON.parse(
    await readFile(path.join(source, "package.json"), "utf8")
  );
  const provenance = {
    format: 1,
    sourceRepository: "https://github.com/PandelisZ/lossless-claw",
    commit,
    nativeBaseline: "51c3c90bfe82fa649e4895dd1a2c4874b5e28f76",
    schemaVersion: 1,
    node: process.version,
    npm: execFileSync("npm", ["--version"], { env, encoding: "utf8" }).trim(),
    lockSha256: digest(await readFile(path.join(source, "package-lock.json"))),
    published: false,
  };
  await writeFile(
    path.join(source, "dist/postgres-build.json"),
    JSON.stringify(provenance, null, 2) + "\n"
  );
  const packs = JSON.parse(
    execFileSync(
      "npm",
      ["pack", "--json", "--ignore-scripts", "--pack-destination", directory],
      {
        cwd: source,
        env,
        encoding: "utf8",
        stdio: ["ignore", "pipe", "inherit"],
      }
    )
  );
  const bytes = await readFile(path.join(directory, packs[0].filename)),
    sha256 = digest(bytes),
    file = path.join(destination, `lcm-postgres-${commit}-${sha256}.tgz`);
  try {
    await writeFile(file, bytes, { flag: "wx", mode: 0o600 });
  } catch (error) {
    if (error.code !== "EEXIST" || !(await readFile(file)).equals(bytes))
      throw error;
  }
  const pin = {
    ...provenance,
    package: metadata.name,
    version: metadata.version,
    sha256,
    integrity: packs[0].integrity,
    bytes: bytes.length,
    archive: path.basename(file),
  };
  const pinPath = path.join(destination, `pin-${sha256}.json`),
    encoded = JSON.stringify(pin, null, 2) + "\n";
  try {
    await writeFile(pinPath, encoded, { flag: "wx", mode: 0o600 });
  } catch (error) {
    if (
      error.code !== "EEXIST" ||
      (await readFile(pinPath, "utf8")) !== encoded
    )
      throw error;
  }
  console.log(JSON.stringify({ file, ...pin }));
} finally {
  await rm(directory, { recursive: true, force: true });
}
