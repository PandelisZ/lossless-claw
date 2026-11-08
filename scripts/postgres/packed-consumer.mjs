// Fresh installed-package contract checks, with no OpenClaw host or database credentials.
import { execFileSync } from "node:child_process";
import { createHash } from "node:crypto";
import { mkdtemp, readFile, writeFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import path from "node:path";
import { createRequire } from "node:module";
const pinFile = path.resolve(process.argv[2] ?? ""),
  pin = JSON.parse(await readFile(pinFile, "utf8")),
  archive = path.join(path.dirname(pinFile), pin.archive);
if (
  createHash("sha256")
    .update(await readFile(archive))
    .digest("hex") !== pin.sha256
)
  throw new Error("Packed consumer archive hash mismatch");
const directory = await mkdtemp(path.join(tmpdir(), "lcm-packed-consumer-"));
const env = Object.fromEntries(
  ["PATH", "HOME", "LANG", "TMPDIR", "NPM_CONFIG_CACHE"]
    .filter((k) => process.env[k] !== undefined)
    .map((k) => [k, process.env[k]])
);
try {
  await writeFile(
    path.join(directory, "package.json"),
    JSON.stringify({ private: true, type: "module" }) + "\n"
  );
  execFileSync(
    "npm",
    [
      "install",
      archive,
      "--ignore-scripts",
      "--omit=dev",
      "--omit=peer",
      "--no-audit",
      "--no-fund",
    ],
    { cwd: directory, env, stdio: ["ignore", "ignore", "inherit"] }
  );
  const require = createRequire(path.join(directory, "package.json"));
  const contracts = [
    pin.package,
    `${pin.package}/package.json`,
    `${pin.package}/doctor-contract-api.js`,
    `${pin.package}/dist/experimental/context-storage.js`,
    `${pin.package}/dist/experimental/postgres-operations.js`,
  ];
  for (const contract of contracts) require.resolve(contract);
  const code = `const pg=await import(${JSON.stringify(
    pin.package + "/dist/experimental/postgres-operations.js"
  )});const tp=await import(${JSON.stringify(
    pin.package + "/dist/experimental/context-storage.js"
  )});if(typeof pg.PostgresDatabase?.open!=="function"||typeof pg.inspectRuntimePrivileges!=="function"||!Object.keys(tp).length)throw new Error("Missing experimental API export");let rejected=false;try{await pg.PostgresDatabase.open({schema:"lcm_unconfigured",organizationId:"synthetic-org",agentId:"one"});}catch(error){rejected=String(error).includes("Dedicated LCM");}if(!rejected)throw new Error("Unconfigured PG consumer did not fail before I/O");`;
  execFileSync(process.execPath, ["--input-type=module", "-e", code], {
    cwd: directory,
    env,
    stdio: ["ignore", "ignore", "inherit"],
  });
  const cli = require.resolve(`${pin.package}/dist/postgres-cli.js`),
    help = JSON.parse(
      execFileSync(process.execPath, [cli, "--help"], {
        cwd: directory,
        env,
        encoding: "utf8",
      })
    );
  const linkedHelp = JSON.parse(
    execFileSync(
      path.join(directory, "node_modules/.bin/lcm-postgres"),
      ["--help"],
      { cwd: directory, env, encoding: "utf8" }
    )
  );
  if (JSON.stringify(linkedHelp) !== JSON.stringify(help))
    throw new Error("Installed operator binary differs from its module entry");
  if (!help.usage?.some((value) => value.includes("rollback")))
    throw new Error("Packed operator CLI contract missing");
  console.log(
    JSON.stringify({
      node: process.version,
      commit: pin.commit,
      archiveSha256: pin.sha256,
      contracts,
      resolutionPassed: true,
      experimentalImportsPassed: true,
      operatorHelpPassed: true,
      unconfiguredPostgresRejected: true,
      production: false,
    })
  );
} finally {
  await rm(directory, { recursive: true, force: true });
}
