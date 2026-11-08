// Creates only task-owned disposable resources and removes them in finally.
// Invoke via mise from the monorepo root; no production credentials are needed.
import { execFileSync, spawn } from "node:child_process";
import { randomBytes } from "node:crypto";
import {
  mkdir,
  mkdtemp,
  readFile,
  writeFile,
  rm,
  access,
  chmod,
} from "node:fs/promises";
import path from "node:path";
import { tmpdir } from "node:os";
import { root, prepare, commit } from "./prepare.mjs";
const id = `lcm-pg-${process.pid}-${randomBytes(4).toString("hex")}`;
const network = id,
  postgres = id,
  pooler = `${id}-pooler`;
const fixtureDirectory =
  process.argv[2] ?? path.join(tmpdir(), "lossless-claw-postgres-fixture");
const outputDirectory =
  process.argv[3] ?? path.join(root, ".generated/results");
await mkdir(outputDirectory, { recursive: true });
await mkdir(path.join(root, ".generated"), { recursive: true });
await prepare();
async function run(file, args = [], environment = {}) {
  return new Promise((resolve, reject) => {
    const child = spawn(process.execPath, [path.join(root, file), ...args], {
      stdio: "inherit",
      env: { ...process.env, ...environment },
    });
    child.on("error", reject);
    child.on("exit", (code) =>
      code === 0 ? resolve() : reject(new Error(`${file} exited ${code}`)),
    );
  });
}
try {
  await access(path.join(fixtureDirectory, "fixture-manifest.json"));
} catch {
  await run("fixture.mjs", [fixtureDirectory]);
}
const { readFixture } = await import("./transfer.mjs");
const { manifest } = await readFixture(
  path.join(fixtureDirectory, "fixture-manifest.json"),
);
if (manifest.upstreamCommit !== commit || manifest.seed !== 20261104)
  throw new Error("Unexpected common fixture");
const docker = (...args) =>
  execFileSync("docker", args, {
    encoding: "utf8",
    stdio: ["ignore", "pipe", "pipe"],
  }).trim();
const port = (name) => docker("port", name, "5432").split(":").at(-1);
const configDirectory = await mkdtemp(path.join(tmpdir(), `${id}-config-`));
await chmod(configDirectory, 0o755);
try {
  docker("network", "create", network);
  docker(
    "run",
    "-d",
    "--name",
    postgres,
    "--label",
    "lossless-claw.experiment=lcm-postgres",
    "--network",
    network,
    "-e",
    "POSTGRES_HOST_AUTH_METHOD=trust",
    "-p",
    "127.0.0.1::5432",
    "postgres:18",
    "-c",
    "shared_preload_libraries=pg_stat_statements",
  );
  const { createPool } = await import("./database.mjs");
  const environment = {
    LCM_DISPOSABLE_POSTGRES: "1",
    PGHOST: "127.0.0.1",
    PGPORT: port(postgres),
    PGUSER: "postgres",
    PGDATABASE: "postgres",
    PGPASSWORD: "",
    PGSSLMODE: "disable",
    TZ: "UTC",
    LCM_FIXTURE_MANIFEST: path.join(fixtureDirectory, "fixture-manifest.json"),
  };
  const probe = createPool({
    host: "127.0.0.1",
    port: Number(environment.PGPORT),
    user: "postgres",
    password: "",
    database: "postgres",
    ssl: false,
  });
  try {
    let ready = false;
    for (let i = 0; i < 100; i++) {
      try {
        await probe.query("CREATE EXTENSION IF NOT EXISTS pg_stat_statements");
        ready = true;
        break;
      } catch {
        await new Promise((r) => setTimeout(r, 100));
      }
    }
    if (!ready) throw new Error("Disposable PostgreSQL not ready");
  } finally {
    await probe.end();
  }
  await run("store.test.mjs", [], environment);
  if (process.env.LCM_FULL_BENCHMARK !== "1") {
    await run("snapshot.test.mjs", [], environment);
    await run(
      "snapshot-benchmark.mjs",
      [path.join(outputDirectory, "snapshot-direct.json")],
      environment,
    );
  } else
    await run(
      "benchmark.mjs",
      [path.join(outputDirectory, "direct.json")],
      environment,
    );
  await writeFile(
    path.join(configDirectory, "pgbouncer.ini"),
    (await readFile(path.join(root, "pgbouncer.ini"), "utf8")).replace(
      "__POSTGRES_CONTAINER__",
      postgres,
    ),
  );
  await writeFile(
    path.join(configDirectory, "userlist.txt"),
    '"postgres" ""\n',
  );
  docker(
    "run",
    "-d",
    "--name",
    pooler,
    "--label",
    "lossless-claw.experiment=lcm-postgres",
    "--network",
    network,
    "-p",
    "127.0.0.1::5432",
    "-v",
    `${configDirectory}:/etc/pgbouncer:ro`,
    "edoburu/pgbouncer@sha256:53d98b3174b0842c475b9842fb0a733b2d9f7ec9da834ec42252aa553f48c628",
  );
  const poolEnvironment = {
    ...environment,
    PGPORT: port(pooler),
    LCM_POOLER: "1",
  };
  const poolProbe = createPool({
    host: "127.0.0.1",
    port: Number(poolEnvironment.PGPORT),
    user: "postgres",
    password: "",
    database: "postgres",
    ssl: false,
  });
  try {
    let ready = false;
    for (let i = 0; i < 100; i++) {
      try {
        await poolProbe.query("SELECT 1");
        ready = true;
        break;
      } catch {
        await new Promise((r) => setTimeout(r, 100));
      }
    }
    if (!ready) throw new Error("Disposable PgBouncer not ready");
  } finally {
    await poolProbe.end();
  }
  await run("store.test.mjs", [], poolEnvironment);
  if (process.env.LCM_FULL_BENCHMARK !== "1") {
    await run("snapshot.test.mjs", [], poolEnvironment);
    await run(
      "snapshot-benchmark.mjs",
      [path.join(outputDirectory, "snapshot-pooler.json")],
      poolEnvironment,
    );
  } else
    await run(
      "benchmark.mjs",
      [path.join(outputDirectory, "pooler.json")],
      poolEnvironment,
    );
  if (process.env.LCM_FULL_BENCHMARK === "1")
    await run(
      "restart-test.mjs",
      [path.join(outputDirectory, "restart.json")],
      {
        ...environment,
        LCM_OWNED_POSTGRES_CONTAINER: postgres,
      },
    );
  console.log(`Evidence saved in ${outputDirectory}`);
} finally {
  for (const name of [pooler, postgres])
    try {
      docker("rm", "-f", "-v", name);
    } catch {}
  try {
    docker("network", "rm", network);
  } catch {}
  await rm(configDirectory, { recursive: true, force: true });
}
