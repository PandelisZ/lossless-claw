import {
  discardAmbientPostgresSecrets,
  requireLoopbackPostgres,
} from "./local-environment.mjs";
// Owns and cleans only uniquely labeled disposable containers/network/config.
import { execFileSync, spawn } from "node:child_process";
import { randomBytes, createHash } from "node:crypto";
import {
  mkdir,
  mkdtemp,
  writeFile,
  readFile,
  rm,
  chmod,
} from "node:fs/promises";
import { tmpdir } from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";
import pg from "pg";
discardAmbientPostgresSecrets();
process.env.LCM_DISPOSABLE_POSTGRES = "1";
const root = path.resolve(
    path.dirname(fileURLToPath(import.meta.url)),
    "../.."
  ),
  output = path.join(root, "experiments/postgres/.generated/native");
await mkdir(output, { recursive: true });
const id = `lcm-native-${process.pid}-${randomBytes(4).toString("hex")}`,
  server = id,
  pooler = `${id}-pooler`;
const config = await mkdtemp(path.join(tmpdir(), `${id}-`));
await chmod(config, 0o755);
const docker = (...args) =>
  execFileSync("docker", args, {
    encoding: "utf8",
    stdio: ["ignore", "pipe", "pipe"],
  }).trim();
const port = (name) => docker("port", name, "5432").split(":").at(-1);
const restartOnly = process.argv.includes("--restart-only");
const checksOnly = process.argv.includes("--checks-only");
const operationsOnly = process.argv.includes("--operations-only");
const runtimeRole = `lcm_org_${createHash("sha256")
  .update("synthetic-gateway-org")
  .digest("hex")
  .slice(0, 32)}`;
const evidence = {
  production: false,
  node: process.version,
  commands: [],
  sources: {},
};
async function run(args, env) {
  const started = Date.now();
  await new Promise((resolve, reject) => {
    const child = spawn(process.execPath, args, {
      cwd: root,
      env: { ...process.env, ...env },
      stdio: "inherit",
    });
    child.on("error", reject);
    child.on("exit", (code) =>
      code === 0
        ? resolve()
        : reject(
            new Error(`Native check ${path.basename(args[0])} failed (${code})`)
          )
    );
  });
  evidence.commands.push({
    arguments: args,
    environment:
      env.LCM_POSTGRES_VIA_POOLER === "1"
        ? "local transaction pooler"
        : "direct local PostgreSQL",
    durationMs: Date.now() - started,
    passed: true,
  });
}
async function ready(env) {
  for (let i = 0; i < 80; i++) {
    const pool = new pg.Pool({
      host: env.PGHOST,
      port: Number(env.PGPORT),
      user: env.PGUSER,
      database: env.PGDATABASE,
      max: 1,
      connectionTimeoutMillis: 1000,
    });
    try {
      await pool.query("SELECT 1");
      await pool.end();
      return;
    } catch {
      await pool.end();
      await new Promise((resolve) => setTimeout(resolve, 200));
    }
  }
  throw new Error("Disposable database did not become ready");
}
try {
  docker("network", "create", id);
  docker(
    "run",
    "-d",
    "--name",
    server,
    "--label",
    "lossless-claw.experiment=native-postgres",
    "--network",
    id,
    "-e",
    "POSTGRES_HOST_AUTH_METHOD=trust",
    "-p",
    "127.0.0.1::5432",
    "postgres:18"
  );
  const direct = {
    LCM_DISPOSABLE_POSTGRES: "1",
    LCM_POSTGRES_VIA_POOLER: "0",
    PGHOST: "127.0.0.1",
    PGPORT: port(server),
    PGUSER: "postgres",
    PGDATABASE: "postgres",
  };
  await ready(direct);
  if (operationsOnly) {
    const admin = new pg.Pool({
      host: direct.PGHOST,
      port: Number(direct.PGPORT),
      user: direct.PGUSER,
      database: direct.PGDATABASE,
      max: 1,
    });
    try {
      await admin.query(`CREATE ROLE "${runtimeRole}" LOGIN`);
    } finally {
      await admin.end();
    }
    direct.LCM_GATEWAY_RUNTIME_ROLE = runtimeRole;
    const { prepare } = await import("../../experiments/postgres/prepare.mjs");
    await prepare();
  }
  const fixture =
    process.argv[2] && !process.argv[2].startsWith("--")
      ? process.argv[2]
      : "/tmp/lossless-claw-postgres-fixture/fixture-manifest.json";
  try {
    await readFile(fixture);
  } catch {
    const { prepare } = await import("../../experiments/postgres/prepare.mjs");
    await prepare();
    const { generate } = await import("../../experiments/postgres/fixture.mjs");
    await generate(path.dirname(fixture));
  }
  const tests = [
    "node_modules/vitest/vitest.mjs",
    "run",
    "test/postgres-engine.integration.test.ts",
    ...(operationsOnly ? ["test/postgres-operations.integration.test.ts"] : []),
    "--maxWorkers=2",
    "--reporter=default",
    "--reporter=json",
  ];
  if (!restartOnly) {
    await run(
      [...tests, `--outputFile=${path.join(output, "tests-direct.json")}`],
      direct
    );
    if (!operationsOnly || checksOnly)
      await run(
        [
          "scripts/postgres/benchmark.mjs",
          fixture,
          ...(checksOnly ? ["--parity-only"] : []),
        ],
        direct
      );
    await run(["scripts/postgres/gateway-smoke.mjs"], direct);
  }
  await writeFile(
    path.join(config, "pgbouncer.ini"),
    (
      await readFile(
        path.join(root, "experiments/postgres/pgbouncer.ini"),
        "utf8"
      )
    ).replace("__POSTGRES_CONTAINER__", server)
  );
  await writeFile(
    path.join(config, "userlist.txt"),
    '"postgres" ""\n' + (operationsOnly ? `"${runtimeRole}" ""\n` : "")
  );
  docker(
    "run",
    "-d",
    "--name",
    pooler,
    "--label",
    "lossless-claw.experiment=native-postgres",
    "--network",
    id,
    "-p",
    "127.0.0.1::5432",
    "-v",
    `${config}:/etc/pgbouncer:ro`,
    "edoburu/pgbouncer@sha256:53d98b3174b0842c475b9842fb0a733b2d9f7ec9da834ec42252aa553f48c628"
  );
  const pooled = {
    ...direct,
    PGPORT: port(pooler),
    LCM_POSTGRES_VIA_POOLER: "1",
  };
  await ready(pooled);
  if (!restartOnly) {
    await run(
      [
        ...tests.filter(
          (value) => value !== "test/postgres-operations.integration.test.ts"
        ),
        `--outputFile=${path.join(output, "tests-pooler.json")}`,
      ],
      pooled
    );
    if (!operationsOnly || checksOnly)
      await run(
        [
          "scripts/postgres/benchmark.mjs",
          fixture,
          ...(checksOnly ? ["--parity-only"] : []),
        ],
        pooled
      );
  }
  await run(["scripts/postgres/gateway-smoke.mjs"], pooled);
  // Restart only our disposable PostgreSQL server and verify actual gateway-created LCM state.
  const gateway = JSON.parse(
    await readFile(path.join(output, "gateway-smoke-pooler.json"), "utf8")
  );
  const { PostgresDatabase } = await import(path.join(output, "store.mjs"));
  const identity = {
    schema: gateway.storeSchema,
    organizationId: "synthetic-gateway-org",
    agentId: "one",
    pool: {
      host: pooled.PGHOST,
      port: Number(pooled.PGPORT),
      user: pooled.PGUSER,
      database: pooled.PGDATABASE,
    },
  };
  async function state() {
    const db = await PostgresDatabase.open(identity);
    try {
      return {
        messages: await db
          .prepare("SELECT * FROM messages ORDER BY message_id")
          .all(),
        context: await db
          .prepare("SELECT * FROM context_items ORDER BY ordinal")
          .all(),
        cursors: await db
          .prepare(
            "SELECT * FROM conversation_transcript_cursors ORDER BY conversation_id"
          )
          .all(),
        receipts: await db
          .prepare("SELECT * FROM turn_advancements ORDER BY advancement_key")
          .all(),
        summaries: await db
          .prepare("SELECT * FROM summaries ORDER BY summary_id")
          .all(),
        sources: await db
          .prepare("SELECT * FROM summary_messages ORDER BY summary_id,ordinal")
          .all(),
        parents: await db
          .prepare("SELECT * FROM summary_parents ORDER BY summary_id,ordinal")
          .all(),
      };
    } finally {
      await db.close();
    }
  }
  const before = await state();
  docker("restart", server);
  direct.PGPORT = port(server);
  pooled.PGPORT = port(pooler);
  identity.pool.port = Number(pooled.PGPORT);
  await ready(direct);
  await ready(pooled);
  const after = await state();
  const hash = (value) =>
    createHash("sha256").update(JSON.stringify(value)).digest("hex");
  if (hash(before) !== hash(after))
    throw new Error(
      "Native LCM state changed across disposable server restart"
    );
  evidence.restart = {
    backend:
      "local PostgreSQL server restarted; fresh client reads through transaction pooler",
    stateHash: hash(after),
    messages: after.messages.length,
    contextItems: after.context.length,
    cursors: after.cursors.length,
    receipts: after.receipts.length,
    summaries: after.summaries.length,
    sourceEdges: after.sources.length,
    parentEdges: after.parents.length,
  };
  const pool = new pg.Pool(identity.pool);
  try {
    evidence.postgresVersion = (
      await pool.query("SELECT version() AS version")
    ).rows[0].version;
  } finally {
    await pool.end();
  }
  for (const file of execFileSync(
    "git",
    [
      "ls-files",
      "--cached",
      "--others",
      "--exclude-standard",
      "src",
      "test",
      "scripts",
      "package.json",
      "package-lock.json",
      "cli.ts",
      "postgres-cli.ts",
    ],
    { cwd: root, encoding: "utf8" }
  )
    .trim()
    .split("\n"))
    evidence.sources[file] = hash(
      await readFile(path.join(root, file), "utf8")
    );
  for (const file of [
    "src/db/storage.ts",
    "src/db/postgres/database.ts",
    "src/db/postgres/dialect.ts",
    "src/db/postgres/schema.ts",
    "src/db/postgres/context-snapshot.ts",
    "src/db/postgres/transfer.ts",
    "src/plugin/postgres-init.ts",
    "test/postgres-engine.integration.test.ts",
  ])
    evidence.sources[file] = hash(
      await readFile(path.join(root, file), "utf8")
    );
  await writeFile(
    path.join(
      output,
      restartOnly
        ? "checks-restart.json"
        : operationsOnly
        ? "checks-operations.json"
        : checksOnly
        ? "checks-post-merge.json"
        : "checks-native.json"
    ),
    JSON.stringify(evidence, null, 2) + "\n"
  );
  console.log(
    JSON.stringify({
      nativeChecks: restartOnly ? "restart probe passed" : "passed",
      restart: evidence.restart,
    })
  );
} finally {
  for (const name of [pooler, server])
    try {
      docker("rm", "-f", "-v", name);
    } catch {}
  try {
    docker("network", "rm", id);
  } catch {}
  await rm(config, { recursive: true, force: true });
}
