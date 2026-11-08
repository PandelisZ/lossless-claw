// Focused follow-up only: retain all initial benchmark/migration evidence unchanged.
import assert from "node:assert/strict";
import { performance } from "node:perf_hooks";
import { mkdtemp, writeFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import path from "node:path";
import { Database, createPool, migrate } from "./database.mjs";
import {
  readFixture,
  importFixture,
  restoreSqlite,
  hash,
} from "./transfer.mjs";
import { PostgresStore } from "./store.mjs";
import {
  ConversationStore,
  SummaryStore,
  ContextAssembler,
} from "./.generated/upstream.mjs";
import {
  upstreamAssembler,
  batchedUpstreamAssembler,
  upstreamCompactor,
  batchedUpstreamCompactor,
  compactionConfig,
} from "./upstream-adapter.mjs";
if (
  process.env.LCM_DISPOSABLE_POSTGRES !== "1" ||
  process.env.PGHOST !== "127.0.0.1"
)
  throw new Error("Explicit loopback disposable PG required");
const pool = createPool(),
  db = new Database(pool);
const directory = await mkdtemp(path.join(tmpdir(), "lcm-snapshot-bench-"));
let sqlite,
  injectedMs = 0;
const query = db.query.bind(db);
db.query = async (...args) => {
  // Artificial client-side delay once per SQL request, BEFORE sending it. This
  // models serialized per-trip cost, not a measured network or production RTT.
  if (injectedMs)
    await new Promise((resolve) => setTimeout(resolve, injectedMs));
  return query(...args);
};
const input = {
  conversationId: 1,
  tokenBudget: 16000,
  freshTailCount: 80,
  freshTailMaxTokens: 65536,
};
const results = [];
function p(values, f) {
  return values[Math.ceil(values.length * f) - 1];
}
async function measure(operation, fn, samples) {
  if (!operation.includes("compaction")) await fn(0);
  const trips = db.roundTrips,
    values = [];
  let bytes = 0;
  for (let i = 0; i < samples; i++) {
    const t = performance.now();
    const result = await fn(i);
    values.push(performance.now() - t);
    bytes += Buffer.byteLength(JSON.stringify(result));
  }
  values.sort((a, b) => a - b);
  const result = {
    operation,
    samples,
    concurrency: 1,
    injectedClientDelayPerQueryMs: injectedMs,
    p50Ms: p(values, 0.5),
    p95Ms: p(values, 0.95),
    maxMs: values.at(-1),
    clientNetworkRoundTrips: db.roundTrips - trips,
    tripsPerOperation: (db.roundTrips - trips) / samples,
    serializedOutputBytes: bytes,
  };
  results.push(result);
  console.log(JSON.stringify(result));
}
try {
  const { fixture, manifest } = await readFixture(
    process.env.LCM_FIXTURE_MANIFEST ??
      "/tmp/lossless-claw-postgres-fixture/fixture-manifest.json",
  );
  await db.query("DROP SCHEMA IF EXISTS lcm_experiment CASCADE");
  await migrate(db);
  await importFixture(db, fixture, manifest.sha256);
  await db.query(
    "ANALYZE lcm_experiment.conversations; ANALYZE lcm_experiment.context_items; ANALYZE lcm_experiment.messages; ANALYZE lcm_experiment.summaries; ANALYZE lcm_experiment.summary_messages; ANALYZE lcm_experiment.summary_parents; ANALYZE lcm_experiment.auxiliary_rows; ANALYZE lcm_experiment.large_files",
  );
  sqlite = restoreSqlite(
    fixture.organizations[0],
    path.join(directory, "baseline.db"),
  );
  const baseline = new ContextAssembler(
    new ConversationStore(sqlite, { fts5Available: true }),
    new SummaryStore(sqlite, { fts5Available: true }),
    "UTC",
  );
  const store = new PostgresStore(db, {
    organizationId: "org1",
    agentId: "agent1",
  });
  const before = upstreamAssembler(store),
    after = batchedUpstreamAssembler(store);
  const expected = await baseline.assemble(input);
  assert.deepEqual(await before.assemble(input), expected);
  assert.deepEqual(await after.assemble(input), expected);
  await measure("sqlite-full-assembly", () => baseline.assemble(input), 20);
  await measure("unbatched-full-assembly", () => before.assemble(input), 20);
  await measure("batched-full-assembly", () => after.assemble(input), 20);
  injectedMs = 2;
  await measure(
    "unbatched-full-assembly-injected-2ms-per-query",
    () => before.assemble(input),
    8,
  );
  await measure(
    "batched-full-assembly-injected-2ms-per-query",
    () => after.assemble(input),
    8,
  );
  injectedMs = 0;
  for (const [name, factory, start] of [
    ["unbatched", upstreamCompactor, 1],
    ["batched", batchedUpstreamCompactor, 4],
  ]) {
    await measure(
      `${name}-full-leaf-compaction`,
      (i) =>
        factory(store, start + i, compactionConfig).compactLeaf({
          conversationId: start + i,
          tokenBudget: 2000,
          force: true,
          summarize: async () => "Deterministic snapshot benchmark summary.",
        }),
      3,
    );
  }
  await writeFile(
    process.argv[2],
    JSON.stringify(
      {
        fixtureSha256: manifest.sha256,
        sourceCommit: fixture.upstreamCommit,
        recordedAt: new Date().toISOString(),
        transport:
          process.env.LCM_POOLER === "1"
            ? "local PgBouncer 1.24.1 transaction pooling (2 backends, DISCARD ALL)"
            : "local Docker PostgreSQL direct",
        postgres: (await pool.query("SHOW server_version")).rows[0]
          .server_version,
        node: process.version,
        poolMax: 4,
        fullAssemblyOutputSha256: hash(JSON.stringify(expected)),
        initialContextItems: 95,
        initialMessages: 200,
        initialSummaryRows: 35,
        notes: [
          "Every batched sample includes snapshot query, JSON decoding, exact pinned upstream assembler and disposal; not merely contextSnapshot.",
          "Original unbatched benchmark files remain unchanged. These before/after samples share a fresh import and unchanged input.",
          "Injected rows add a synthetic 2ms client timer before every read query. These are not production RTT or HA/replica evidence. Timer overhead/host noise mean total time is not exactly trips*2ms.",
          "Assembly has no inference. Compaction uses a deterministic constant only at the inference boundary, no network model calls.",
          "Compaction has 3 samples per mode, separate conversations with identical initial row counts and graph shape; p95=max. It includes SQL commits and any snapshot refresh.",
          "Result sizes are UTF-8 JSON serialization bytes, not packet captures. All I/O is to the real local PostgreSQL instance or local PgBouncer.",
        ],
        results,
      },
      null,
      2,
    ) + "\n",
  );
} finally {
  sqlite?.close();
  await pool.end();
  await rm(directory, { recursive: true, force: true });
}
