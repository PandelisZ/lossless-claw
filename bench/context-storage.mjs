// Synthetic-only comparison. PG modules are a read-only export of the parallel
// experiment, not a new PG implementation or a claim of equivalent topology.
import assert from "node:assert/strict";
import { createHash, randomUUID } from "node:crypto";
import { readFile, writeFile, mkdir } from "node:fs/promises";
import { parseArgs } from "node:util";
import { pathToFileURL } from "node:url";
import path from "node:path";
import { DatabaseSync } from "node:sqlite";
import { execFileSync } from "node:child_process";
import { TurbopufferContextStorage } from "../dist/experimental/context-storage.js";

const { values } = parseArgs({
  options: {
    manifest: { type: "string" },
    output: { type: "string" },
    postgres: { type: "string" },
    "postgres-source-sha": { type: "string" },
    "key-helper": { type: "string" },
    "client-location": { type: "string" },
  },
});
if (
  process.env.LCM_TURBOPUFFER_EXPERIMENT !== "1" ||
  !values.manifest ||
  !values.output
)
  throw new Error("Explicit opt-in, --manifest and --output required");
const sha = (value) => createHash("sha256").update(value).digest("hex");
const manifest = JSON.parse(await readFile(values.manifest, "utf8"));
const raw = await readFile(manifest.fixturePath);
assert.equal(sha(raw), manifest.sha256);
const fixture = JSON.parse(raw);
assert.equal(fixture.seed, 20261104);
assert.equal(
  fixture.upstreamCommit,
  "4bd12aa198c4903f6400a42fa00fa0efc6697063",
);
const key = values["key-helper"]
  ? await (
      await import(pathToFileURL(path.resolve(values["key-helper"])).href)
    ).honchoKey()
  : process.env.TURBOPUFFER_API_KEY;
if (!key) throw new Error("Key required in process memory");
const output = path.resolve(values.output);
await mkdir(output, { recursive: true });
const modules = values.postgres
  ? await import(
      pathToFileURL(
        path.join(path.resolve(values.postgres), ".generated/upstream.mjs"),
      ).href
    )
  : null;
if (!modules)
  throw new Error(
    "--postgres read-only experiment export required for this paired benchmark",
  );
const { ContextAssembler, ConversationStore, SummaryStore, runLcmMigrations } =
  modules;
const dbm = await import(
  pathToFileURL(path.join(path.resolve(values.postgres), "database.mjs")).href
);
const transfer = await import(
  pathToFileURL(path.join(path.resolve(values.postgres), "transfer.mjs")).href
);
const { PostgresStore } = await import(
  pathToFileURL(path.join(path.resolve(values.postgres), "store.mjs")).href
);
const adapter = await import(
  pathToFileURL(
    path.join(path.resolve(values.postgres), "upstream-adapter.mjs"),
  ).href
);
const nativeProvenance = JSON.parse(
  await readFile(
    path.join(path.resolve(values.postgres), ".generated/provenance.json"),
    "utf8",
  ),
);
execFileSync(
  "git",
  [
    "diff",
    "--exit-code",
    nativeProvenance.commit,
    "--",
    "src/assembler.ts",
    "src/compaction.ts",
    "src/store",
    "src/db",
  ],
  { stdio: "pipe" },
);
if (
  process.env.PGHOST !== "127.0.0.1" ||
  process.env.PGDATABASE !== "lcm_fork_comparison"
)
  throw new Error("Task-owned loopback lcm_fork_comparison database required");
const pool = dbm.createPool(),
  database = new dbm.Database(pool);
const prefix = "lcm-fork-exp-" + randomUUID();
const stores = [],
  sqlites = [];
const evidence = {
  recordedAt: new Date().toISOString(),
  fixtureSha256: manifest.sha256,
  fixtureSourceCommit: fixture.upstreamCommit,
  nativeSourceCommit: nativeProvenance.commit,
  forkBaseCommit: execFileSync("git", ["rev-parse", "HEAD"], {
    encoding: "utf8",
  }).trim(),
  postgresSourceCommit: values["postgres-source-sha"],
  node: process.version,
  results: [],
  assertions: {},
  notes: [
    values["client-location"] ??
      "Actual remote Turbopuffer vs local Docker PG: not a same-region network comparison.",
    "All assembly samples include remote reads, projection construction/migrations/FTS rebuild, exact upstream assembler and projection disposal.",
    "Only deterministic fake inference at the actual upstream summary boundary. Compaction n=3, so p95=max.",
    "Cold Turbopuffer assembly creates a fresh client per sample. Warm assembly/search/compaction prime verified immutable blocks outside measured samples, but every operation strongly reads the root and creates its own disposable projection. No cached head or shared writable SQLite session.",
    "Turbopuffer is canonical. Its SQLite :memory: projection is disposable; no persistent local database or PostgreSQL underneath it.",
  ],
};
const input = {
  conversationId: 1,
  tokenBudget: 16000,
  freshTailCount: 80,
  freshTailMaxTokens: 65536,
};
function subset(org, cid) {
  const owner = org.tables.conversations.find((r) => r.conversation_id === cid);
  const mids = new Set(
    org.tables.messages
      .filter((r) => r.conversation_id === cid)
      .map((r) => r.message_id),
  );
  const sids = new Set(
    org.tables.summaries
      .filter((r) => r.conversation_id === cid)
      .map((r) => r.summary_id),
  );
  return Object.fromEntries(
    Object.entries(org.tables)
      .filter(([name]) => name !== "lcm_migration_state")
      .map(([name, rows]) => [
        name,
        rows.filter((r) =>
          r.conversation_id != null
            ? r.conversation_id === cid
            : r.message_id != null
            ? mids.has(r.message_id)
            : r.summary_id != null
            ? sids.has(r.summary_id)
            : r.session_id != null
            ? r.session_id === owner.session_id
            : false,
        ),
      ]),
  );
}
function baseline(data) {
  const db = new DatabaseSync(":memory:");
  runLcmMigrations(db, { fts5Available: true });
  db.exec("PRAGMA foreign_keys=OFF; BEGIN");
  for (const [name, rows] of Object.entries(data))
    for (const row of rows) {
      const cols = Object.keys(row);
      db.prepare(
        `INSERT INTO "${name}" (${cols
          .map((c) => `"${c}"`)
          .join(",")}) VALUES(${cols.map(() => "?").join(",")})`,
      ).run(...cols.map((c) => row[c]));
    }
  db.exec(
    "INSERT INTO messages_fts(rowid,content) SELECT message_id,content FROM messages; INSERT INTO summaries_fts(summary_id,content) SELECT summary_id,content FROM summaries; COMMIT; PRAGMA foreign_keys=ON",
  );
  sqlites.push(db);
  return new ContextAssembler(
    new ConversationStore(db),
    new SummaryStore(db),
    "UTC",
  );
}
const httpCount = () =>
  stores.reduce((n, s) => n + s.transport.metrics.length, 0);
async function measure(operation, fn, n, expected) {
  const samples = [];
  for (let i = 0; i < n; i++) {
    const start = performance.now(),
      pg = database.roundTrips,
      http = httpCount();
    const result = await fn(i);
    if (expected !== undefined) assert.deepEqual(result, expected);
    samples.push({
      ms: performance.now() - start,
      pgRoundTrips: database.roundTrips - pg,
      httpRoundTrips: httpCount() - http,
      resultBytes: Buffer.byteLength(JSON.stringify(result)),
    });
  }
  const ms = samples.map((s) => s.ms).sort((a, b) => a - b);
  evidence.results.push({
    operation,
    count: n,
    p50Ms: ms[Math.ceil(n * 0.5) - 1],
    p95Ms: ms[Math.ceil(n * 0.95) - 1],
    maxMs: ms.at(-1),
    samples,
  });
  console.log(operation, ms[Math.ceil(n * 0.5) - 1].toFixed(2) + "ms p50");
}
try {
  await dbm.migrate(database);
  await transfer.importFixture(database, fixture, manifest.sha256);
  await database.query(
    "ANALYZE lcm_experiment.conversations; ANALYZE lcm_experiment.messages; ANALYZE lcm_experiment.summaries; ANALYZE lcm_experiment.context_items; ANALYZE lcm_experiment.summary_messages; ANALYZE lcm_experiment.summary_parents; ANALYZE lcm_experiment.auxiliary_rows; ANALYZE lcm_experiment.large_files",
  );
  const pg = new PostgresStore(database, {
    organizationId: "org1",
    agentId: "agent1",
  });
  const base = baseline(subset(fixture.organizations[0], 1));
  const expected = await base.assemble(input);
  evidence.assemblyOutputSha256 = sha(JSON.stringify(expected));
  function createRemote(cid) {
    const store = new TurbopufferContextStorage({
      backend: "turbopuffer",
      namespacePrefix: prefix,
      organizationId: "org1",
      agentId: "agent1",
      conversationId: cid,
      apiKey: key,
    });
    stores.push(store);
    return store;
  }
  for (const cid of [1, 2, 3]) {
    const store = createRemote(cid);
    await store.initialize();
    await store.importTables(subset(fixture.organizations[0], cid));
  }
  const originals = stores.slice();
  const remote = stores[0];
  await measure(
    "sqlite-full-assembly",
    () => base.assemble(input),
    5,
    expected,
  );
  await measure(
    "postgres-unbatched-full-assembly",
    () => adapter.upstreamAssembler(pg).assemble(input),
    5,
    expected,
  );
  await measure(
    "postgres-batched-full-assembly",
    () => adapter.batchedUpstreamAssembler(pg).assemble(input),
    5,
    expected,
  );
  await measure(
    "turbopuffer-fork-cold-full-assembly",
    () => createRemote(1).assemble(input),
    5,
    expected,
  );
  // Prime each original client explicitly. Warm samples still strongly read the
  // root; only immutable blocks are cached. Cold samples above use fresh clients.
  for (const store of originals)
    await store.assemble({ ...input, conversationId: store.conversationId });
  await measure(
    "turbopuffer-fork-warm-full-assembly",
    () => remote.assemble(input),
    5,
    expected,
  );
  const fulltext = {
    conversationId: 1,
    query: "quartz",
    mode: "full_text",
    limit: 20,
  };
  await measure("postgres-message-fts", () => pg.searchMessages(fulltext), 5);
  await measure(
    "turbopuffer-fork-warm-message-fts",
    () => remote.searchMessages(fulltext),
    5,
  );
  await measure(
    "postgres-batched-leaf-compaction",
    (i) =>
      adapter
        .batchedUpstreamCompactor(pg, i + 1, adapter.compactionConfig)
        .compactLeaf({
          conversationId: i + 1,
          tokenBudget: 2000,
          force: true,
          summarize: async () => "Deterministic snapshot benchmark summary.",
        }),
    3,
  );
  await measure(
    "turbopuffer-fork-warm-leaf-compaction",
    (i) =>
      originals[i].compactLeaf("paired-leaf", adapter.compactionConfig, {
        conversationId: i + 1,
        tokenBudget: 2000,
        force: true,
        summarize: async () => "Deterministic snapshot benchmark summary.",
      }),
    3,
  );
  for (let i = 0; i < 3; i++) {
    const result = await originals[i].assemble({
      ...input,
      conversationId: i + 1,
    });
    // Upstream generates summary IDs with a random nonce, independently on each
    // backend. Normalize only that new reference, never content or message data.
    const normalized = (value) => {
      const copy = JSON.parse(
        JSON.stringify(value).replace(
          /sum_[0-9a-f]{16,64}/g,
          "sum_generated_summary",
        ),
      );
      // These three diagnostics hash the unnormalized random references. All
      // messages, token counts and remaining diagnostics still compare exactly.
      for (const key of [
        "preSanitizeEvictableHash",
        "preSanitizeMessagesHash",
        "finalMessagesHash",
      ])
        delete copy.debug[key];
      return JSON.stringify(copy);
    };
    assert.equal(
      normalized(result),
      normalized(
        await adapter
          .batchedUpstreamAssembler(pg)
          .assemble({ ...input, conversationId: i + 1 }),
      ),
    );
  }
  evidence.assertions.postCompactionAssemblyEqual = true;
  evidence.notes.push(
    "Post-compaction equality normalizes only newly generated random summary IDs and omits three debug digests derived from those references. All message content, token counts and other diagnostics compare exactly. Initial full assembly compares byte-for-byte with no normalization.",
  );
  await assert.rejects(
    remote.assemble({ ...input, conversationId: 7 }),
    /authority/,
  );
  await assert.rejects(
    remote.searchMessages({ ...fulltext, conversationIds: [7] }),
    /authority/,
  );
  assert.equal(
    (await remote.searchMessages({ ...fulltext, query: "canaryORG1AGENT2" }))
      .length,
    0,
  );
  assert.equal(
    (await remote.searchMessages({ ...fulltext, query: "canaryORG2AGENT1" }))
      .length,
    0,
  );
  evidence.assertions.agentAndOrganizationCanaries = true;
  const snapshot = await remote.snapshot();
  try {
    evidence.finalConversation = {
      revision: snapshot.head.revision,
      blocks: snapshot.head.manifest.blocks.length,
      manifestBytes: Buffer.byteLength(JSON.stringify(snapshot.head.manifest)),
      messages: await snapshot.local.conversations.getMessageCount(1),
    };
  } finally {
    snapshot.close();
  }
  evidence.completed = true;
} finally {
  evidence.cleanup = [];
  for (const store of new Map(stores.map((s) => [s.namespace, s])).values()) {
    try {
      assert.ok(store.namespace.startsWith(prefix + "-"));
      await store.transport.request("DELETE", "");
      evidence.cleanup.push({ namespace: store.namespace, deleted: true });
    } catch {
      evidence.cleanup.push({ namespace: store.namespace, deleted: false });
      process.exitCode = 1;
    }
  }
  evidence.httpRequests = stores.flatMap((s) => s.transport.metrics);
  for (const db of sqlites) db.close();
  await pool.end();
  await writeFile(
    path.join(output, "fork-comparison.json"),
    JSON.stringify(evidence, null, 2) + "\n",
  );
}
