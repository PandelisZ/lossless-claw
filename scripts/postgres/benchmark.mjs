import {
  discardAmbientPostgresSecrets,
  requireLoopbackPostgres,
} from "./local-environment.mjs";
import { tmpdir } from "node:os";
// Small comparable full-assembler probe. All rows originate from the immutable synthetic fixture.
import { readFile, mkdir, writeFile, mkdtemp, rm } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { createHash, randomBytes } from "node:crypto";
import { performance } from "node:perf_hooks";
import { isDeepStrictEqual } from "node:util";
import { build } from "esbuild";
import { prepare } from "../../experiments/postgres/prepare.mjs";
if (process.env.LCM_DISPOSABLE_POSTGRES !== "1")
  throw new Error("Requires an explicitly disposable PostgreSQL server");
discardAmbientPostgresSecrets();
requireLoopbackPostgres();
const root = path.resolve(
    path.dirname(fileURLToPath(import.meta.url)),
    "../.."
  ),
  out = path.join(root, "experiments/postgres/.generated/native");
await mkdir(out, { recursive: true });
await prepare();
await build({
  stdin: {
    contents: `export {PostgresDatabase,provisionPostgres} from './src/db/postgres/database.ts';export {loadContextSnapshot} from './src/db/postgres/context-snapshot.ts';export {exportSqlite,exportHash,importSqliteExport} from './src/db/postgres/transfer.ts';export {ConversationStore} from './src/store/conversation-store.ts';export {SummaryStore} from './src/store/summary-store.ts';export {FocusBriefStore} from './src/store/focus-brief-store.ts';export {ContextAssembler} from './src/assembler.ts';export {RetrievalEngine} from './src/retrieval.ts';export {buildMessageParts} from './src/message-content.ts';export {LcmContextEngine} from './src/engine.ts';export {resolveLcmConfig} from './src/db/config.ts';export {LcmContextEngine as FrozenLcmContextEngine} from './experiments/postgres/.generated/sqlite-baseline-source/src/engine.ts';`,
    resolveDir: root,
  },
  bundle: true,
  platform: "node",
  format: "esm",
  external: ["pg", "openclaw", "openclaw/*", "@earendil-works/*"],
  outfile: path.join(out, "benchmark-native.mjs"),
});
const native = await import(path.join(out, "benchmark-native.mjs"));
const frozen = await import(
  "../../experiments/postgres/.generated/upstream.mjs"
);
const { restoreSqlite } = await import(
  "../../experiments/postgres/transfer.mjs"
);
const manifest = JSON.parse(
  await readFile(
    process.argv[2] ??
      "/tmp/lossless-claw-postgres-fixture/fixture-manifest.json",
    "utf8"
  )
);
const bytes = await readFile(manifest.fixturePath);
if (
  createHash("sha256").update(bytes).digest("hex") !== manifest.sha256 ||
  manifest.seed !== 20261104
)
  throw new Error("Immutable fixture hash/seed mismatch");
const fixture = JSON.parse(bytes),
  source = fixture.organizations[0],
  cid = 1;
const messageIds = new Set(
  source.tables.messages
    .filter((r) => r.conversation_id === cid)
    .map((r) => r.message_id)
);
const summaryIds = new Set(
  source.tables.summaries
    .filter((r) => r.conversation_id === cid)
    .map((r) => r.summary_id)
);
const tables = Object.fromEntries(
  Object.entries(source.tables).map(([name, rows]) => [
    name,
    rows.filter((row) =>
      row.conversation_id != null
        ? row.conversation_id === cid
        : row.summary_id != null
        ? summaryIds.has(row.summary_id)
        : row.message_id != null
        ? messageIds.has(row.message_id)
        : true
    ),
  ])
);
const sqliteDirectory = await mkdtemp(
  path.join(tmpdir(), "lcm-native-benchmark-")
);
const sqlite = restoreSqlite(
  { ...source, tables, authority: { [cid]: "agent1" } },
  path.join(sqliteDirectory, "baseline.db")
);
// Private derivative of the shared export, with deterministic structured-content coverage.
const last = tables.messages.at(-1);
const parts = native.buildMessageParts({
  sessionId: "org1-agent1-thread1",
  message: {
    role: "assistant",
    content: [
      { type: "thinking", thinking: "synthetic reasoning" },
      { type: "text", text: "Structured deterministic assistant output" },
    ],
  },
  fallbackContent: last.content,
});
await new native.ConversationStore(sqlite).createMessageParts(
  last.message_id,
  parts
);
for (const part of parts)
  sqlite
    .prepare(
      "UPDATE message_parts SET part_id=? WHERE message_id=? AND ordinal=?"
    )
    .run(
      `synthetic-seed20261104-part-${part.ordinal}`,
      last.message_id,
      part.ordinal
    );
const config = native.resolveLcmConfig(
  {},
  {
    maxAssemblyTokenBudget: 100000,
    promptAwareEviction: true,
    largeFilesDir: "/synthetic/files",
    databasePath: "/synthetic/unused.db",
  }
);
let inferenceCalls = 0;
const deps = {
  config,
  complete: async () => {
    inferenceCalls++;
    throw new Error("No inference allowed in assembly benchmark");
  },
  callGateway: async () => {
    throw new Error("No gateway calls allowed in assembly benchmark");
  },
  resolveModel: () => ({ provider: "synthetic", model: "synthetic" }),
  parseAgentSessionKey: (key) => {
    const match = /^agent:([^:]+):(.+)$/.exec(key);
    return match ? { agentId: match[1], suffix: match[2] } : null;
  },
  normalizeAgentId: (id) => id ?? "agent1",
  isSubagentSessionKey: (key) => key.includes(":subagent:"),
  buildSubagentSystemPrompt: () => "synthetic",
  readLatestAssistantReply: () => undefined,
  resolveAgentDir: () => "/synthetic",
  agentLaneSubagent: "subagent",
  log: { info() {}, warn() {}, error() {}, debug() {} },
};
const sqliteEngine = new native.FrozenLcmContextEngine(deps, sqlite);
const data = native.exportSqlite(sqlite),
  sliceHash = native.exportHash(data);
const schema = `lcm_bench_${randomBytes(6).toString("hex")}`,
  identity = { schema, organizationId: "org1", agentId: "agent1" };
await native.provisionPostgres(identity);
const database = await native.PostgresDatabase.open(identity);
const pgConversations = new native.ConversationStore(database),
  pgSummaries = new native.SummaryStore(database),
  pgFocus = new native.FocusBriefStore(database);
const sqliteAssembler = new frozen.ContextAssembler(
  new frozen.ConversationStore(sqlite),
  new frozen.SummaryStore(sqlite),
  "UTC",
  new native.FocusBriefStore(sqlite)
);
const unbatched = new native.ContextAssembler(
  pgConversations,
  pgSummaries,
  "UTC",
  pgFocus
);
const batched = async (input) => {
  const snapshot = await native.loadContextSnapshot(database, cid);
  try {
    return await new native.ContextAssembler(
      snapshot.conversationStore,
      snapshot.summaryStore,
      "UTC",
      snapshot.focusBriefStore
    ).assemble(input);
  } finally {
    snapshot.close();
  }
};
try {
  await native.importSqliteExport(database, data);
  const engine = new native.LcmContextEngine(deps, database);
  const engineInput = {
    sessionId: "org1-agent1-thread1",
    sessionKey: "agent:agent1:org1-agent1-thread1",
    messages: [],
    tokenBudget: 100000,
  };
  if (
    !isDeepStrictEqual(
      await engine.assemble(engineInput),
      await sqliteEngine.assemble(engineInput)
    )
  )
    throw new Error("Complete native engine assembly parity failed");
  const deepest = tables.summaries.reduce((best, row) =>
    row.depth > best.depth ? row : best
  );
  const dagInput = {
    summaryId: deepest.summary_id,
    depth: 8,
    includeMessages: true,
    tokenCap: 1000000,
  };
  const pgExpanded = await new native.RetrievalEngine(
    pgConversations,
    pgSummaries
  ).expand(dagInput);
  const sqliteExpanded = await new frozen.RetrievalEngine(
    new frozen.ConversationStore(sqlite),
    new frozen.SummaryStore(sqlite)
  ).expand(dagInput);
  if (
    pgExpanded.messages.length !== 200 ||
    !isDeepStrictEqual(pgExpanded, sqliteExpanded)
  )
    throw new Error("Complete expanded message DAG parity failed");
  const cases = [
    { name: "normal", tokenBudget: 100000 },
    { name: "token-budget", tokenBudget: 900 },
    {
      name: "prompt-and-structured",
      tokenBudget: 1400,
      prompt: "orbit quartz",
      promptAwareEviction: true,
    },
  ];
  for (const test of cases) {
    const input = { conversationId: cid, freshTailCount: 8, ...test };
    const reference = await sqliteAssembler.assemble(input);
    if (
      !isDeepStrictEqual(await unbatched.assemble(input), reference) ||
      !isDeepStrictEqual(await batched(input), reference)
    )
      throw new Error(`Full assembler parity failed: ${test.name}`);
    const nativeInput = {
      ...engineInput,
      tokenBudget: test.tokenBudget,
      prompt: test.prompt,
    };
    if (
      !isDeepStrictEqual(
        await engine.assemble(nativeInput),
        await sqliteEngine.assemble(nativeInput)
      )
    )
      throw new Error(`Complete native engine parity failed: ${test.name}`);
  }
  const input = { conversationId: cid, freshTailCount: 8, tokenBudget: 100000 };
  const execute = database.query.bind(database);
  let active,
    delay = 0;
  database.query = async (...args) => {
    if (active) {
      active.trips++;
      if (delay) await new Promise((resolve) => setTimeout(resolve, delay));
      const started = performance.now();
      try {
        return await execute(...args);
      } finally {
        active.ioMs += performance.now() - started;
      }
    }
    return execute(...args);
  };
  const percentile = (xs, p) =>
    [...xs].sort((a, b) => a - b)[
      Math.min(xs.length - 1, Math.ceil(xs.length * p) - 1)
    ];
  const results = [];
  for (const injectedDelayMs of process.argv.includes("--parity-only")
    ? []
    : [0, 2])
    for (const backend of [
      "sqlite-baseline",
      "sqlite-engine-baseline",
      "postgres-unbatched",
      "postgres-batched",
      "postgres-engine-batched",
    ]) {
      if (injectedDelayMs && backend.startsWith("sqlite")) continue;
      delay = injectedDelayMs;
      const samples = [];
      for (let i = 0; i < 12; i++) {
        active = { trips: 0, ioMs: 0 };
        const started = performance.now();
        const output =
          backend === "sqlite-baseline"
            ? await sqliteAssembler.assemble(input)
            : backend === "sqlite-engine-baseline"
            ? await sqliteEngine.assemble(engineInput)
            : backend === "postgres-engine-batched"
            ? await engine.assemble(engineInput)
            : backend.endsWith("-unbatched")
            ? await unbatched.assemble(input)
            : await batched(input);
        samples.push({
          ...active,
          ms: performance.now() - started,
          outputBytes: Buffer.byteLength(JSON.stringify(output)),
        });
        active = undefined;
      }
      results.push({
        backend,
        injectedDelayMs,
        samples: samples.length,
        concurrency: 1,
        p50: percentile(
          samples.map((s) => s.ms),
          0.5
        ),
        p95: percentile(
          samples.map((s) => s.ms),
          0.95
        ),
        max: Math.max(...samples.map((s) => s.ms)),
        tripsPerAssembly: [...new Set(samples.map((s) => s.trips))],
        p50ClientIoMs: percentile(
          samples.map((s) => s.ioMs),
          0.5
        ),
        outputBytes: samples[0].outputBytes,
      });
    }
  const evidence = {
    environment:
      process.env.LCM_POSTGRES_VIA_POOLER === "1"
        ? "local PostgreSQL through PgBouncer transaction pooling"
        : "direct local PostgreSQL",
    production: false,
    sourceFixtureSha256: manifest.sha256,
    sliceHash,
    seed: manifest.seed,
    scope: {
      organizations: 1,
      agents: 1,
      conversations: 1,
      messages: 200,
      messageContentBytes: 80000,
      structuredParts: parts.length,
      contextItems: 95,
    },
    parityCases: cases.map((c) => c.name),
    completeEngineParityCases: cases.map((c) => c.name),
    expandedDagMessages: pgExpanded.messages.length,
    expandedDagSummaries: pgExpanded.children.length,
    inferenceCalls,
    note: "Complete ContextAssembler calls include snapshot loading and native focus lookup; postgres-engine-batched includes the complete native LcmContextEngine.assemble lifecycle and pressure checks. SQLite uses a warm local on-disk WAL file, not an in-memory database. Client I/O includes round trips and decoding, not isolated server CPU. 2ms/query rows are synthetic injected latency, not production measurements.",
    results,
  };
  await writeFile(
    path.join(
      out,
      process.argv.includes("--parity-only")
        ? process.env.LCM_POSTGRES_VIA_POOLER === "1"
          ? "parity-post-merge-pooler.json"
          : "parity-post-merge-direct.json"
        : process.env.LCM_POSTGRES_VIA_POOLER === "1"
        ? "benchmark-pooler.json"
        : "benchmark-direct.json"
    ),
    JSON.stringify(evidence, null, 2) + "\n"
  );
  console.log(JSON.stringify(evidence, null, 2));
} finally {
  await database.close();
  frozen.closeLcmConnection(sqlite);
  await rm(sqliteDirectory, { recursive: true, force: true });
}
