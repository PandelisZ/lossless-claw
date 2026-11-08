import { performance } from "node:perf_hooks";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir, cpus, totalmem } from "node:os";
import path from "node:path";
import { Database, createPool, migrate } from "./database.mjs";
import {
  readFixture,
  importFixture,
  restoreSqlite,
  contentHash,
  exportFixture,
} from "./transfer.mjs";
import { PostgresStore } from "./store.mjs";
import {
  ConversationStore,
  SummaryStore,
  ContextAssembler,
  CompactionEngine,
} from "./.generated/upstream.mjs";
import {
  upstreamAssembler,
  upstreamCompactor,
  compactionConfig,
} from "./upstream-adapter.mjs";
if (
  process.env.LCM_DISPOSABLE_POSTGRES !== "1" ||
  process.env.PGHOST !== "127.0.0.1"
)
  throw new Error(
    "Benchmark requires explicitly authorized loopback disposable PG",
  );
const pool = createPool();
const db = new Database(pool);
const directory = await mkdtemp(path.join(tmpdir(), "lcm-benchmark-"));
const handles = [];
const results = [];
let sqliteCalls = 0,
  sqliteMs = 0;
function instrument(handle) {
  return new Proxy(handle, {
    get(target, key) {
      if (key === "prepare")
        return (...args) => {
          const statement = target.prepare(...args);
          return new Proxy(statement, {
            get(s, k) {
              const value = s[k];
              return typeof value === "function"
                ? (...a) => {
                    const start = performance.now();
                    sqliteCalls++;
                    try {
                      return value.apply(s, a);
                    } finally {
                      sqliteMs += performance.now() - start;
                    }
                  }
                : value;
            },
          });
        };
      const value = target[key];
      return typeof value === "function"
        ? (...a) => {
            const start = performance.now();
            sqliteCalls++;
            try {
              return value.apply(target, a);
            } finally {
              sqliteMs += performance.now() - start;
            }
          }
        : value;
    },
  });
}
const stat = async () =>
  Number(
    (
      await pool.query(
        "SELECT coalesce(sum(total_exec_time),0) AS ms FROM pg_stat_statements WHERE dbid=(SELECT oid FROM pg_database WHERE datname=current_database()) AND query NOT ILIKE '%pg_stat_statements%'",
      )
    ).rows[0].ms,
  );
function percentile(values, fraction) {
  return values[
    Math.min(values.length - 1, Math.ceil(values.length * fraction) - 1)
  ];
}
async function measure(
  backend,
  operation,
  fn,
  { samples = 60, concurrency = 1 } = {},
) {
  // Warm read paths outside the measured window. Mutations supply samples explicitly.
  if (!/ingest|compaction|dedup/.test(operation))
    for (let i = 0; i < 5; i++) await fn(i);
  const before = {
    trips: db.roundTrips,
    sqliteCalls,
    sqliteMs,
    serverMs: await stat(),
  };
  const values = [];
  let rows = 0,
    bytes = 0;
  const start = performance.now();
  for (let i = 0; i < samples; i += concurrency) {
    await Promise.all(
      Array.from(
        { length: Math.min(concurrency, samples - i) },
        async (_, j) => {
          const t = performance.now();
          const result = await fn(i + j);
          values.push(performance.now() - t);
          rows += Array.isArray(result)
            ? result.length
            : (result?.inserted ?? 1);
          bytes += Buffer.byteLength(JSON.stringify(result) ?? "null");
        },
      ),
    );
  }
  const elapsed = performance.now() - start;
  const server = await stat();
  values.sort((a, b) => a - b);
  const result = {
    backend,
    operation,
    samples,
    concurrency,
    elapsedMs: elapsed,
    p50Ms: percentile(values, 0.5),
    p95Ms: percentile(values, 0.95),
    maxMs: values.at(-1),
    rowsReturnedOrInserted: rows,
    serializedResultBytes: bytes,
    clientNetworkRoundTrips:
      backend === "sqlite" ? 0 : db.roundTrips - before.trips,
    sqliteStatementCalls:
      backend === "sqlite" ? sqliteCalls - before.sqliteCalls : 0,
    sqliteExecutionMs: backend === "sqlite" ? sqliteMs - before.sqliteMs : null,
    postgresServerExecutionMs:
      backend === "sqlite" ? null : server - before.serverMs,
    inferenceMs: 0,
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
  const importStart = performance.now();
  await importFixture(db, fixture, manifest.sha256);
  const importMs = performance.now() - importStart;
  await db.query(
    "ANALYZE lcm_experiment.conversations; ANALYZE lcm_experiment.messages; ANALYZE lcm_experiment.summaries; ANALYZE lcm_experiment.summary_parents; ANALYZE lcm_experiment.summary_messages; ANALYZE lcm_experiment.context_items",
  );
  if (
    contentHash(await exportFixture(db, manifest.sha256)) !==
    contentHash(fixture)
  )
    throw new Error("Benchmark import parity failed");
  const sqlites = fixture.organizations.map((org, i) => {
    const handle = restoreSqlite(org, path.join(directory, `org${i}.db`));
    handles.push(handle);
    const wrapped = instrument(handle);
    return {
      conversations: new ConversationStore(wrapped, { fts5Available: true }),
      summaries: new SummaryStore(wrapped, { fts5Available: true }),
      db: wrapped,
    };
  });
  const sqlite = sqlites[0];
  const store = new PostgresStore(db, {
    organizationId: "org1",
    agentId: "agent1",
  });
  const stores = fixture.organizations.flatMap((org) =>
    [1, 2, 3].map(
      (a) =>
        new PostgresStore(db, {
          organizationId: org.organizationId,
          agentId: `agent${a}`,
        }),
    ),
  );
  async function sqliteExpand(summaryId) {
    const seen = new Set(),
      ids = new Set();
    async function walk(id) {
      if (seen.has(id)) return;
      seen.add(id);
      for (const mid of await sqlite.summaries.getSummaryMessages(id))
        ids.add(mid);
      for (const parent of await sqlite.summaries.getSummaryParents(id))
        await walk(parent.summaryId);
    }
    await walk(summaryId);
    const records = await Promise.all(
      [...ids].map((id) => sqlite.conversations.getMessageById(id)),
    );
    return records.sort((a, b) => a.seq - b.seq);
  }
  for (const backend of ["sqlite", "postgres"]) {
    const pg = backend === "postgres";
    await measure(backend, "recent-tail-80-256KiB", () =>
      pg ? store.recentTail(1) : sqlite.conversations.getLastMessages(1, 80),
    );
    await measure(backend, "full-text-messages", () =>
      pg
        ? store.searchMessages({
            conversationId: 1,
            mode: "full_text",
            query: "quartz orbit",
            limit: 50,
          })
        : sqlite.conversations.searchMessages({
            conversationId: 1,
            mode: "full_text",
            query: "quartz orbit",
            limit: 50,
          }),
    );
    await measure(backend, "full-text-selective-message199", () =>
      pg
        ? store.searchMessages({
            conversationId: 1,
            mode: "full_text",
            query: "message199",
            limit: 50,
          })
        : sqlite.conversations.searchMessages({
            conversationId: 1,
            mode: "full_text",
            query: "message199",
            limit: 50,
          }),
    );
    await measure(backend, "full-text-summaries", () =>
      pg
        ? store.searchSummaries({
            conversationId: 1,
            mode: "full_text",
            query: "quartz orbit",
            limit: 50,
          })
        : sqlite.summaries.searchSummaries({
            conversationId: 1,
            mode: "full_text",
            query: "quartz orbit",
            limit: 50,
          }),
    );
    const assembler = pg
      ? upstreamAssembler(store)
      : new ContextAssembler(sqlite.conversations, sqlite.summaries, "UTC");
    await measure(
      backend,
      "pinned-upstream-assemble-80-tail",
      () =>
        assembler.assemble({
          conversationId: 1,
          tokenBudget: 16000,
          freshTailCount: 80,
          freshTailMaxTokens: 65536,
        }),
      { samples: 40 },
    );
    await measure(backend, "summary-root-expand-200", () =>
      pg
        ? store.expand("sum_org1-agent1-thread1_depth3_0")
        : sqliteExpand("sum_org1-agent1-thread1_depth3_0"),
    );
    if (pg)
      await measure(backend, "batched-context-snapshot-95", () =>
        store.contextSnapshot(1),
      );
  }
  // All six agents / two organizations read concurrently through the same bounded pool.
  await measure(
    "postgres",
    "concurrent-tail-six-agents",
    (i) => stores[i % 6].recentTail((i % 3) * 6 + 1),
    { samples: 120, concurrency: 12 },
  );
  for (const backend of ["sqlite", "postgres"]) {
    const pg = backend === "postgres";
    let c;
    if (pg)
      c = await store.createConversation({
        sessionId: "bench-ingest",
        sessionKey: "agent:agent1:bench-ingest",
      });
    else
      c = await sqlite.conversations.createConversation({
        sessionId: "bench-ingest",
        sessionKey: "agent:agent1:bench-ingest",
      });
    let version = 0;
    const inputs = fixture.organizations[0].tables.messages
      .slice(0, 200)
      .map((m) => ({
        role: m.role,
        content: m.content,
        tokenCount: m.token_count,
        transcriptEntryId: `bench-${m.seq}`,
      }));
    const receipts = new Map();
    async function ingest(i) {
      const messages = inputs.slice(i * 2, i * 2 + 2);
      if (pg) {
        const result = await store.commitTurn({
          conversationId: c.conversationId,
          expectedRevision: version,
          receiptKey: `bench${i}`,
          messages,
        });
        version = result.revision;
        return result;
      }
      return sqlite.conversations.withTransaction(async () => {
        if (receipts.has(i)) return receipts.get(i);
        const records = await sqlite.conversations.createMessagesBulk(
          messages.map((m, j) => ({
            ...m,
            conversationId: c.conversationId,
            seq: i * 2 + j,
            skipReplayTimestampFloodGuard: true,
          })),
        );
        await sqlite.summaries.appendContextMessages(
          c.conversationId,
          records.map((r) => r.messageId),
        );
        sqlite.db
          .prepare(
            "INSERT INTO turn_advancements(advancement_key,payload_hash,session_id,admission_entry_id,terminal_entry_id,message_count) VALUES(?,?,?,?,?,?)",
          )
          .run(
            `bench${i}`,
            "synthetic",
            "bench-ingest",
            `entry${i * 2}`,
            `entry${i * 2 + 1}`,
            2,
          );
        const result = {
          inserted: records.length,
          messageIds: records.map((r) => r.messageId),
        };
        receipts.set(i, result);
        return result;
      });
    }
    await measure(backend, "ingest-two-message-atomic-turn", ingest, {
      samples: 60,
    });
    // SQLite checks its actual persisted receipt, not a process cache.
    await measure(
      backend,
      "dedup-turn-receipt",
      async (i) =>
        pg
          ? store.commitTurn({
              conversationId: c.conversationId,
              expectedRevision: 0,
              receiptKey: `bench${i}`,
              messages: inputs.slice(i * 2, i * 2 + 2),
            })
          : sqlite.db
              .prepare(
                "SELECT * FROM turn_advancements WHERE advancement_key=?",
              )
              .get(`bench${i}`),
      { samples: 60 },
    );
    await measure(
      backend,
      "pinned-upstream-leaf-compaction",
      async (i) => {
        const cid = i + 1;
        const compactor = pg
          ? upstreamCompactor(store, cid, compactionConfig)
          : new CompactionEngine(
              sqlite.conversations,
              sqlite.summaries,
              compactionConfig,
            );
        return compactor.compactLeaf({
          conversationId: cid,
          tokenBudget: 2000,
          force: true,
          summarize: async () => "Deterministic summary: orbit quartz storage.",
        });
      },
      { samples: 6 },
    );
  }
  const ginPlan = (
    await pool.query(
      "EXPLAIN (ANALYZE,BUFFERS,FORMAT JSON) SELECT message_id FROM lcm_experiment.messages WHERE organization_id='org1' AND agent_id='agent1' AND search @@ phraseto_tsquery('simple','message199')",
    )
  ).rows[0]["QUERY PLAN"];
  const sizes = (
    await pool.query(
      "SELECT pg_relation_size('lcm_experiment.messages') AS messages_heap_bytes,pg_total_relation_size('lcm_experiment.messages') AS messages_total_bytes,pg_relation_size('lcm_experiment.lcm_message_fts') AS messages_fts_bytes,pg_total_relation_size('lcm_experiment.summaries') AS summaries_total_bytes",
    )
  ).rows[0];
  const counts = (
    await pool.query(
      "SELECT (SELECT count(*) FROM lcm_experiment.messages) AS messages,(SELECT count(*) FROM lcm_experiment.summaries) AS summaries,(SELECT count(*) FROM lcm_experiment.context_items) AS context_items",
    )
  ).rows[0];
  const evidence = {
    recordedAt: new Date().toISOString(),
    fixture: manifest,
    importMs,
    environment: {
      node: process.version,
      postgres: (await pool.query("SHOW server_version")).rows[0]
        .server_version,
      transport:
        process.env.LCM_POOLER === "1"
          ? "PgBouncer-1.24.1-transaction-pool-2"
          : "direct-loopback-docker",
      poolMax: 4,
      hostCpu: cpus()[0]?.model,
      logicalCpus: cpus().length,
      hostMemoryBytes: totalmem(),
      sqliteJournalMode: "wal",
      sqliteSynchronous: "normal",
      postgresSynchronousCommit: (await pool.query("SHOW synchronous_commit"))
        .rows[0].synchronous_commit,
      postgresFsync: (await pool.query("SHOW fsync")).rows[0].fsync,
    },
    sizes,
    counts,
    ginPlan,
    notes: [
      "Warm local synthetic benchmark; no production, WAN, replica or inference latency.",
      "SQLite ingest uses pinned store methods plus persisted turn receipt; it is not a full gateway commitTurn benchmark.",
      "PG server execution time is pg_stat_statements delta; excludes parse/plan, wire, JSON decode and JS. Client timings include these.",
      "Result byte counts are JSON serialization size, not captured wire bytes. SQLite statements are executions, zero network round trips.",
      "Fake inference returns a constant at the upstream LLM boundary; no artificial sleeps.",
      "Compaction samples are six separate conversations, one pass each; low sample count limits tail estimates.",
    ],
    results,
  };
  await writeFile(process.argv[2], JSON.stringify(evidence, null, 2) + "\n");
} finally {
  for (const h of handles) h.close();
  await pool.end();
  await rm(directory, { recursive: true, force: true });
}
