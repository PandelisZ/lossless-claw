import { test, before, after } from "node:test";
import assert from "node:assert/strict";
import { mkdtemp, rm, readFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import path from "node:path";
import { Database, createPool, migrate, Conflict } from "./database.mjs";
import {
  readFixture,
  importFixture,
  exportFixture,
  contentHash,
  restoreSqlite,
  hash,
} from "./transfer.mjs";
import { PostgresStore, openExperimentalStore } from "./store.mjs";
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
let pool,
  db,
  fixture,
  manifest,
  directory,
  sqlite,
  baseline,
  baseSummaries,
  store;
const authority = { organizationId: "org1", agentId: "agent1" };
before(async () => {
  if (
    process.env.LCM_DISPOSABLE_POSTGRES !== "1" ||
    process.env.PGHOST !== "127.0.0.1"
  )
    throw new Error(
      "Tests require explicitly authorized loopback disposable PG",
    );
  pool = createPool();
  db = new Database(pool);
  await db.query("DROP SCHEMA IF EXISTS lcm_experiment CASCADE");
  await migrate(db);
  ({ fixture, manifest } = await readFixture(
    process.env.LCM_FIXTURE_MANIFEST ??
      "/tmp/lossless-claw-postgres-fixture/fixture-manifest.json",
  ));
  directory = await mkdtemp(path.join(tmpdir(), "lcm-pg-tests-"));
  sqlite = restoreSqlite(
    fixture.organizations[0],
    path.join(directory, "baseline.db"),
  );
  baseline = new ConversationStore(sqlite, { fts5Available: true });
  baseSummaries = new SummaryStore(sqlite, { fts5Available: true });
  store = openExperimentalStore({
    backend: "postgres",
    database: db,
    authority,
  });
});
after(async () => {
  sqlite?.close();
  await pool?.end();
  if (directory) await rm(directory, { recursive: true, force: true });
});
test("explicit opt-in and authority required", () => {
  assert.throws(
    () => openExperimentalStore({ database: db, authority }),
    /Explicit/,
  );
  assert.throws(() => new PostgresStore(db, {}), /authority/);
});
test("restartable import preserves every table and graph/content hash", async () => {
  let committed = 0;
  await assert.rejects(
    importFixture(db, fixture, manifest.sha256, {
      afterConversation() {
        if (++committed === 3)
          throw new Error("simulated importer termination");
      },
    }),
    /termination/,
  );
  assert.equal(
    Number(
      (await db.query("SELECT count(*) AS n FROM lcm_experiment.conversations"))
        .rows[0].n,
    ),
    3,
  );
  await pool.end();
  pool = createPool();
  db = new Database(pool);
  store = new PostgresStore(db, authority);
  await importFixture(db, fixture, manifest.sha256);
  await importFixture(db, fixture, manifest.sha256);
  const exported = await exportFixture(db, manifest.sha256);
  assert.equal(contentHash(exported), contentHash(fixture));
  const rollback = restoreSqlite(
    exported.organizations[0],
    path.join(directory, "rollback.db"),
  );
  assert.equal(
    rollback.prepare("SELECT count(*) AS n FROM messages").get().n,
    3600,
  );
  assert.equal(
    rollback
      .prepare(
        "SELECT count(*) AS n FROM messages_fts WHERE messages_fts MATCH 'quartz'",
      )
      .get().n,
    sqlite
      .prepare(
        "SELECT count(*) AS n FROM messages_fts WHERE messages_fts MATCH 'quartz'",
      )
      .get().n,
  );
  assert.deepEqual(
    rollback
      .prepare("SELECT * FROM summary_parents ORDER BY summary_id,ordinal")
      .all(),
    sqlite
      .prepare("SELECT * FROM summary_parents ORDER BY summary_id,ordinal")
      .all(),
  );
  rollback.close();
});
test("recent tail, context, metadata, graph expansion match pinned SQLite", async () => {
  assert.deepEqual(
    await store.getLastMessages(1, 80),
    await baseline.getLastMessages(1, 80),
  );
  assert.equal((await store.recentTail(1)).length, 80);
  assert.equal((await store.recentTail(1, 80, 1024)).length, 1);
  assert.equal((await store.contextSnapshot(1)).length, 95);
  assert.deepEqual(
    await store.getContextItems(1),
    await baseSummaries.getContextItems(1),
  );
  assert.deepEqual(
    await store.getLargeFile("file_org1-agent1-thread1"),
    await baseSummaries.getLargeFile("file_org1-agent1-thread1"),
  );
  const root = "sum_org1-agent1-thread1_depth3_0";
  assert.equal((await store.expand(root)).length, 200);
  assert.deepEqual(
    await store.getSummaryParents(root),
    await baseSummaries.getSummaryParents(root),
  );
  assert.deepEqual(await store.getSummaryMessageSeqRange(root), {
    minSeq: 0,
    maxSeq: 199,
  });
});
test("exact upstream assembler has equivalent messages and token budget", async () => {
  const input = {
    conversationId: 1,
    tokenBudget: 16000,
    freshTailCount: 80,
    freshTailMaxTokens: 65536,
  };
  const expected = await new ContextAssembler(
    baseline,
    baseSummaries,
    "UTC",
  ).assemble(input);
  const actual = await upstreamAssembler(store).assemble(input);
  assert.deepEqual(actual, expected);
  assert.equal(actual.stats.rawMessageCount, 80);
  assert.equal(actual.stats.summaryCount, 15);
});
test("agent recall spans its six threads and excludes other agents/organizations", async () => {
  for (const mode of ["full_text", "literal", "regex"]) {
    const own = await store.searchMessages({
      mode,
      query: "canaryORG1AGENT1",
      limit: 500,
    });
    assert.equal(new Set(own.map((r) => r.conversationId)).size, 6);
    assert.equal(
      (await store.searchMessages({ mode, query: "canaryORG1AGENT2" })).length,
      0,
    );
    assert.equal(
      (await store.searchMessages({ mode, query: "canaryORG2AGENT1" })).length,
      0,
    );
    assert.equal(
      (await store.searchSummaries({ mode, query: "canaryORG1AGENT2" })).length,
      0,
    );
  }
  assert.equal(await store.getConversation(7), null);
  assert.equal(await store.getMessageById(1201), null);
  assert.equal(await store.getSummary("sum_org1-agent2-thread1_leaf0"), null);
  assert.equal(await store.getLargeFile("file_org1-agent2-thread1"), null);
  assert.deepEqual(await store.expand("sum_org1-agent2-thread1_depth3_0"), []);
  assert.deepEqual(
    await store.searchMessages({
      mode: "full_text",
      query: "orbit",
      conversationIds: [],
    }),
    [],
  );
  assert.deepEqual(
    await store.searchMessages({
      mode: "full_text",
      query: "orbit",
      conversationId: 7,
    }),
    [],
  );
  await assert.rejects(
    store.createConversation({
      sessionId: "bad",
      sessionKey: "agent:agent2:bad",
    }),
    /authority/,
  );
  await assert.rejects(
    store.insertLargeFile({
      conversationId: 7,
      fileId: "file_bad",
      storageUri: "/tmp/bad",
    }),
    /foreign key/,
  );
});
async function newConversation(label = "test") {
  return store.createConversation({
    sessionId: randomLabel(label),
    sessionKey: `agent:agent1:${randomLabel(label)}`,
  });
}
let serial = 0;
function randomLabel(label) {
  return `${label}-${++serial}`;
}
const message = (id, content = "orbit quartz atomic context") => ({
  role: "user",
  content,
  tokenCount: 10,
  transcriptEntryId: id,
});
test("turn/cursor/receipt atomicity, ordering, envelope dedup and idempotency conflict", async () => {
  const c = await newConversation("ingest");
  const args = {
    conversationId: c.conversationId,
    expectedRevision: 0,
    receiptKey: "turn1",
    messages: [message("a"), message("b")],
    cursor: { cursor: "visible-2" },
  };
  const result = await store.commitTurn(args);
  assert.equal(result.inserted, 2);
  assert.deepEqual(await store.commitTurn(args), result);
  assert.deepEqual(
    (await store.getMessages(c.conversationId)).map((m) => m.seq),
    [0, 1],
  );
  await assert.rejects(
    store.commitTurn({ ...args, messages: [message("changed")] }),
    /different turn/,
  );
  const dedup = await store.commitTurn({
    ...args,
    receiptKey: "turn2",
    expectedRevision: 1,
  });
  assert.equal(dedup.inserted, 0);
  assert.equal(
    (
      await store.auxiliary(
        "conversation_transcript_cursors",
        (r) => r.conversation_id === c.conversationId,
      )
    )[0].cursor,
    "visible-2",
  );
  await assert.rejects(
    store.commitTurn({
      ...args,
      receiptKey: "invalid",
      expectedRevision: 2,
      messages: [{ ...message("invalid"), content: null }],
      cursor: { cursor: "wrong" },
    }),
  );
  assert.equal(await store.revision(c.conversationId), 2);
  assert.equal(await store.getMessageCount(c.conversationId), 2);
  assert.equal(
    (
      await store.auxiliary(
        "conversation_transcript_cursors",
        (r) => r.conversation_id === c.conversationId,
      )
    )[0].cursor,
    "visible-2",
  );
});
test("concurrent appends have one fenced winner; independent conversations progress", async () => {
  const c = await newConversation("race");
  const result = await Promise.allSettled(
    Array.from({ length: 4 }, (_, i) =>
      store.commitTurn({
        conversationId: c.conversationId,
        expectedRevision: 0,
        receiptKey: `r${i}`,
        messages: [message(`r${i}`)],
      }),
    ),
  );
  assert.equal(result.filter((r) => r.status === "fulfilled").length, 1);
  assert.equal(
    result.filter(
      (r) => r.status === "rejected" && r.reason instanceof Conflict,
    ).length,
    3,
  );
  const cs = await Promise.all(
    Array.from({ length: 8 }, (_, i) => newConversation(`parallel${i}`)),
  );
  const independent = await Promise.all(
    cs.map((c, i) =>
      store.commitTurn({
        conversationId: c.conversationId,
        expectedRevision: 0,
        receiptKey: `p${i}`,
        messages: [message(`p${i}`)],
      }),
    ),
  );
  assert.ok(independent.every((r) => r.inserted === 1));
  assert.ok(pool.totalCount <= 4);
});
test("compaction is atomic, stale writes fenced, sources ordered, graph acyclic", async () => {
  const cid = 2;
  const revision = await store.revision(cid);
  const context = await store.getContextItems(cid);
  const input = {
    conversationId: cid,
    expectedRevision: revision,
    summary: {
      summaryId: "sum_atomic",
      depth: 1,
      content: "orbit quartz compacted",
      tokenCount: 5,
    },
    sourceSummaryIds: context.slice(0, 4).map((i) => i.summaryId),
    startOrdinal: 0,
    endOrdinal: 3,
  };
  await assert.rejects(
    store.commitCompaction({
      ...input,
      summary: { ...input.summary, depth: 0 },
    }),
    /depth/,
  );
  assert.equal(await store.getSummary("sum_atomic"), null);
  await store.commitCompaction(input);
  assert.equal((await store.getContextItems(cid)).length, 92);
  assert.equal((await store.expand("sum_atomic")).length, 32);
  await assert.rejects(store.commitCompaction(input), Conflict);
  const graphBefore = await store.getSummariesByConversation(cid);
  // Force FK failure AFTER the summary was inserted; the entire write set rolls back.
  const updated = await store.getContextItems(cid);
  const victim = updated.find((c) => c.messageId);
  const actualId = victim.messageId;
  const data = (await store.rows("messages", "message_id=$3", [actualId]))[0];
  await db.transaction(async () => {
    // Defer failure injection to the store's insertion function via a PostgreSQL trigger.
    await db.query(
      "CREATE FUNCTION lcm_experiment.reject_edge() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'injected graph failure'; END $$",
    );
    await db.query(
      "CREATE TRIGGER reject_edge BEFORE INSERT ON lcm_experiment.summary_messages FOR EACH ROW EXECUTE FUNCTION lcm_experiment.reject_edge()",
    );
  });
  try {
    await assert.rejects(
      store.commitCompaction({
        conversationId: cid,
        expectedRevision: revision + 1,
        summary: {
          summaryId: "sum_rolled_back",
          content: data.content,
          tokenCount: 2,
        },
        messageIds: [actualId],
        startOrdinal: victim.ordinal,
        endOrdinal: victim.ordinal,
      }),
      /injected graph failure/,
    );
  } finally {
    await db.query(
      "DROP TRIGGER reject_edge ON lcm_experiment.summary_messages; DROP FUNCTION lcm_experiment.reject_edge()",
    );
  }
  assert.deepEqual(await store.getSummariesByConversation(cid), graphBefore);
  assert.deepEqual(await store.getContextItems(cid), updated);
});
test("actual upstream leaf compactor runs with deterministic inference only at LLM boundary", async () => {
  const input = {
    conversationId: 3,
    tokenBudget: 2000,
    force: true,
    summarize: async () => "Deterministic summary: orbit quartz storage.",
  };
  const result = await upstreamCompactor(
    store,
    3,
    compactionConfig,
  ).compactLeaf(input);
  const expected = await new CompactionEngine(
    baseline,
    baseSummaries,
    compactionConfig,
  ).compactLeaf(input);
  assert.equal(result.actionTaken, expected.actionTaken);
  assert.deepEqual(
    (await store.getContextItems(3)).map((r) => r.itemType),
    (await baseSummaries.getContextItems(3)).map((r) => r.itemType),
  );
  const newSummary = (await store.getSummariesByConversation(3)).find(
    (r) => r.content === "Deterministic summary: orbit quartz storage.",
  );
  assert.ok(newSummary);
  assert.equal(newSummary.kind, "leaf");
  assert.equal(
    (await store.getSummaryMessages(newSummary.summaryId)).length,
    8,
  );
});
test("FTS grammar, literal, regex, time scope and known tokenizer differences", async () => {
  const c = await newConversation("search");
  const corpus = [
    'running runner café orbit OR quartz agent:foo foo-bar "quoted phrase" 100% _literal_',
    "run cafe orbit quartz",
    "東京の記憶 alpha beta",
    "alpha far beta",
    "the and not",
    "orbit orbit orbit",
    '{"type":"text","text":"orbit hidden"}',
  ];
  await store.commitTurn({
    conversationId: c.conversationId,
    expectedRevision: 0,
    receiptKey: "corpus",
    messages: corpus.map((content, i) => ({
      ...message(`s${i}`, content),
      createdAt: new Date(Date.UTC(2026, 10, 4, 1, 0, i)).toISOString(),
    })),
  });
  const sqlc = await baseline.createConversation({
    sessionId: "search",
    sessionKey: "agent:agent1:search",
  });
  for (const [i, content] of corpus.entries())
    await baseline.createMessage({
      conversationId: sqlc.conversationId,
      seq: i,
      role: "user",
      content,
      tokenCount: 10,
      transcriptEntryId: `s${i}`,
      createdAt: new Date(Date.UTC(2026, 10, 4, 1, 0, i)),
      skipReplayTimestampFloodGuard: true,
    });
  const pgQuery = (query, mode = "full_text", extra = {}) =>
    store.searchMessages({
      conversationId: c.conversationId,
      query,
      mode,
      ...extra,
    });
  const sqliteQuery = (query, mode = "full_text") =>
    baseline.searchMessages({
      conversationId: sqlc.conversationId,
      query,
      mode,
    });
  for (const query of [
    "orbit OR quartz",
    "agent:foo",
    "foo-bar",
    '"quoted phrase"',
    '"alpha beta"',
    "東京",
    "the",
  ]) {
    assert.equal(
      (await pgQuery(query)).length,
      (await sqliteQuery(query)).length,
      query,
    );
  }
  for (const query of ["100%", "_literal_"])
    assert.equal((await pgQuery(query, "literal")).length, 1);
  for (const query of ["orbit.*quartz", "(?<=orbit )OR", "[", "(a+)+"])
    assert.equal(
      (await pgQuery(query, "regex")).length,
      (await sqliteQuery(query, "regex")).length,
      query,
    );
  assert.equal(
    (
      await pgQuery("orbit", "full_text", {
        since: new Date("2026-11-04T01:00:01Z"),
        before: new Date("2026-11-04T01:00:02Z"),
      })
    ).length,
    1,
  );
  // Explicit expected incompatibilities, not altered upstream expectations.
  assert.equal((await sqliteQuery("running")).length, 2);
  assert.equal((await pgQuery("running")).length, 1);
  assert.equal((await sqliteQuery("cafe")).length, 2);
  assert.equal((await pgQuery("cafe")).length, 1);
  const ranked = await pgQuery("orbit", "full_text", { sort: "relevance" });
  assert.equal(ranked[0].snippet, "orbit orbit orbit");
  for (const sort of ["recency", "hybrid", "relevance"])
    assert.ok((await pgQuery("orbit", "full_text", { sort })).length > 0);
});
test("transaction settings do not leak, aborted connections roll back and pool recovers", async () => {
  await assert.rejects(
    db.transaction(async () => {
      await db.query("SET LOCAL application_name='lcm-injected'");
      await db.query("SELECT 1/0");
    }),
    /division by zero/,
  );
  assert.notEqual(
    (await db.query("SHOW application_name")).rows[0].application_name,
    "lcm-injected",
  );
  const c = await newConversation("disconnect");
  await assert.rejects(
    store.mutate(c.conversationId, 0, async () => {
      await db.query("SELECT pg_terminate_backend(pg_backend_pid())");
    }),
  );
  assert.equal(await store.revision(c.conversationId), 0);
  assert.equal((await db.query("SELECT 42 AS n")).rows[0].n, 42);
});
test("limited organization role enforces tenant isolation even for direct SQL", async () => {
  await db.query(
    "DO $$ BEGIN IF NOT EXISTS(SELECT FROM pg_roles WHERE rolname='lcm_pg_org1') THEN CREATE ROLE lcm_pg_org1 LOGIN; END IF; END $$",
  );
  await db.query(
    "INSERT INTO lcm_experiment.tenant_roles VALUES('lcm_pg_org1','org1') ON CONFLICT DO NOTHING",
  );
  await db.query(
    "GRANT USAGE ON SCHEMA lcm_experiment TO lcm_pg_org1; GRANT SELECT ON lcm_experiment.tenant_roles TO lcm_pg_org1; GRANT SELECT,INSERT,UPDATE,DELETE ON lcm_experiment.conversations,lcm_experiment.messages,lcm_experiment.summaries,lcm_experiment.summary_messages,lcm_experiment.summary_parents,lcm_experiment.context_items,lcm_experiment.large_files,lcm_experiment.auxiliary_rows,lcm_experiment.turn_receipts TO lcm_pg_org1; GRANT USAGE ON ALL SEQUENCES IN SCHEMA lcm_experiment TO lcm_pg_org1",
  );
  await db.transaction(async () => {
    await db.query("SET LOCAL ROLE lcm_pg_org1");
    assert.deepEqual(
      (
        await db.query(
          "SELECT DISTINCT organization_id FROM lcm_experiment.messages",
        )
      ).rows,
      [{ organization_id: "org1" }],
    );
    const scoped = new PostgresStore(db, authority);
    assert.equal((await scoped.recentTail(1)).length, 80);
    assert.equal(
      (
        await new PostgresStore(db, {
          organizationId: "org2",
          agentId: "agent1",
        }).recentTail(1)
      ).length,
      0,
    );
    const c = await scoped.createConversation({
      sessionId: "limited",
      sessionKey: "agent:agent1:limited",
    });
    assert.equal(
      (
        await scoped.commitTurn({
          conversationId: c.conversationId,
          expectedRevision: 0,
          receiptKey: "limited",
          messages: [message("limited")],
        })
      ).inserted,
      1,
    );
  });
  await assert.rejects(
    db.transaction(async () => {
      await db.query("SET LOCAL ROLE lcm_pg_org1");
      await db.query(
        "UPDATE lcm_experiment.tenant_roles SET organization_id='org2'",
      );
    }),
    /permission denied/,
  );
  await assert.rejects(
    db.transaction(async () => {
      await db.query("SET LOCAL ROLE lcm_pg_org1");
      await new PostgresStore(db, {
        organizationId: "org2",
        agentId: "agent1",
      }).createConversation({
        sessionId: "forbidden",
        sessionKey: "agent:agent1:forbidden",
      });
    }),
    /row-level security/,
  );
});
test("stable key collision preserves differing content and native rollback preserves new structured turns", async () => {
  const c = await newConversation("structured");
  const first = {
    ...message("structured1", "tool call content"),
    stableEventKey: "response:one",
    openClawSenderMetadata: {
      senderId: "sender1",
      senderName: "Synthetic Sender",
      untrusted: "ignored",
    },
    parts: [
      {
        sessionId: c.sessionId,
        partType: "text",
        textContent: "tool call content",
        ordinal: 0,
      },
    ],
  };
  await store.commitTurn({
    conversationId: c.conversationId,
    expectedRevision: 0,
    receiptKey: "structured1",
    messages: [first],
    cursor: {
      cursor: "v1",
      frontierEntryId: "structured1",
      frontierSeq: 1,
      origin: "fresh",
    },
    advancement: {
      sessionId: c.sessionId,
      sessionKey: c.sessionKey,
      admissionEntryId: "admit",
      terminalEntryId: "terminal",
    },
  });
  await store.commitTurn({
    conversationId: c.conversationId,
    expectedRevision: 1,
    receiptKey: "structured2",
    messages: [
      {
        ...message("structured2", "different content"),
        stableEventKey: "response:one",
      },
    ],
  });
  const messages = await store.getMessages(c.conversationId);
  assert.equal(messages.length, 2);
  assert.deepEqual(messages[0].openClawSenderMetadata, {
    senderId: "sender1",
    senderName: "Synthetic Sender",
  });
  assert.equal(
    (await store.getMessageParts(messages[0].messageId))[0].textContent,
    "tool call content",
  );
  await assert.rejects(
    store.commitTurn({
      conversationId: c.conversationId,
      expectedRevision: 2,
      receiptKey: "bad-envelope",
      messages: [message("structured1", "changed")],
    }),
    /Transcript entry/,
  );
  const exported = await exportFixture(db, manifest.sha256);
  const restored = restoreSqlite(
    exported.organizations[0],
    path.join(directory, "new-turn-rollback.db"),
  );
  try {
    const reread = new ConversationStore(restored, { fts5Available: true });
    assert.deepEqual(await reread.getMessages(c.conversationId), messages);
    assert.equal(
      restored
        .prepare(
          "SELECT mode FROM conversation_transcript_cursors WHERE conversation_id=?",
        )
        .get(c.conversationId).mode,
      "cursor_v1",
    );
    assert.equal(
      restored
        .prepare(
          "SELECT count(*) AS n FROM turn_advancements WHERE advancement_key='structured1'",
        )
        .get().n,
      1,
    );
    assert.equal(restored.prepare("PRAGMA foreign_key_check").all().length, 0);
  } finally {
    restored.close();
  }
  await store.archiveConversation(c.conversationId, "manual-reset", 2);
  assert.equal((await store.getConversation(c.conversationId)).active, false);
  await store.deleteConversation(c.conversationId, 3);
  assert.equal(await store.getConversation(c.conversationId), null);
  assert.equal(await store.getMessageById(messages[0].messageId), null);
});
test("inference runs outside SQL transaction and a concurrent turn fences stale upstream compaction", async () => {
  const cid = 4;
  const before = await store.getSummariesByConversation(cid);
  let called = false;
  await assert.rejects(
    upstreamCompactor(store, cid, compactionConfig).compactLeaf({
      conversationId: cid,
      tokenBudget: 2000,
      force: true,
      summarize: async () => {
        called = true;
        await store.commitTurn({
          conversationId: cid,
          expectedRevision: 0,
          receiptKey: "during-inference",
          messages: [message("during-inference")],
        });
        return "Stale inference summary should never commit.";
      },
    }),
    Conflict,
  );
  assert.ok(called);
  assert.equal(await store.getMessageCount(cid), 201);
  assert.deepEqual(await store.getSummariesByConversation(cid), before);
  assert.equal(await store.revision(cid), 1);
});
test("pinned upstream condensed compaction preserves source-DAG semantics", async () => {
  const cid = 5;
  const config = {
    ...compactionConfig,
    freshTailCount: 80,
    condensedTargetTokens: 20,
    leafChunkTokens: 200,
    summaryPrefixTargetTokens: 1,
    maxSweepIterations: 2,
  };
  const input = {
    conversationId: cid,
    tokenBudget: 1000,
    force: true,
    summarize: async (_text, _aggressive, options) =>
      options?.isCondensed
        ? "Deterministic condensed summary."
        : "Unexpected leaf summary.",
  };
  await upstreamCompactor(store, cid, config).compactFullSweep(input);
  await new CompactionEngine(baseline, baseSummaries, config).compactFullSweep(
    input,
  );
  const actual = await store.getContextItems(cid),
    expected = await baseSummaries.getContextItems(cid);
  assert.deepEqual(
    actual.map((r) => r.itemType),
    expected.map((r) => r.itemType),
  );
  const condensed = (await store.getSummariesByConversation(cid)).find(
    (r) => r.content === "Deterministic condensed summary.",
  );
  assert.ok(condensed);
  assert.equal(condensed.kind, "condensed");
  assert.ok((await store.getSummaryParents(condensed.summaryId)).length >= 4);
  assert.ok((await store.expand(condensed.summaryId)).length >= 32);
});
