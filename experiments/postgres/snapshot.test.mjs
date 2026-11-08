import { test, before, after } from "node:test";
import assert from "node:assert/strict";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import path from "node:path";
import { Database, createPool, migrate, Conflict } from "./database.mjs";
import { PostgresStore, record } from "./store.mjs";
import { operationSnapshot, SnapshotLimit } from "./snapshot.mjs";
import { readFixture, importFixture, restoreSqlite } from "./transfer.mjs";
import {
  parseUtcTimestamp,
  ConversationStore,
  SummaryStore,
  ContextAssembler,
  CompactionEngine,
} from "./.generated/upstream.mjs";
import {
  batchedUpstreamAssembler,
  batchedUpstreamCompactor,
  upstreamAssembler,
  compactionConfig,
} from "./upstream-adapter.mjs";
let pool, db, store, sqlite, baseline, summaries, directory;
before(async () => {
  if (
    process.env.LCM_DISPOSABLE_POSTGRES !== "1" ||
    process.env.PGHOST !== "127.0.0.1"
  )
    throw new Error("Explicit loopback disposable database required");
  pool = createPool();
  db = new Database(pool);
  await db.query("DROP SCHEMA IF EXISTS lcm_experiment CASCADE");
  await migrate(db);
  const { fixture, manifest } = await readFixture(
    process.env.LCM_FIXTURE_MANIFEST ??
      "/tmp/lossless-claw-postgres-fixture/fixture-manifest.json",
  );
  await importFixture(db, fixture, manifest.sha256);
  store = new PostgresStore(db, { organizationId: "org1", agentId: "agent1" });
  directory = await mkdtemp(path.join(tmpdir(), "lcm-snapshot-tests-"));
  sqlite = restoreSqlite(
    fixture.organizations[0],
    path.join(directory, "baseline.db"),
  );
  baseline = new ConversationStore(sqlite, { fts5Available: true });
  summaries = new SummaryStore(sqlite, { fts5Available: true });
});
after(async () => {
  sqlite?.close();
  await pool?.end();
  if (directory) await rm(directory, { recursive: true, force: true });
});
const input = {
  conversationId: 1,
  tokenBudget: 16000,
  freshTailCount: 80,
  freshTailMaxTokens: 65536,
};
const message = (id, content = "orbit quartz current turn") => ({
  role: "user",
  content,
  tokenCount: 10,
  transcriptEntryId: id,
});
test("one-statement full upstream assembly matches SQLite for normal, budgets and prompt eviction", async () => {
  for (const options of [
    {},
    { tokenBudget: 1200, freshTailCount: 8 },
    { tokenBudget: 1800, freshTailCount: 8, prompt: "quartz archive" },
    {
      tokenBudget: 1800,
      freshTailCount: 8,
      prompt: "cobalt",
      promptAwareEviction: false,
    },
  ]) {
    const args = { ...input, ...options };
    const expected = await new ContextAssembler(
      baseline,
      summaries,
      "UTC",
    ).assemble(args);
    const before = db.roundTrips;
    assert.deepEqual(
      await batchedUpstreamAssembler(store).assemble(args),
      expected,
    );
    assert.equal(
      db.roundTrips - before,
      1,
      "entire assembly, including snapshot load",
    );
  }
});
test("snapshots enforce scope, size limits, private copies and request lifecycle", async () => {
  const snapshot = await operationSnapshot(store, 1);
  const item = (await snapshot.getContextItems(1))[0];
  item.summaryId = "mutated";
  assert.notEqual((await snapshot.getContextItems(1))[0].summaryId, "mutated");
  assert.equal(await snapshot.getMessageById(1201), null);
  assert.equal(
    await snapshot.getSummary("sum_org1-agent2-thread1_leaf0"),
    null,
  );
  assert.equal(await snapshot.getLargeFile("file_org1-agent2-thread1"), null);
  await assert.rejects(snapshot.getContextItems(7), /authority/);
  await assert.rejects(operationSnapshot(store, 7), /authority/);
  await assert.rejects(
    operationSnapshot(store, 1, { maxRows: 10 }),
    SnapshotLimit,
  );
  await assert.rejects(
    operationSnapshot(store, 1, { maxBytes: 100 }),
    SnapshotLimit,
  );
  const org2 = new PostgresStore(db, {
    organizationId: "org2",
    agentId: "agent1",
  });
  const foreign = await operationSnapshot(org2, 1);
  assert.match((await foreign.getMessageById(1)).content, /canaryORG2AGENT1/);
  foreign.close();
  snapshot.close();
  await assert.rejects(snapshot.getContextItems(1), /closed/);
});
test("wrapper reloads every operation and snapshots remain internally consistent across a turn", async () => {
  const cid = 2;
  const snapshot = await operationSnapshot(store, cid);
  const oldContext = await snapshot.getContextItems(cid);
  const assembler = batchedUpstreamAssembler(store);
  await store.commitTurn({
    conversationId: cid,
    expectedRevision: 0,
    receiptKey: "between-snapshots",
    messages: [
      message(
        "between-snapshots",
        "New request must observe this committed turn.",
      ),
    ],
  });
  assert.deepEqual(await snapshot.getContextItems(cid), oldContext);
  snapshot.close();
  const args = { ...input, conversationId: cid };
  const actual = await assembler.assemble(args);
  assert.deepEqual(actual, await upstreamAssembler(store).assemble(args));
  assert.equal(actual.stats.rawMessageCount, 81);
  await store.commitTurn({
    conversationId: cid,
    expectedRevision: 1,
    receiptKey: "next-request",
    messages: [message("next-request")],
  });
  assert.equal((await assembler.assemble(args)).stats.rawMessageCount, 82);
});
test("structured reasoning/tool parts, sender metadata and token budget match exact SQLite assembler", async () => {
  const sessionId = "snapshot-structured";
  const c = await store.createConversation({
    sessionId,
    sessionKey: `agent:agent1:${sessionId}`,
  });
  sqlite
    .prepare("UPDATE sqlite_sequence SET seq=? WHERE name='conversations'")
    .run(c.conversationId - 1);
  const b = await baseline.createConversation({
    sessionId,
    sessionKey: `agent:agent1:${sessionId}`,
  });
  const inputs = [
    {
      ...message("structured-user", "Find the orbit record"),
      openClawSenderMetadata: {
        senderId: "synthetic-sender",
        senderName: "Synthetic Sender",
      },
    },
    {
      role: "assistant",
      content: "",
      tokenCount: 25,
      transcriptEntryId: "structured-assistant",
      parts: [
        {
          sessionId,
          partType: "reasoning",
          ordinal: 0,
          textContent: "Look up the record",
        },
        {
          sessionId,
          partType: "tool",
          ordinal: 1,
          toolCallId: "call-orbit",
          toolName: "lookup",
          toolInput: '{"term":"orbit"}',
        },
      ],
    },
    {
      role: "tool",
      content: "orbit record found",
      tokenCount: 25,
      transcriptEntryId: "structured-tool",
      parts: [
        {
          sessionId,
          partType: "tool",
          ordinal: 0,
          toolCallId: "call-orbit",
          toolName: "lookup",
          toolOutput: "orbit record found",
          metadata: '{"isError":false}',
        },
      ],
    },
    {
      role: "assistant",
      content: "The orbit record is present.",
      tokenCount: 10,
      transcriptEntryId: "structured-result",
    },
  ];
  await store.commitTurn({
    conversationId: c.conversationId,
    expectedRevision: 0,
    receiptKey: "structured",
    messages: inputs.map((m, i) => ({
      ...m,
      createdAt: new Date(Date.UTC(2026, 10, 4, 1, 0, i)).toISOString(),
    })),
  });
  const persisted = await store.getMessages(c.conversationId);
  sqlite
    .prepare("UPDATE sqlite_sequence SET seq=? WHERE name='messages'")
    .run(persisted[0].messageId - 1);
  for (const [seq, m] of inputs.entries()) {
    const row = await baseline.createMessage({
      ...m,
      conversationId: b.conversationId,
      seq,
      createdAt: new Date(Date.UTC(2026, 10, 4, 1, 0, seq)),
      skipReplayTimestampFloodGuard: true,
    });
    if (m.parts) await baseline.createMessageParts(row.messageId, m.parts);
    await summaries.appendContextMessage(b.conversationId, row.messageId);
  }
  // Align synthetic IDs too: compare the entire upstream result, including
  // ID-bearing overflow diagnostics, rather than discarding debug fields.
  for (const tokenBudget of [2000, 120]) {
    const expected = await new ContextAssembler(
      baseline,
      summaries,
      "UTC",
    ).assemble({
      conversationId: b.conversationId,
      tokenBudget,
      freshTailCount: 2,
      prompt: "orbit",
    });
    const actual = await batchedUpstreamAssembler(store).assemble({
      conversationId: c.conversationId,
      tokenBudget,
      freshTailCount: 2,
      prompt: "orbit",
    });
    assert.deepEqual(actual, expected);
    assert.ok(actual.messages.some((m) => m.role === "toolResult"));
    assert.match(JSON.stringify(actual.messages), /call-orbit/);
  }
});
test("batched leaf compaction matches SQLite, and a turn during inference fences its write", async () => {
  const args = {
    conversationId: 3,
    tokenBudget: 2000,
    force: true,
    summarize: async () => "Snapshot leaf summary: orbit quartz.",
  };
  const result = await batchedUpstreamCompactor(
    store,
    3,
    compactionConfig,
  ).compactLeaf(args);
  const expected = await new CompactionEngine(
    baseline,
    summaries,
    compactionConfig,
  ).compactLeaf(args);
  assert.deepEqual(
    { ...result, createdSummaryId: undefined },
    { ...expected, createdSummaryId: undefined },
  );
  assert.deepEqual(
    (await store.getContextItems(3)).map((r) => r.itemType),
    (await summaries.getContextItems(3)).map((r) => r.itemType),
  );
  const old = await store.getSummariesByConversation(4);
  await assert.rejects(
    batchedUpstreamCompactor(store, 4, compactionConfig).compactLeaf({
      ...args,
      conversationId: 4,
      summarize: async () => {
        await store.commitTurn({
          conversationId: 4,
          expectedRevision: 0,
          receiptKey: "during-snapshot-inference",
          messages: [message("during-snapshot-inference")],
        });
        return "Stale snapshot inference must not commit.";
      },
    }),
    Conflict,
  );
  assert.deepEqual(await store.getSummariesByConversation(4), old);
  assert.equal(await store.getMessageCount(4), 201);
});
test("batched condensed multi-pass compaction refreshes after commits and matches SQLite", async () => {
  const config = {
    ...compactionConfig,
    freshTailCount: 80,
    condensedTargetTokens: 20,
    leafChunkTokens: 200,
    summaryPrefixTargetTokens: 1,
    maxSweepIterations: 2,
  };
  const args = {
    conversationId: 5,
    tokenBudget: 1000,
    force: true,
    summarize: async (_text, _aggressive, options) =>
      options?.isCondensed ? "Snapshot condensed summary." : "Snapshot leaf.",
  };
  const actual = await batchedUpstreamCompactor(
    store,
    5,
    config,
  ).compactFullSweep(args);
  const expected = await new CompactionEngine(
    baseline,
    summaries,
    config,
  ).compactFullSweep(args);
  assert.ok(
    (await store.revision(5)) >= 2,
    "multi-pass compaction committed with refreshed snapshots",
  );
  assert.equal(actual.actionTaken, true);
  assert.equal(actual.condensed, true);
  assert.deepEqual(
    { ...actual, createdSummaryId: undefined },
    { ...expected, createdSummaryId: undefined },
  );
  assert.deepEqual(
    (await store.getContextItems(5)).map((r) => r.itemType),
    (await summaries.getContextItems(5)).map((r) => r.itemType),
  );
  const assembly = { ...input, conversationId: 5, prompt: "quartz" };
  assert.deepEqual(
    await batchedUpstreamAssembler(store).assemble(assembly),
    await upstreamAssembler(store).assemble(assembly),
  );
});
test("same body/key/revision racing replays have one winner, explicit conflict and coherent retry", async () => {
  const c = await store.createConversation({
    sessionId: "receipt-race",
    sessionKey: "agent:agent1:receipt-race",
  });
  const args = {
    conversationId: c.conversationId,
    expectedRevision: 0,
    receiptKey: "identical-race",
    messages: [message("identical-a"), message("identical-b")],
    cursor: { cursor: "identical-frontier" },
  };
  const original = db.query.bind(db);
  let arrivals = 0,
    release;
  const barrier = new Promise((resolve) => {
    release = resolve;
  });
  // Real queries on two connections both observe no receipt before either CAS.
  db.query = async (sql, params = []) => {
    const result = await original(sql, params);
    if (
      sql.includes("SELECT fingerprint,result") &&
      params[3] === args.receiptKey
    ) {
      if (++arrivals === 2) release();
      await barrier;
    }
    return result;
  };
  let attempts;
  try {
    attempts = await Promise.allSettled([
      store.commitTurn(args),
      store.commitTurn(args),
    ]);
  } finally {
    db.query = original;
  }
  assert.equal(arrivals, 2);
  const winner = attempts.filter((r) => r.status === "fulfilled");
  assert.equal(winner.length, 1);
  assert.equal(
    attempts.filter(
      (r) => r.status === "rejected" && r.reason instanceof Conflict,
    ).length,
    1,
  );
  assert.deepEqual(await store.commitTurn(args), winner[0].value);
  assert.equal(await store.getMessageCount(c.conversationId), 2);
  assert.equal((await store.getContextItems(c.conversationId)).length, 2);
  assert.equal(await store.revision(c.conversationId), 1);
  assert.equal(
    Number(
      (
        await db.query(
          "SELECT count(*) AS n FROM lcm_experiment.turn_receipts WHERE conversation_id=$1",
          [c.conversationId],
        )
      ).rows[0].n,
    ),
    1,
  );
});
test("tail budget includes serialized sender metadata and JSON envelope, not only content", async () => {
  const c = await store.createConversation({
    sessionId: "tail-metadata",
    sessionKey: "agent:agent1:tail-metadata",
  });
  await store.commitTurn({
    conversationId: c.conversationId,
    expectedRevision: 0,
    receiptKey: "large-metadata",
    messages: [
      {
        ...message("metadata", "x"),
        openClawSenderMetadata: { senderName: "界".repeat(4000) },
      },
    ],
  });
  assert.equal((await store.getLastMessages(c.conversationId, 80)).length, 1);
  assert.deepEqual(await store.recentTail(c.conversationId, 80, 1024), []);
  const tail = await store.recentTail(c.conversationId, 80, 16000);
  assert.equal(tail.length, 1);
  assert.ok(Buffer.byteLength(JSON.stringify(tail)) <= 16000);
  assert.ok(Buffer.byteLength(JSON.stringify(tail)) > 12000);
  assert.deepEqual(await store.recentTail(c.conversationId, 80, 2), []);
});

test("referenced metadata across this agent threads stays scoped and assembles identically", async () => {
  const cid = 6,
    id = 1121,
    fileId = "file_org1-agent1-thread2";
  await db.query(
    "UPDATE lcm_experiment.messages SET data=data||jsonb_build_object('large_content',$1::text) WHERE organization_id='org1' AND agent_id='agent1' AND message_id=$2",
    [fileId, id],
  );
  sqlite
    .prepare("UPDATE messages SET large_content=? WHERE message_id=?")
    .run(fileId, id);
  const snapshot = await operationSnapshot(store, cid);
  try {
    assert.deepEqual(
      await snapshot.getLargeFile(fileId),
      await store.getLargeFile(fileId),
    );
    assert.equal(await snapshot.getLargeFile("file_org1-agent2-thread1"), null);
  } finally {
    snapshot.close();
  }
  const args = { ...input, conversationId: cid, stubLargeToolPayloads: true };
  assert.deepEqual(
    await batchedUpstreamAssembler(store).assemble(args),
    await new ContextAssembler(baseline, summaries, "UTC").assemble(args),
  );
});

test("large tail enforces 256 KiB serialized UTF-8 limit on a contiguous recent suffix", async () => {
  const c = await store.createConversation({
    sessionId: "large-tail",
    sessionKey: "agent:agent1:large-tail",
  });
  const messages = Array.from({ length: 100 }, (_, i) => ({
    ...message(`large-tail-${i}`, `${i} ${"界".repeat(1500)}`),
    openClawSenderMetadata: { senderName: "Synthetic " + "m".repeat(1024) },
  }));
  await store.commitTurn({
    conversationId: c.conversationId,
    expectedRevision: 0,
    receiptKey: "large-tail",
    messages,
  });
  const all = await store.getLastMessages(c.conversationId, 100);
  assert.ok(Buffer.byteLength(JSON.stringify(all)) > 256 * 1024);
  const tail = await store.recentTail(c.conversationId, 80, 256 * 1024);
  assert.ok(tail.length > 0 && tail.length < 80);
  assert.ok(Buffer.byteLength(JSON.stringify(tail)) <= 256 * 1024);
  assert.deepEqual(tail, all.slice(-tail.length));
  await store.commitTurn({
    conversationId: c.conversationId,
    expectedRevision: 1,
    receiptKey: "oversized-newest",
    messages: [
      {
        ...message("oversized-newest", "x"),
        openClawSenderMetadata: { senderName: "m".repeat(300000) },
      },
    ],
  });
  assert.deepEqual(
    await store.recentTail(c.conversationId, 80, 256 * 1024),
    [],
  );
});

test("FTS comparisons use identical conversation, recency sort, dates and result limit", async () => {
  const args = {
    conversationId: 1,
    mode: "full_text",
    query: "orbit quartz",
    sort: "recency",
    limit: 17,
    since: new Date("2026-11-04T00:00:15Z"),
    before: new Date("2026-11-04T00:02:30Z"),
  };
  const project = (row) => ({
    id: row.messageId ?? row.summaryId,
    conversationId: row.conversationId,
    createdAt: row.createdAt,
    role: row.role,
    kind: row.kind,
  });
  const pg = await store.searchMessages(args),
    sq = await baseline.searchMessages(args);
  assert.equal(pg.length, 17);
  assert.deepEqual(pg.map(project), sq.map(project));
  // Summary fixture timestamps tie. Give both disposable copies the same unique
  // timestamps so LIMIT tests compare recency, not backend-specific tie order.
  const records = await store.getSummariesByConversation(1);
  for (const [i, row] of records.entries()) {
    const at = new Date(Date.UTC(2026, 10, 4, 0, 0, i)).toISOString();
    await db.query(
      "UPDATE lcm_experiment.summaries SET data=data||jsonb_build_object('created_at',$1::text) WHERE organization_id='org1' AND agent_id='agent1' AND summary_id=$2",
      [at, row.summaryId],
    );
    sqlite
      .prepare("UPDATE summaries SET created_at=? WHERE summary_id=?")
      .run(at, row.summaryId);
  }
  const summaryArgs = {
    ...args,
    since: new Date("2026-11-04T00:00:01Z"),
    before: new Date("2026-11-04T00:00:34Z"),
    limit: 11,
  };
  const pgs = await store.searchSummaries(summaryArgs),
    sqs = await summaries.searchSummaries(summaryArgs);
  assert.equal(pgs.length, 11);
  assert.deepEqual(pgs.map(project), sqs.map(project));
  // Snippet, rank and the known Porter/accent differences remain deliberately
  // non-equivalent; this compares a common ASCII query's identical selection.
});

test("resolved context and expanded DAG compare full message payloads, not subtree metadata", async () => {
  const expected = [];
  for (const item of await summaries.getContextItems(1))
    expected.push({
      item,
      value: item.messageId
        ? await baseline.getMessageById(item.messageId)
        : await summaries.getSummary(item.summaryId),
    });
  assert.deepEqual(await store.contextSnapshot(1), expected);
  const seen = new Set(),
    ids = new Set();
  async function visit(id) {
    if (seen.has(id)) return;
    seen.add(id);
    for (const mid of await summaries.getSummaryMessages(id)) ids.add(mid);
    for (const parent of await summaries.getSummaryParents(id))
      await visit(parent.summaryId);
  }
  await visit("sum_org1-agent1-thread1_depth3_0");
  const messages = (
    await Promise.all([...ids].map((id) => baseline.getMessageById(id)))
  ).sort((a, b) => a.seq - b.seq);
  assert.equal(messages.length, 200);
  assert.ok(messages.every((row) => row.content.length === 400));
  assert.deepEqual(
    await store.expand("sum_org1-agent1-thread1_depth3_0"),
    messages,
  );
});

test("record timestamp decoding reuses the pinned UTC parser including offsets and whitespace", () => {
  for (const value of [
    "2026-11-04T00:00:00+00:00",
    "2026-11-04T05:30:00+05:30",
    "2026-11-04T00:00:00z",
    " 2026-11-04T00:00:00Z ",
    "2026-11-04 00:00:00",
  ]) {
    assert.deepEqual(
      record({ created_at: value }).createdAt,
      parseUtcTimestamp(value),
    );
    assert.equal(
      record({ created_at: value }).createdAt.toISOString(),
      "2026-11-04T00:00:00.000Z",
    );
  }
  assert.equal(record({ archived_at: null }).archivedAt, null);
});

test("same-agent native conversation family takes precedence over current conversation in recall", async () => {
  for (const mode of ["full_text", "regex"]) {
    const args = {
      conversationId: 1,
      conversationIds: [1, 2],
      mode,
      query: "thread2",
      sort: "recency",
      limit: 20,
    };
    const pg = await store.searchMessages(args),
      sq = await baseline.searchMessages(args);
    assert.equal(pg.length, 20);
    assert.ok(pg.every((row) => row.conversationId === 2));
    assert.deepEqual(
      pg.map((row) => row.messageId),
      sq.map((row) => row.messageId),
    );
    const summaryArgs = { ...args, query: "orbit", limit: 500 };
    const pgs = await store.searchSummaries(summaryArgs),
      sqs = await summaries.searchSummaries(summaryArgs);
    assert.deepEqual(
      pgs.map((row) => row.summaryId).sort(),
      sqs.map((row) => row.summaryId).sort(),
    );
    assert.ok(pgs.some((row) => row.conversationId === 2));
    assert.equal(
      (
        await store.searchMessages({
          ...args,
          conversationIds: [7],
          query: "canaryORG1AGENT2",
        })
      ).length,
      0,
    );
    assert.equal(
      (
        await store.searchMessages({
          ...args,
          conversationIds: [1, 2],
          query: "canaryORG2AGENT1",
        })
      ).length,
      0,
    );
  }
});

test("non-user messages discard sender identity exactly as pinned ingestion does", async () => {
  const c = await store.createConversation({
    sessionId: "nonuser-sender",
    sessionKey: "agent:agent1:nonuser-sender",
  });
  const b = await baseline.createConversation({
    sessionId: "nonuser-sender",
    sessionKey: "agent:agent1:nonuser-sender",
  });
  const messages = ["assistant", "tool"].map((role, i) => ({
    ...message(`nonuser-${i}`),
    role,
    openClawSenderMetadata: { senderId: "must-not-persist" },
  }));
  await store.commitTurn({
    conversationId: c.conversationId,
    expectedRevision: 0,
    receiptKey: "nonuser-sender",
    messages,
  });
  for (const [seq, m] of messages.entries())
    await baseline.createMessage({
      ...m,
      conversationId: b.conversationId,
      seq,
    });
  assert.ok(
    (await store.getMessages(c.conversationId)).every(
      (row) => row.openClawSenderMetadata === null,
    ),
  );
  assert.ok(
    (await baseline.getMessages(b.conversationId)).every(
      (row) => row.openClawSenderMetadata === null,
    ),
  );
});

test("naive SQLite UTC search timestamps do not depend on the pooled backend TimeZone", async () => {
  await db.query(
    "UPDATE lcm_experiment.messages SET data=data||jsonb_build_object('created_at',to_char((data->>'created_at')::timestamptz AT TIME ZONE 'UTC','YYYY-MM-DD HH24:MI:SS')) WHERE organization_id='org1' AND agent_id='agent1' AND conversation_id=1",
  );
  await db.query(
    "UPDATE lcm_experiment.summaries SET data=data||jsonb_build_object('created_at',to_char((data->>'created_at')::timestamptz AT TIME ZONE 'UTC','YYYY-MM-DD HH24:MI:SS')) WHERE organization_id='org1' AND agent_id='agent1' AND conversation_id=1",
  );
  const args = {
    conversationId: 1,
    query: "orbit",
    mode: "full_text",
    sort: "recency",
    limit: 500,
    since: new Date("2026-11-04T00:00:15Z"),
    before: new Date("2026-11-04T00:00:30Z"),
  };
  const expectedMessages = await baseline.searchMessages(args),
    expectedSummaries = await summaries.searchSummaries(args);
  assert.ok(expectedMessages.length > 0 && expectedSummaries.length > 0);
  await db.transaction(async () => {
    await db.query("SET LOCAL TIME ZONE 'America/Los_Angeles'");
    assert.deepEqual(
      (await store.searchMessages(args)).map((r) => r.messageId),
      expectedMessages.map((r) => r.messageId),
    );
    assert.deepEqual(
      (await store.searchSummaries(args)).map((r) => r.summaryId),
      expectedSummaries.map((r) => r.summaryId),
    );
  });
});
