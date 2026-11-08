// Bounded real-provider protocol regression for the combined immutable/root batch.
// Run only with synthetic namespaces; credentials arrive via environment/stdin wrapper.
import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { parseArgs } from "node:util";
import { writeFile } from "node:fs/promises";
import {
  TurbopufferContextStorage,
  StorageConflict,
  UnknownCommit,
} from "../dist/experimental/context-storage.js";
const { values } = parseArgs({ options: { output: { type: "string" } } });
if (process.env.LCM_TURBOPUFFER_EXPERIMENT !== "1" || !values.output)
  throw new Error("Explicit experiment opt-in and output required");
const prefix = `lcm-fork-exp-${randomUUID()}`;
const options = {
  backend: "turbopuffer",
  namespacePrefix: prefix,
  organizationId: "synthetic-org",
  agentId: "synthetic-agent",
  conversationId: 1,
  apiKey: process.env.TURBOPUFFER_API_KEY,
};
const base = new TurbopufferContextStorage(options),
  transport = base.transport;
const evidence = {
  timestamp: new Date().toISOString(),
  namespace: base.namespace,
  assertions: {},
  batches: [],
};
const open = (write) =>
  new TurbopufferContextStorage({
    ...options,
    transport: {
      read: (ids) => transport.read(ids),
      write: async (body) => {
        const result = await (write ? write(body) : transport.write(body));
        evidence.batches.push({
          upserts: body.upsert_rows?.length ?? 0,
          patches: body.patch_rows?.length ?? 0,
          ...result,
        });
        return result;
      },
    },
  });
const message = (content) => [{ role: "user", content, tokenCount: 100 }];
const config = {
  contextThreshold: 0.75,
  freshTailCount: 8,
  leafMinFanout: 4,
  condensedMinFanout: 4,
  condensedMinFanoutHard: 2,
  leafChunkTokens: 800,
  leafTargetTokens: 100,
  condensedTargetTokens: 100,
  maxRounds: 1,
  maxSweepIterations: 1,
  summaryMaxOverageFactor: 3,
  timezone: "UTC",
};
function barrier() {
  let arrived = 0,
    release;
  const gate = new Promise((resolve) => {
    release = resolve;
  });
  return async () => {
    if (++arrived === 2) release();
    await gate;
  };
}
async function snapshotCheck(expectedMessages) {
  const snapshot = await open().snapshot();
  try {
    assert.equal(
      await snapshot.local.conversations.getMessageCount(1),
      expectedMessages,
    );
    assert.equal(
      snapshot.local.db.prepare("PRAGMA foreign_key_check").all().length,
      0,
    );
    const context = await snapshot.local.summaries.getContextItems(1);
    for (const item of context)
      if (item.summaryId) {
        assert.ok(await snapshot.local.summaries.getSummary(item.summaryId));
        assert.equal(
          (await snapshot.local.summaries.getSummaryMessages(item.summaryId))
            .length,
          8,
        );
      }
  } finally {
    snapshot.close();
  }
}
try {
  await base.initialize();
  await base.importTables({
    conversations: [
      {
        conversation_id: 1,
        session_id: "synthetic",
        session_key: "agent:synthetic-agent:synthetic",
      },
    ],
  });
  await base.appendTurn(
    "seed",
    Array.from({ length: 16 }, (_, i) => ({
      role: i % 2 ? "assistant" : "user",
      content: `synthetic quartz ${i}`,
      tokenCount: 100,
    })),
  );
  const wait = barrier();
  const compactions = await Promise.allSettled(
    ["first", "second"].map((label) =>
      open().compactLeaf(`compact-${label}`, config, {
        conversationId: 1,
        tokenBudget: 2000,
        force: true,
        summarize: async () => {
          await wait();
          return `quartz ${label} compaction`;
        },
      }),
    ),
  );
  assert.equal(compactions.filter((x) => x.status === "fulfilled").length, 1);
  assert.ok(
    compactions.find((x) => x.status === "rejected").reason instanceof
      StorageConflict,
  );
  await snapshotCheck(16);
  const winner = compactions[0].status === "fulfilled" ? "first" : "second";
  const s = await open().snapshot();
  try {
    const summaries = s.local.db.prepare("SELECT content FROM summaries").all();
    assert.equal(summaries.length, 1);
    assert.equal(summaries[0].content, `quartz ${winner} compaction`);
  } finally {
    s.close();
  }
  evidence.assertions.competingCompactionsOneWinner = true;
  evidence.assertions.freshClientCoherentGraph = true;
  const gate = barrier();
  const contestants = ["first", "second"].map((label) =>
    open(async (body) => {
      if (body.patch_rows) await gate();
      return transport.write(body);
    }),
  );
  const appends = await Promise.allSettled(
    contestants.map((store, i) =>
      store.appendTurn(`append-${i}`, message(`append ${i}`)),
    ),
  );
  assert.equal(appends.filter((x) => x.status === "fulfilled").length, 1);
  assert.ok(
    appends.find((x) => x.status === "rejected").reason instanceof
      StorageConflict,
  );
  assert.ok(
    evidence.batches.some(
      (x) => x.upserted > 0 && x.patched === 0 && x.patches === 1,
    ),
  );
  await snapshotCheck(17);
  evidence.assertions.losingInsertsAreNotRootSuccess = true;
  evidence.assertions.competingAppendsOneWinner = true;
  const oldHead = await base.head();
  const crashed = open(async () => {
    throw new Error("crash before send");
  });
  await assert.rejects(
    crashed.appendTurn("pre-send-crash", message("must remain absent")),
    UnknownCommit,
  );
  assert.equal((await base.head()).commit, oldHead.commit);
  await snapshotCheck(17);
  evidence.assertions.preBatchCrashNoPublication = true;
  const lost = open(async (body) => {
    const result = await transport.write(body);
    if (body.patch_rows && result.patched === 1) {
      await open().appendTurn("advance-after-lost", message("advance head"));
      throw new Error("lost response after server commit and another writer");
    }
    return result;
  });
  const result = await lost.appendTurn("lost", message("lost but committed"));
  assert.equal((await base.head()).revision, oldHead.revision + 2);
  assert.deepEqual(
    await open().appendTurn("lost", message("lost but committed")),
    result,
  );
  assert.ok(result[0].createdAt instanceof Date);
  await snapshotCheck(19);
  evidence.assertions.lostReplyReconcilesAfterHeadAdvances = true;
  evidence.assertions.freshClientReceiptReplay = true;
  const sameGate = barrier();
  const replies = await Promise.all(
    ["local-A", "local-B"].map((nonce) =>
      open().mutate("same-token", { request: "same" }, async () => {
        await sameGate();
        return { nonce, date: new Date(nonce === "local-A" ? 0 : 1000) };
      }),
    ),
  );
  assert.deepEqual(replies[0], replies[1]);
  evidence.assertions.sameTokenReturnsCommittedResult = true;
  evidence.completed = true;
} finally {
  try {
    assert.ok(base.namespace.startsWith(prefix + "-"));
    await transport.request("DELETE", "");
    evidence.deleted = true;
  } catch {
    evidence.deleted = false;
    process.exitCode = 1;
  }
  evidence.requests = transport.metrics;
  await writeFile(values.output, JSON.stringify(evidence, null, 2) + "\n");
}
