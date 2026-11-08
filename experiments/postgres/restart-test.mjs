import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { performance } from "node:perf_hooks";
import { writeFile } from "node:fs/promises";
import { Database, createPool } from "./database.mjs";
import { PostgresStore } from "./store.mjs";
import { exportFixture, contentHash, readFixture } from "./transfer.mjs";
const name = process.env.LCM_OWNED_POSTGRES_CONTAINER;
if (
  process.env.LCM_DISPOSABLE_POSTGRES !== "1" ||
  process.env.PGHOST !== "127.0.0.1" ||
  !/^lcm-pg-\d+-[a-f0-9]+$/.test(name ?? "")
)
  throw new Error("Only this experiment disposable container may be restarted");
const { manifest } = await readFixture(process.env.LCM_FIXTURE_MANIFEST);
let pool = createPool(),
  db = new Database(pool);
const store = new PostgresStore(db, {
  organizationId: "org1",
  agentId: "agent1",
});
const c = await store.createConversation({
  sessionId: "restart-probe",
  sessionKey: "agent:agent1:restart-probe",
});
await store.commitTurn({
  conversationId: c.conversationId,
  expectedRevision: 0,
  receiptKey: "committed",
  messages: [
    {
      role: "user",
      content: "durable synthetic restart probe",
      tokenCount: 8,
      transcriptEntryId: "restart-committed",
    },
  ],
});
const before = contentHash(await exportFixture(db, manifest.sha256));
const client = await pool.connect();
client.on("error", () => {});
await client.query("BEGIN");
await client.query(
  "UPDATE lcm_experiment.messages SET data=data || '{\"content\":\"UNCOMMITTED\"}'::jsonb WHERE organization_id='org1' AND conversation_id=$1",
  [c.conversationId],
);
const started = performance.now();
execFileSync("docker", ["restart", name], { stdio: "pipe" });
client.release(true);
await pool.end().catch(() => {});
// Docker can reassign a random host port on restart. Never cache that mapping.
process.env.PGPORT = execFileSync("docker", ["port", name, "5432"], {
  encoding: "utf8",
})
  .trim()
  .split(":")
  .at(-1);
pool = createPool();
db = new Database(pool);
let ready = false;
for (let attempt = 0; attempt < 100; attempt++) {
  try {
    await pool.query("SELECT 1");
    ready = true;
    break;
  } catch {
    await new Promise((r) => setTimeout(r, 100));
  }
}
try {
  assert.ok(ready, "PostgreSQL restart did not become ready in 10 seconds");
  const after = contentHash(await exportFixture(db, manifest.sha256));
  assert.equal(after, before);
  const reopened = new PostgresStore(db, {
    organizationId: "org1",
    agentId: "agent1",
  });
  assert.equal(
    (await reopened.getLastMessages(c.conversationId, 1))[0].content,
    "durable synthetic restart probe",
  );
  assert.equal(await reopened.revision(c.conversationId), 1);
  const receipt = await reopened.commitTurn({
    conversationId: c.conversationId,
    expectedRevision: 0,
    receiptKey: "committed",
    messages: [
      {
        role: "user",
        content: "durable synthetic restart probe",
        tokenCount: 8,
        transcriptEntryId: "restart-committed",
      },
    ],
  });
  assert.equal(receipt.inserted, 1);
  const evidence = {
    passed: true,
    beforeContentHash: before,
    afterContentHash: after,
    restartAndVerificationMs: performance.now() - started,
    committedRowsSurvived: 1,
    uncommittedMutationRolledBack: true,
    receiptSurvived: true,
  };
  await writeFile(process.argv[2], JSON.stringify(evidence, null, 2) + "\n");
  console.log(JSON.stringify(evidence));
} finally {
  await pool.end();
}
