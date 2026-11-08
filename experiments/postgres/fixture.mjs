import { createHash } from "node:crypto";
import { mkdir, writeFile, rename, chmod, readFile } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  createLcmDatabaseConnection,
  runLcmMigrations,
  ConversationStore,
  SummaryStore,
} from "./.generated/upstream.mjs";
import { commit } from "./prepare.mjs";
export const seed = 20261104;
const epoch = "2026-11-04T00:00:00.000Z";
const digest = (data) => createHash("sha256").update(data).digest("hex");
export async function generate(directory) {
  await mkdir(directory, { recursive: true });
  let state = seed;
  const next = () =>
    (state = (Math.imul(state, 1664525) + 1013904223) >>> 0) / 2 ** 32;
  const organizations = [];
  let messageBytes = 0;
  for (let org = 1; org <= 2; org++) {
    const db = createLcmDatabaseConnection(":memory:");
    runLcmMigrations(db, { fts5Available: true });
    const conversations = new ConversationStore(db, { fts5Available: true });
    const summaries = new SummaryStore(db, { fts5Available: true });
    const authority = {};
    for (let agent = 1; agent <= 3; agent++) {
      for (let thread = 1; thread <= 6; thread++) {
        const sessionId = `org${org}-agent${agent}-thread${thread}`;
        const conversation = await conversations.createConversation({
          sessionId,
          sessionKey: `agent:agent${agent}:${sessionId}`,
        });
        const cid = conversation.conversationId;
        authority[cid] = `agent${agent}`;
        const inputs = Array.from({ length: 200 }, (_, seq) => {
          const vocabulary = [
            "orbit",
            "mercury",
            "quartz",
            "storage",
            "context",
            "cobalt",
            "recall",
            "database",
            "atomic",
            "summary",
            "nebula",
            "archive",
          ];
          let content = `canaryORG${org}AGENT${agent} thread${thread} message${seq} `;
          while (content.length < 400)
            content += vocabulary[Math.floor(next() * vocabulary.length)] + " ";
          content = content.slice(0, 400);
          messageBytes += Buffer.byteLength(content);
          return {
            conversationId: cid,
            seq,
            role: seq % 2 ? "assistant" : "user",
            content,
            tokenCount: 100,
            transcriptEntryId: `${sessionId}-${seq}`,
            createdAt: new Date(Date.parse(epoch) + seq * 1000),
            skipReplayTimestampFloodGuard: true,
          };
        });
        const messages = await conversations.createMessagesBulk(inputs);
        let layer = [];
        for (let start = 0; start < 200; start += 8) {
          const summaryId = `sum_${sessionId}_leaf${start / 8}`;
          await summaries.insertSummary({
            summaryId,
            conversationId: cid,
            kind: "leaf",
            depth: 0,
            content: `canaryORG${org}AGENT${agent} orbit quartz storage messages ${start}-${start + 7}`,
            tokenCount: 20,
            model: "deterministic-fixture",
            sourceMessageTokenCount: 800,
          });
          await summaries.linkSummaryToMessages(
            summaryId,
            messages.slice(start, start + 8).map((m) => m.messageId),
          );
          layer.push(summaryId);
          if (start < 120) await summaries.appendContextSummary(cid, summaryId);
        }
        for (let depth = 1; layer.length > 1; depth++) {
          const parents = [];
          for (let start = 0; start < layer.length; start += 4) {
            const summaryId = `sum_${sessionId}_depth${depth}_${start / 4}`;
            await summaries.insertSummary({
              summaryId,
              conversationId: cid,
              kind: "condensed",
              depth,
              content: `canaryORG${org}AGENT${agent} orbit quartz storage depth ${depth}`,
              tokenCount: 20,
              model: "deterministic-fixture",
            });
            // Upstream's historical name: parent_summary_id points to SOURCE summaries.
            await summaries.linkSummaryToParents(
              summaryId,
              layer.slice(start, start + 4),
            );
            parents.push(summaryId);
          }
          layer = parents;
        }
        await summaries.appendContextMessages(
          cid,
          messages.slice(120).map((m) => m.messageId),
        );
        await summaries.insertLargeFile({
          fileId: `file_${sessionId}`,
          conversationId: cid,
          fileName: "synthetic.txt",
          mimeType: "text/plain",
          byteSize: 400,
          lineCount: 1,
          storageUri: `/synthetic/${sessionId}.txt`,
          explorationSummary: `canaryORG${org}AGENT${agent} synthetic metadata only`,
        });
      }
    }
    const names = db
      .prepare(
        "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' AND name NOT LIKE '%fts%' ORDER BY name",
      )
      .all()
      .map((r) => r.name);
    const tables = {};
    const schema = {};
    for (const name of names) {
      const info = db.prepare(`PRAGMA table_info("${name}")`).all();
      schema[name] = info;
      tables[name] = db
        .prepare(`SELECT * FROM "${name}" ORDER BY rowid`)
        .all()
        .map((row) =>
          Object.fromEntries(
            Object.entries(row).map(([key, value]) => [
              key,
              /_at$/.test(key) && value != null && name !== "messages"
                ? epoch
                : value,
            ]),
          ),
        );
    }
    organizations.push({
      organizationId: `org${org}`,
      authority,
      schema,
      tables,
    });
    db.close();
  }
  const fixture = {
    format: 1,
    seed,
    upstreamVersion: "1.2.0",
    upstreamCommit: commit,
    organizations,
  };
  const data = JSON.stringify(fixture) + "\n";
  const hash = digest(data);
  const fixturePath = path.join(directory, `fixture-${hash}.json`);
  try {
    if ((await readFile(fixturePath, "utf8")) !== data)
      throw new Error("Immutable fixture differs");
  } catch (error) {
    if (error.code !== "ENOENT") throw error;
    await writeFile(fixturePath + ".tmp", data, { flag: "wx" });
    await rename(fixturePath + ".tmp", fixturePath);
    await chmod(fixturePath, 0o444);
  }
  const counts = {};
  for (const org of organizations)
    for (const [name, rows] of Object.entries(org.tables))
      counts[name] = (counts[name] ?? 0) + rows.length;
  const manifest = {
    fixturePath,
    sha256: hash,
    bytes: Buffer.byteLength(data),
    messageContentBytes: messageBytes,
    seed,
    upstreamVersion: "1.2.0",
    upstreamCommit: commit,
    organizations: 2,
    agentsPerOrganization: 3,
    conversationsPerAgent: 6,
    messagesPerConversation: 200,
    messageCharacters: 400,
    leafMessages: 8,
    parentSummaries: 4,
    counts,
    filePayloads: "metadata only; /synthetic paths do not exist",
    generator: "experiments/postgres/fixture.mjs",
  };
  await writeFile(
    path.join(directory, "fixture-manifest.json.tmp"),
    JSON.stringify(manifest, null, 2) + "\n",
  );
  await rename(
    path.join(directory, "fixture-manifest.json.tmp"),
    path.join(directory, "fixture-manifest.json"),
  );
  return manifest;
}
if (process.argv[1] === fileURLToPath(import.meta.url))
  console.log(JSON.stringify(await generate(process.argv[2]), null, 2));
