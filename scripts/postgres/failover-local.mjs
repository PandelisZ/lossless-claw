import {
  discardAmbientPostgresSecrets,
  requireLoopbackPostgres,
} from "./local-environment.mjs";
// Local crash/replay/promotion mechanics only; not provider HA or unplanned data-loss qualification.
import { execFileSync } from "node:child_process";
import { randomBytes, createHash } from "node:crypto";
import { mkdir, writeFile } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { build } from "esbuild";
import pg from "pg";
discardAmbientPostgresSecrets();
process.env.LCM_DISPOSABLE_POSTGRES = "1";
const root = path.resolve(
    path.dirname(fileURLToPath(import.meta.url)),
    "../.."
  ),
  out = path.join(root, "experiments/postgres/.generated/operations");
await mkdir(out, { recursive: true });
await build({
  stdin: {
    contents:
      "export {PostgresDatabase,provisionPostgres} from './src/db/postgres/database.ts';export {exportNativePostgres,exportHash} from './src/db/postgres/transfer.ts';export {ConversationStore} from './src/store/conversation-store.ts';export {SummaryStore} from './src/store/summary-store.ts';",
    resolveDir: root,
  },
  bundle: true,
  platform: "node",
  format: "esm",
  external: ["pg"],
  outfile: path.join(out, "failover-store.mjs"),
});
const {
  PostgresDatabase,
  provisionPostgres,
  exportNativePostgres,
  exportHash,
  ConversationStore,
  SummaryStore,
} = await import(path.join(out, "failover-store.mjs"));
const id = `lcm-failover-${process.pid}-${randomBytes(4).toString("hex")}`,
  primary = `${id}-primary`,
  standby = `${id}-standby`,
  volume = `${id}-standby-data`,
  seed = `${id}-seed`;
const docker = (...args) =>
  execFileSync("docker", args, {
    encoding: "utf8",
    stdio: ["ignore", "pipe", "pipe"],
  }).trim();
const port = (name) => Number(docker("port", name, "5432").split(":").at(-1));
let db, replica;
const schema = `lcm_failover_${randomBytes(6).toString("hex")}`;
const identity = {
  schema,
  organizationId: "synthetic-failover-org",
  agentId: "one",
};
const poolOptions = (port) => ({
  host: "127.0.0.1",
  port,
  user: "postgres",
  password: () => "",
  database: "postgres",
  max: 1,
  connectionTimeoutMillis: 1000,
});
async function waitReady(options, predicate = async () => true) {
  for (let i = 0; i < 100; i++) {
    const pool = new pg.Pool(options);
    try {
      await pool.query("SELECT 1");
      if (await predicate(pool)) return;
    } catch {
    } finally {
      await pool.end();
    }
    await new Promise((r) => setTimeout(r, 100));
  }
  throw new Error("Disposable failover endpoint did not reach required state");
}
try {
  docker("network", "create", id);
  docker("volume", "create", volume);
  docker(
    "run",
    "-d",
    "--name",
    primary,
    "--label",
    "lossless-claw.experiment=postgres-failover",
    "--network",
    id,
    "-e",
    "POSTGRES_HOST_AUTH_METHOD=trust",
    "-p",
    "127.0.0.1::5432",
    "postgres:18",
    "-c",
    "wal_level=replica",
    "-c",
    "max_wal_senders=3"
  );
  const primaryOptions = poolOptions(port(primary));
  await waitReady(primaryOptions);
  db = await PostgresDatabase.open({ ...identity, pool: primaryOptions }).catch(
    async () => {
      await provisionPostgres({ ...identity, pool: primaryOptions });
      return PostgresDatabase.open({ ...identity, pool: primaryOptions });
    }
  );
  const conversations = new ConversationStore(db),
    summaries = new SummaryStore(db);
  await db.transaction(async () => {
    const conversation = await conversations.createConversation({
      sessionId: "failover-session",
      sessionKey: "agent:one:failover",
    });
    const message = await conversations.createMessage({
      conversationId: conversation.conversationId,
      seq: 1,
      role: "user",
      content: "SYNTHETIC-COMMITTED-BEFORE-CRASH",
      tokenCount: 4,
    });
    await summaries.appendContextMessages(conversation.conversationId, [
      message.messageId,
    ]);
    await db
      .prepare(
        "INSERT INTO turn_advancements(advancement_key,payload_hash,session_id,session_key,admission_entry_id,terminal_entry_id,message_count) VALUES(?,?,?,?,?,?,?)"
      )
      .run(
        "synthetic-failover-receipt",
        "synthetic-body-hash",
        "failover-session",
        "agent:one:failover",
        "entry1",
        "entry2",
        1
      );
  });
  const before = await exportNativePostgres(db),
    barrier = (
      await db.prepare("SELECT pg_current_wal_lsn()::text AS lsn").get()
    ).lsn;
  docker(
    "exec",
    primary,
    "bash",
    "-ec",
    "printf 'host replication postgres 0.0.0.0/0 trust\\n' >> /var/lib/postgresql/18/docker/pg_hba.conf"
  );
  docker(
    "exec",
    "--user",
    "postgres",
    primary,
    "pg_ctl",
    "reload",
    "-D",
    "/var/lib/postgresql/18/docker"
  );
  // All values are generated container identifiers, not credentials or user input.
  docker(
    "run",
    "--rm",
    "--name",
    seed,
    "--label",
    "lossless-claw.experiment=postgres-failover",
    "--network",
    id,
    "-v",
    `${volume}:/standby-data`,
    "--entrypoint",
    "bash",
    "postgres:18",
    "-ec",
    `chown postgres:postgres /standby-data; exec gosu postgres pg_basebackup -h ${primary} -U postgres -D /standby-data -R -X stream`
  );
  docker(
    "run",
    "-d",
    "--name",
    standby,
    "--label",
    "lossless-claw.experiment=postgres-failover",
    "--network",
    id,
    "-e",
    "PGDATA=/standby-data",
    "-v",
    `${volume}:/standby-data`,
    "-p",
    "127.0.0.1::5432",
    "postgres:18"
  );
  const standbyOptions = poolOptions(port(standby));
  await waitReady(standbyOptions, async (pool) =>
    Boolean(
      (
        await pool.query(
          "SELECT pg_is_in_recovery() AND pg_last_wal_replay_lsn()>=$1::pg_lsn AS ready",
          [barrier]
        )
      ).rows[0].ready
    )
  );
  let refused = false;
  try {
    replica = await PostgresDatabase.open({
      ...identity,
      pool: standbyOptions,
    });
  } catch {
    refused = true;
  }
  if (!refused) throw new Error("Read replica was admitted as primary");
  let entered, release;
  const parked = new Promise((r) => {
      entered = r;
    }),
    gate = new Promise((r) => {
      release = r;
    });
  const write = db.transaction(async () => {
    await conversations.createMessage({
      conversationId: 1,
      seq: 2,
      role: "assistant",
      content: "SYNTHETIC-UNACKNOWLEDGED-MUST-ROLL-BACK",
      tokenCount: 4,
    });
    entered();
    await gate;
    await db.prepare("SELECT 1").get();
  });
  const failed = write.then(
    () => false,
    () => true
  );
  await parked;
  docker("kill", primary);
  release();
  if (!(await failed))
    throw new Error("Crashed in-flight write was acknowledged");
  await db.close();
  db = undefined;
  docker(
    "exec",
    "--user",
    "postgres",
    standby,
    "pg_ctl",
    "promote",
    "-D",
    "/standby-data",
    "-w"
  );
  await waitReady(
    standbyOptions,
    async (pool) =>
      !(
        await pool.query("SELECT pg_is_in_recovery() AS replica")
      ).rows[0].replica
  );
  db = await PostgresDatabase.open({ ...identity, pool: standbyOptions });
  const after = await exportNativePostgres(db);
  const rowsHash = (value) => exportHash({ ...value, sequences: undefined });
  const allocatorMonotonic = ["conversations", "messages"].every(
    (name) => after.sequences[name] >= before.sequences[name]
  );
  if (rowsHash(before) !== rowsHash(after) || !allocatorMonotonic)
    throw new Error(
      "Committed graph/receipt state changed across caught-up local promotion"
    );
  const messages = await db
    .prepare("SELECT COUNT(*) AS count FROM messages")
    .get();
  const receipt = await db
    .prepare(
      "SELECT advancement_key,payload_hash FROM turn_advancements WHERE advancement_key=?"
    )
    .get("synthetic-failover-receipt");
  if (messages.count !== 1 || receipt.payload_hash !== "synthetic-body-hash")
    throw new Error("Failover durability/receipt mismatch");
  await new ConversationStore(db).createMessage({
    conversationId: 1,
    seq: 2,
    role: "assistant",
    content: "SYNTHETIC-AFTER-PROMOTION",
    tokenCount: 4,
  });
  const result = {
    production: false,
    environment:
      "local disposable PostgreSQL18 streaming standby, explicit replay barrier before primary SIGKILL, manual promotion, explicit new endpoint; not automatic/provider HA",
    replicaRejected: refused,
    inFlightWriteRejected: true,
    committedStateHash: rowsHash(after),
    allocatorMonotonic,
    allocatorBefore: before.sequences,
    allocatorAfter: after.sequences,
    committedMessages: messages.count,
    receiptPreserved: true,
    newPrimaryWritable: true,
    sourceSha256: createHash("sha256")
      .update(
        await import("node:fs/promises").then((fs) =>
          fs.readFile(fileURLToPath(import.meta.url))
        )
      )
      .digest("hex"),
  };
  await writeFile(
    path.join(out, "failover.json"),
    JSON.stringify(result, null, 2) + "\n"
  );
  console.log(JSON.stringify(result));
} finally {
  await replica?.close();
  await db?.close();
  for (const name of [seed, standby, primary])
    try {
      docker("rm", "-f", "-v", name);
    } catch {}
  try {
    docker("volume", "rm", volume);
  } catch {}
  try {
    docker("network", "rm", id);
  } catch {}
}
