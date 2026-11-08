import { openSync, closeSync } from "node:fs";
import { DatabaseSync } from "node:sqlite";
import { createHash } from "node:crypto";
import { readFile } from "node:fs/promises";
import { table } from "./database.mjs";
import {
  normalizeMessageContentForFullTextIndex,
  createLcmDatabaseConnection,
  runLcmMigrations,
} from "./.generated/upstream.mjs";
export const core = [
  "conversations",
  "messages",
  "summaries",
  "summary_messages",
  "summary_parents",
  "context_items",
  "large_files",
];
export const hash = (value) => createHash("sha256").update(value).digest("hex");
export async function readFixture(manifestPath) {
  const manifest = JSON.parse(await readFile(manifestPath));
  const data = await readFile(manifest.fixturePath);
  if (hash(data) !== manifest.sha256) throw new Error("Fixture hash mismatch");
  return { fixture: JSON.parse(data), manifest };
}
const columns = {
  conversations: ["conversation_id"],
  messages: ["message_id", "seq", "content", "indexed_content"],
  summaries: ["summary_id", "depth", "content"],
  summary_messages: ["summary_id", "message_id", "ordinal"],
  summary_parents: ["summary_id", "parent_summary_id", "ordinal"],
  context_items: ["ordinal", "message_id", "summary_id"],
  large_files: ["file_id"],
};
// Input is operator-owned synthetic data; identifiers come only from this allowlist.
export async function insertRows(db, name, organizationId, agentId, cid, rows) {
  if (!rows.length) return;
  const cols = columns[name];
  if (!cols) throw new Error("Unsupported core table");
  const records = rows.map((data) => ({
    ...data,
    indexed_content:
      name === "messages"
        ? (normalizeMessageContentForFullTextIndex(data.content) ?? "")
        : undefined,
    data,
  }));
  const types = cols.map(
    (col) =>
      `${col} ${/(_id$)/.test(col) && !["summary_id", "parent_summary_id", "file_id"].includes(col) ? "bigint" : ["seq", "depth", "ordinal"].includes(col) ? "integer" : "text"}`,
  );
  const extraCols = cols.filter((c) => c !== "conversation_id");
  await db.query(
    `INSERT INTO ${table(name)}(organization_id,agent_id,conversation_id,${extraCols.length ? extraCols.join(",") + "," : ""}data)
    SELECT $1,$2,$3,${extraCols.length ? extraCols.map((c) => `r.${c}`).join(",") + "," : ""}r.data
    FROM jsonb_to_recordset($4::jsonb) AS r(${types.join(",")}, data jsonb)`,
    [organizationId, agentId, cid, JSON.stringify(records)],
  );
}
function rowKey(org, name, row, index) {
  const keys = org.schema[name]
    .filter((c) => c.pk)
    .sort((a, b) => a.pk - b.pk)
    .map((c) => c.name);
  return JSON.stringify(keys.length ? keys.map((key) => row[key]) : [index]);
}
export async function importFixture(
  db,
  fixture,
  fixtureHash,
  { afterConversation } = {},
) {
  const metadata = {
    ...fixture,
    organizations: fixture.organizations.map(({ tables, ...rest }) => ({
      ...rest,
      tableNames: Object.keys(tables),
    })),
  };
  await db.query(
    `INSERT INTO ${table("import_metadata")} VALUES($1,$2) ON CONFLICT DO NOTHING`,
    [fixtureHash, metadata],
  );
  for (const org of fixture.organizations) {
    const ownership = new Map();
    for (const name of core)
      for (const row of org.tables[name]) {
        if (row.conversation_id != null) {
          if (row.message_id != null && name === "messages")
            ownership.set(`message:${row.message_id}`, row.conversation_id);
          if (row.summary_id != null && name === "summaries")
            ownership.set(`summary:${row.summary_id}`, row.conversation_id);
        }
      }
    for (const name of [
      "pending_compaction_batches",
      "pending_summary_nodes",
      "focus_briefs",
    ])
      for (const row of org.tables[name] ?? []) {
        if (row.conversation_id != null)
          for (const key of ["batch_id", "node_id", "brief_id"])
            if (row[key] != null)
              ownership.set(`${key}:${row[key]}`, row.conversation_id);
      }
    const cidFor = (row) =>
      row.conversation_id ??
      ownership.get(`node_id:${row.node_id}`) ??
      ownership.get(`brief_id:${row.brief_id}`) ??
      ownership.get(`message:${row.message_id}`) ??
      ownership.get(`summary:${row.summary_id}`);
    for (const conversation of org.tables.conversations) {
      const cid = conversation.conversation_id;
      const agent = org.authority[cid];
      if (!agent || !conversation.session_key?.startsWith(`agent:${agent}:`))
        throw new Error("Invalid import authority");
      await db.transaction(async () => {
        const claimed = await db.query(
          `INSERT INTO ${table("import_chunks")} VALUES($1,$2,$3) ON CONFLICT DO NOTHING RETURNING *`,
          [fixtureHash, org.organizationId, cid],
        );
        if (!claimed.rowCount) return;
        const summaries = org.tables.summaries.filter(
          (row) => row.conversation_id === cid,
        );
        const depth = new Map(
          summaries.map((row) => [row.summary_id, row.depth]),
        );
        for (const edge of org.tables.summary_parents.filter(
          (row) => cidFor(row) === cid,
        ))
          if (
            !depth.has(edge.parent_summary_id) ||
            !(depth.get(edge.summary_id) > depth.get(edge.parent_summary_id))
          )
            throw new Error(
              "Import summary graph is cyclic, cross-conversation or has invalid depth",
            );
        const ordinals = org.tables.context_items
          .filter((row) => row.conversation_id === cid)
          .map((row) => row.ordinal)
          .sort((a, b) => a - b);
        if (ordinals.some((ordinal, i) => ordinal !== i))
          throw new Error("Import context is not contiguous");
        for (const name of core) {
          const rows = org.tables[name].filter((row) => cidFor(row) === cid);
          await insertRows(db, name, org.organizationId, agent, cid, rows);
        }
        await db.query(
          `UPDATE ${table("conversations")} SET next_seq=$4 WHERE organization_id=$1 AND agent_id=$2 AND conversation_id=$3`,
          [
            org.organizationId,
            agent,
            cid,
            Math.max(
              -1,
              ...org.tables.messages
                .filter((m) => m.conversation_id === cid)
                .map((m) => m.seq),
            ) + 1,
          ],
        );
        for (const [name, rows] of Object.entries(org.tables)) {
          if (core.includes(name)) continue;
          for (const [index, row] of rows.entries())
            if (cidFor(row) === cid) {
              await db.query(
                `INSERT INTO ${table("auxiliary_rows")} VALUES($1,$2,$3,$4,$5,$6)`,
                [
                  org.organizationId,
                  name,
                  rowKey(org, name, row, index),
                  agent,
                  cid,
                  row,
                ],
              );
            }
        }
      });
      await afterConversation?.(org.organizationId, cid);
    }
    await db.transaction(async () => {
      for (const [name, rows] of Object.entries(org.tables)) {
        if (core.includes(name)) continue;
        for (const [index, row] of rows.entries())
          if (cidFor(row) == null) {
            await db.query(
              `INSERT INTO ${table("auxiliary_rows")} VALUES($1,$2,$3,NULL,NULL,$4) ON CONFLICT DO NOTHING`,
              [org.organizationId, name, rowKey(org, name, row, index), row],
            );
          }
      }
    });
  }
}
export async function exportFixture(db, fixtureHash) {
  // All tables share a primary snapshot; replica routing is deliberately absent.
  return db.transaction(async () => {
    await db.query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY");
    const metadata = (
      await db.query(
        `SELECT metadata FROM ${table("import_metadata")} WHERE fixture_hash=$1`,
        [fixtureHash],
      )
    ).rows[0]?.metadata;
    if (!metadata) throw new Error("Missing import metadata");
    const fixture = { ...metadata, organizations: [] };
    for (const org of metadata.organizations) {
      const { tableNames, ...rest } = org;
      const tables = Object.fromEntries(tableNames.map((name) => [name, []]));
      for (const name of core)
        tables[name] = (
          await db.query(
            `SELECT data FROM ${table(name)} WHERE organization_id=$1`,
            [org.organizationId],
          )
        ).rows.map((r) => r.data);
      for (const row of (
        await db.query(
          `SELECT table_name,data FROM ${table("auxiliary_rows")} WHERE organization_id=$1`,
          [org.organizationId],
        )
      ).rows)
        tables[row.table_name].push(row.data);
      const authority = Object.fromEntries(
        tables.conversations.map((c) => [
          c.conversation_id,
          /^agent:([^:]+):/.exec(c.session_key ?? "")?.[1],
        ]),
      );
      fixture.organizations.push({ ...rest, authority, tables });
    }
    return fixture;
  });
}
const stable = (v) =>
  Array.isArray(v)
    ? v.map(stable)
    : v && typeof v === "object"
      ? Object.fromEntries(
          Object.keys(v)
            .sort()
            .map((k) => [k, stable(v[k])]),
        )
      : v;
export function contentHash(fixture) {
  const organizations = fixture.organizations.map((org) => ({
    organizationId: org.organizationId,
    authority: org.authority,
    tables: Object.fromEntries(
      Object.entries(org.tables).map(([name, rows]) => [
        name,
        rows.map((r) => JSON.stringify(stable(r))).sort(),
      ]),
    ),
  }));
  return hash(JSON.stringify(stable(organizations)));
}
export function restoreSqlite(org, sqlitePath) {
  // Fresh synthetic rollback target only. Caller must choose a new path.
  if (sqlitePath !== ":memory:") closeSync(openSync(sqlitePath, "wx", 0o600));
  const db = createLcmDatabaseConnection(sqlitePath);
  runLcmMigrations(db, { fts5Available: true });
  db.exec("PRAGMA foreign_keys=OFF; BEGIN");
  try {
    for (const [name, rows] of Object.entries(org.tables)) {
      if (!/^[a-z_]+$/.test(name) || !org.schema[name])
        throw new Error("Invalid export table");
      db.exec(`DELETE FROM "${name}"`);
      for (const row of rows) {
        const cols = org.schema[name].map((c) => c.name);
        db.prepare(
          `INSERT INTO "${name}"(${cols.map((c) => `"${c}"`).join(",")}) VALUES(${cols.map(() => "?").join(",")})`,
        ).run(...cols.map((c) => row[c] ?? null));
      }
    }
    db.exec("COMMIT; PRAGMA foreign_keys=ON");
    if (db.prepare("PRAGMA foreign_key_check").all().length)
      throw new Error("SQLite rollback foreign-key parity failed");
    // FTS is derived state. Rebuild through pinned migrations, not the import rows.
    db.exec(
      "DROP TABLE IF EXISTS messages_fts; DROP TABLE IF EXISTS summaries_fts",
    );
    runLcmMigrations(db, { fts5Available: true });
    return db;
  } catch (error) {
    try {
      db.exec("ROLLBACK");
    } catch {}
    db.close();
    throw error;
  }
}

export function exportSqlite(sqlitePath, organizationId) {
  const db = new DatabaseSync(sqlitePath, { readOnly: true });
  try {
    db.exec("BEGIN");
    const schema = {},
      tables = {};
    const names = db
      .prepare(
        "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' AND name NOT LIKE '%fts%' ORDER BY name",
      )
      .all()
      .map((r) => r.name);
    for (const name of names) {
      if (!/^[a-z_]+$/.test(name)) throw new Error("Unexpected LCM table");
      schema[name] = db.prepare(`PRAGMA table_info("${name}")`).all();
      tables[name] = db.prepare(`SELECT * FROM "${name}" ORDER BY rowid`).all();
    }
    const authority = Object.fromEntries(
      tables.conversations.map((c) => {
        const agent = /^agent:([^:]+):/.exec(c.session_key ?? "")?.[1];
        if (!agent)
          throw new Error(
            "Conversation has no native agent authority; operator mapping required",
          );
        return [c.conversation_id, agent];
      }),
    );
    db.exec("COMMIT");
    return { organizationId, authority, schema, tables };
  } finally {
    db.close();
  }
}
