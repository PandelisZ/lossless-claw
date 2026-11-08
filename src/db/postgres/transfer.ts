import { createHash } from "node:crypto";
import { existsSync, openSync, closeSync, linkSync, unlinkSync } from "node:fs";
import { DatabaseSync, type SQLInputValue } from "node:sqlite";
import { ensureExpansionGrantTables } from "../../expansion-auth.js";
import { runLcmMigrations } from "../migration.js";
import { POSTGRES_TABLES } from "./schema.js";
import type { PostgresDatabase } from "./database.js";

type Row = Record<string, SQLInputValue>;
export type SqliteExport = {
  version: 1;
  tables: { name: string; columns: string[]; rows: Row[] }[];
};
const internal = new Set([
  "lcm_schema_version",
  "lcm_store_identity",
  "lcm_import_state",
  "lcm_import_chunks",
]);
const fullText = new Set([
  "messages_fts",
  "summaries_fts",
  "summaries_fts_cjk",
]);
const hash = (value: unknown) =>
  createHash("sha256").update(JSON.stringify(value)).digest("hex");
const quoted = (name: string) => {
  if (!/^[a-z_][a-z0-9_]*$/.test(name))
    throw new Error("Invalid LCM transfer identifier");
  return `"${name}"`;
};
const sortRows = (rows: Row[]) =>
  rows.sort((a, b) => JSON.stringify(a).localeCompare(JSON.stringify(b), "en"));
function sqliteLayout(db: DatabaseSync) {
  const present = new Set(
    (
      db.prepare("SELECT name FROM sqlite_master WHERE type='table'").all() as {
        name: string;
      }[]
    ).map((r) => r.name)
  );
  return POSTGRES_TABLES.filter(
    (name) => !internal.has(name) && present.has(name)
  ).map((name) => ({
    name,
    columns:
      name === "messages_fts"
        ? ["rowid", "content"]
        : [
            ...(name === "focus_briefs" ? ["rowid"] : []),
            ...(
              db.prepare(`PRAGMA table_info(${quoted(name)})`).all() as {
                name: string;
              }[]
            ).map((r) => r.name),
          ],
  }));
}
/** Export an isolated synthetic/maintenance copy. Source is never mutated or migrated. */
export function exportSqlite(db: DatabaseSync): SqliteExport {
  db.exec("BEGIN");
  try {
    const data: SqliteExport = {
      version: 1,
      tables: sqliteLayout(db).map(({ name, columns }) => ({
        name,
        columns,
        rows: sortRows(
          db
            .prepare(`SELECT ${columns.map(quoted)} FROM ${quoted(name)}`)
            .all() as Row[]
        ),
      })),
    };
    db.exec("COMMIT");
    return data;
  } catch (error) {
    db.exec("ROLLBACK");
    throw error;
  }
}
export function exportHash(data: SqliteExport): string {
  return hash(data);
}

/** Restartable exact-row import. Stop gateway writers and supply a one-agent SQLite copy. */
export async function importSqliteExport(
  db: PostgresDatabase,
  data: SqliteExport,
  options: { chunkRows?: number; afterChunk?: () => void | Promise<void> } = {}
) {
  if (data.version !== 1) throw new Error("Unsupported LCM export version");
  const chunkRows = options.chunkRows ?? 100;
  if (!Number.isSafeInteger(chunkRows) || chunkRows < 1 || chunkRows > 500)
    throw new Error("Import chunks must contain 1..500 rows");
  const conversations = data.tables.find((t) => t.name === "conversations");
  if (
    !conversations ||
    conversations.rows.some(
      (row) =>
        typeof row.session_key !== "string" ||
        !row.session_key.startsWith(`agent:${db.agentId}:`)
    )
  )
    throw new Error(
      "Import requires an isolated SQLite copy of this agent only"
    );
  const sourceHash = hash(data);
  await db.transaction(async () => {
    const state = await db
      .prepare("SELECT source_hash,complete FROM lcm_import_state WHERE id=1")
      .get();
    if (state?.source_hash && state.source_hash !== sourceHash)
      throw new Error(
        "Different source already imported; use a fresh provisioned schema"
      );
    if (!state?.source_hash) {
      const emptyChecks = POSTGRES_TABLES.filter(
        (name) => !internal.has(name) && name !== "lcm_migration_state"
      )
        .map((name) => `(SELECT 1 FROM ${quoted(name)} LIMIT 1)`)
        .join(" UNION ALL ");
      const count = await db
        .prepare(`SELECT COUNT(*) AS count FROM (${emptyChecks}) nonempty`)
        .get();
      if (count?.count !== 0) throw new Error("Import target is not empty");
      const claim = await db
        .prepare(
          "UPDATE lcm_import_state SET source_hash=?,complete=0 WHERE id=1 AND source_hash IS NULL"
        )
        .run(sourceHash);
      if (Number(claim.changes) !== 1)
        throw new Error(
          "LCM import source claim conflict; retry the original source"
        );
    }
  });
  for (const table of data.tables) {
    if (
      !POSTGRES_TABLES.includes(
        table.name as (typeof POSTGRES_TABLES)[number]
      ) ||
      internal.has(table.name)
    )
      throw new Error("Unsupported LCM import table");
    const columns = table.columns.map(quoted).join(",");
    const insert = db.prepare(
      `INSERT INTO ${quoted(table.name)} (${columns}) VALUES (${table.columns
        .map(() => "?")
        .join(",")})${
        table.name === "lcm_migration_state"
          ? " ON CONFLICT(step_name,algorithm_version) DO UPDATE SET completed_at=excluded.completed_at"
          : ""
      }`
    );
    for (let offset = 0; offset < table.rows.length; offset += chunkRows) {
      const chunkIndex = offset / chunkRows,
        rows = table.rows.slice(offset, offset + chunkRows),
        payloadHash = hash(rows);
      await db.transaction(async () => {
        const done = await db
          .prepare(
            "SELECT payload_hash FROM lcm_import_chunks WHERE source_hash=? AND table_name=? AND chunk_index=?"
          )
          .get(sourceHash, table.name, chunkIndex);
        if (done) {
          if (done.payload_hash !== payloadHash)
            throw new Error("LCM import chunk hash mismatch");
          return;
        }
        for (const row of rows)
          await insert.run(...table.columns.map((column) => row[column]));
        await db
          .prepare(
            "INSERT INTO lcm_import_chunks(source_hash,table_name,chunk_index,payload_hash) VALUES(?,?,?,?)"
          )
          .run(sourceHash, table.name, chunkIndex, payloadHash);
      });
      await options.afterChunk?.();
    }
  }
  // Explicit insertion preserves IDs, so advance identity sequences before any new turn.
  for (const [table, column] of [
    ["conversations", "conversation_id"],
    ["messages", "message_id"],
    ["conversation_transcript_gaps", "gap_id"],
    ["focus_briefs", "rowid"],
  ]) {
    await db
      .prepare(
        `SELECT setval(pg_get_serial_sequence('"${db.schema}".${table}', '${column}'), GREATEST(COALESCE(MAX(${column}),0),1), COUNT(*)>0) FROM ${table}`
      )
      .get();
  }
  const verify = await exportPostgres(
    db,
    data.tables.map(({ name, columns }) => ({ name, columns }))
  );
  if (hash(verify) !== sourceHash)
    throw new Error(
      "LCM imported graph/content parity failed; gateway remains disabled"
    );
  await db
    .prepare(
      "UPDATE lcm_import_state SET complete=1 WHERE id=1 AND source_hash=?"
    )
    .run(sourceHash);
  return {
    sourceHash,
    rows: data.tables.reduce((n, t) => n + t.rows.length, 0),
  };
}

export async function exportPostgres(
  db: PostgresDatabase,
  layout: { name: string; columns: string[] }[]
): Promise<SqliteExport> {
  return db.transaction(async () => {
    const tables: SqliteExport["tables"] = [];
    for (const { name, columns } of layout) {
      if (
        !POSTGRES_TABLES.includes(name as (typeof POSTGRES_TABLES)[number]) ||
        internal.has(name)
      )
        throw new Error("Unsupported LCM export table");
      const rows = (await db
        .prepare(`SELECT ${columns.map(quoted)} FROM ${quoted(name)}`)
        .all()) as Row[];
      tables.push({ name, columns, rows: sortRows(rows) });
    }
    return { version: 1, tables };
  });
}
/** Roll back into a new SQLite file. Never overwrite an existing store. Payload files stay separate. */
export async function exportPostgresToSqlite(
  db: PostgresDatabase,
  destination: string
) {
  const partial = destination + ".partial";
  if (existsSync(destination) || existsSync(partial))
    throw new Error("SQLite export destination already exists");
  closeSync(openSync(partial, "wx", 0o600));
  const sqlite = new DatabaseSync(partial);
  let closed = false;
  try {
    runLcmMigrations(sqlite);
    ensureExpansionGrantTables(sqlite);
    const layout = sqliteLayout(sqlite);
    const data = await exportPostgres(db, layout);
    sqlite.exec("BEGIN");
    try {
      for (const table of data.tables) {
        if (fullText.has(table.name)) continue;
        const insert = sqlite.prepare(
          `INSERT INTO ${quoted(table.name)} (${table.columns.map(
            quoted
          )}) VALUES (${table.columns.map(() => "?")})${
            table.name === "lcm_migration_state"
              ? " ON CONFLICT(step_name,algorithm_version) DO UPDATE SET " +
                table.columns
                  .filter((c) => c !== "step_name" && c !== "algorithm_version")
                  .map((c) => `${quoted(c)}=excluded.${quoted(c)}`)
                  .join(",")
              : ""
          }`
        );
        for (const row of table.rows)
          insert.run(...table.columns.map((column) => row[column]));
      }
      for (const table of data.tables.filter((t) => fullText.has(t.name))) {
        const insert = sqlite.prepare(
          `INSERT INTO ${quoted(table.name)} (${table.columns.map(
            quoted
          )}) VALUES (${table.columns.map(() => "?")})${
            table.name === "lcm_migration_state"
              ? " ON CONFLICT(step_name,algorithm_version) DO UPDATE SET " +
                table.columns
                  .filter((c) => c !== "step_name" && c !== "algorithm_version")
                  .map((c) => `${quoted(c)}=excluded.${quoted(c)}`)
                  .join(",")
              : ""
          }`
        );
        for (const row of table.rows)
          insert.run(...table.columns.map((column) => row[column]));
      }
      if (sqlite.prepare("PRAGMA foreign_key_check").all().length)
        throw new Error("SQLite export foreign key check failed");
      sqlite.exec("COMMIT");
    } catch (error) {
      sqlite.exec("ROLLBACK");
      throw error;
    }
    if (hash(exportSqlite(sqlite)) !== hash(data))
      throw new Error("SQLite rollback export parity failed");
    sqlite.close();
    closed = true;
    linkSync(partial, destination);
    try {
      unlinkSync(partial);
    } catch {
      /* A complete published export remains valid; retain the extra link on cleanup failure. */
    }
    return {
      sourceHash: hash(data),
      rows: data.tables.reduce((n, t) => n + t.rows.length, 0),
    };
  } catch (error) {
    if (!closed)
      try {
        sqlite.close();
      } catch {}
    throw error;
  }
}
