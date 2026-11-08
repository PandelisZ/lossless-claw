import { normalizeMessageContentForFullTextIndex } from "../../store/conversation-store.js";
import { inspectRuntimePrivileges } from "./privileges.js";
import type { PostgresDatabase } from "./database.js";
/** Read-only consistency/operational report. It never repairs, truncates, or reveals message text/credentials. */
export async function inspectPostgres(db: PostgresDatabase) {
  return db.transaction(async () => {
    await db.exec("SET TRANSACTION READ ONLY");
    const primary = await db
      .prepare(
        "SELECT pg_is_in_recovery() AS replica,current_setting('transaction_read_only') AS read_only"
      )
      .get();
    const identity = await db
      .prepare("SELECT organization_id,agent_id FROM lcm_store_identity")
      .all();
    const version = await db
      .prepare("SELECT version FROM lcm_schema_version")
      .all();
    const imported = await db
      .prepare("SELECT source_hash,complete FROM lcm_import_state WHERE id=1")
      .get();
    const invalidIndexes = await db
      .prepare(
        "SELECT c.relname AS name FROM pg_index i JOIN pg_class c ON c.oid=i.indexrelid JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname=? AND (NOT i.indisvalid OR NOT i.indisready)"
      )
      .all(db.schema);
    const invalidConstraints = await db
      .prepare(
        "SELECT c.conname AS name FROM pg_constraint c JOIN pg_namespace n ON n.oid=c.connamespace WHERE n.nspname=? AND NOT c.convalidated"
      )
      .all(db.schema);
    const counts = await db
      .prepare(
        "SELECT (SELECT COUNT(*) FROM conversations) AS conversations,(SELECT COUNT(*) FROM messages) AS messages,(SELECT COUNT(*) FROM summaries) AS summaries,(SELECT COUNT(*) FROM pending_compaction_batches WHERE status IN ('planning','ready','publishing')) AS pending_batches,(SELECT COUNT(*) FROM pending_summary_nodes WHERE status='running') AS running_nodes,(SELECT COUNT(*) FROM focus_briefs WHERE status='active') AS active_focus"
      )
      .get();
    const search = {
      message_mismatches: 0,
      summary_mismatches: Number(
        (
          await db
            .prepare(
              "SELECT COUNT(*) AS count FROM summaries s LEFT JOIN summaries_fts f ON f.summary_id=s.summary_id WHERE f.summary_id IS NULL OR f.content IS DISTINCT FROM s.content"
            )
            .get()
        )?.count ?? 0
      ),
    };
    let cursor: number | null = null;
    for (;;) {
      const page = await db
        .prepare(
          "SELECT m.message_id,m.content,f.content AS indexed_content FROM messages m LEFT JOIN messages_fts f ON f.rowid=m.message_id WHERE (?::bigint IS NULL OR m.message_id>?) ORDER BY m.message_id LIMIT 500"
        )
        .all(cursor, cursor);
      if (!page.length) break;
      for (const row of page) {
        if (
          normalizeMessageContentForFullTextIndex(String(row.content)) !==
          row.indexed_content
        )
          search.message_mismatches++;
        cursor = Number(row.message_id);
      }
    }
    const authority = await db
      .prepare(
        "SELECT COUNT(*) AS count FROM conversations WHERE session_key IS NULL OR left(session_key,?)<>?"
      )
      .get(`agent:${db.agentId}:`.length, `agent:${db.agentId}:`);
    const privileges = await inspectRuntimePrivileges(db);
    const warnings: string[] = [];
    if (
      identity.length !== 1 ||
      identity[0].organization_id !== db.organizationId ||
      identity[0].agent_id !== db.agentId
    )
      warnings.push("schema identity mismatch");
    if (version.length !== 1 || version[0].version !== 1)
      warnings.push("schema version mismatch");
    if (imported?.complete !== 1)
      warnings.push("import is incomplete; gateway must remain disabled");
    if (invalidIndexes.length || invalidConstraints.length)
      warnings.push("invalid indexes or unvalidated constraints");
    if (search?.message_mismatches !== 0 || search?.summary_mismatches !== 0)
      warnings.push("full-text content parity mismatch");
    if (authority?.count !== 0)
      warnings.push("conversation authority mismatch");
    if (!privileges.safe)
      warnings.push(
        "connection has operator or unrelated data privileges; do not use it as a gateway credential"
      );
    return {
      ok: warnings.length === 0,
      schema: db.schema,
      organizationId: db.organizationId,
      agentId: db.agentId,
      primary: primary?.replica === 0,
      readOnlyInspection: true,
      imported,
      counts,
      search,
      invalidIndexes,
      invalidConstraints,
      privileges,
      warnings,
    };
  });
}
