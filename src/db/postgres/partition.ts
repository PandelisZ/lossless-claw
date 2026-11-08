import type { SQLInputValue } from "node:sqlite";
import { POSTGRES_TABLES } from "./schema.js";
import { exportHash, type SqliteExport } from "./transfer.js";

type Row = SqliteExport["tables"][number]["rows"][number];
const agentKey = /^agent:([a-zA-Z0-9][a-zA-Z0-9_-]{0,63}):/;
function fail(table: string): never {
  throw new Error(
    `Cannot establish isolated ownership for ${table}; source remains unchanged`
  );
}

/** Every row is assigned exactly once, except schema migration markers copied to every partition. */
export function partitionSqliteExport(
  data: SqliteExport
): Map<string, SqliteExport> {
  if (data.version !== 1) throw new Error("Unsupported export version");
  const tables = new Map(data.tables.map((t) => [t.name, t]));
  if (tables.size !== data.tables.length || !tables.has("conversations"))
    fail("table inventory");
  for (const table of data.tables) {
    if (
      !POSTGRES_TABLES.includes(
        table.name as (typeof POSTGRES_TABLES)[number]
      ) ||
      table.name.startsWith("lcm_import_") ||
      ["lcm_schema_version", "lcm_store_identity"].includes(table.name)
    )
      fail("table inventory");
    for (const row of table.rows)
      for (const value of Object.values(row))
        if (typeof value === "string" && value.includes("\0"))
          throw new Error(
            `PostgreSQL cannot represent NUL text in ${table.name}; source remains unchanged`
          );
  }
  const rows = (name: string) => tables.get(name)?.rows ?? [];
  const owners = new Map<Row, string>();
  const agents = new Set<string>();
  const sessions = new Map<string, Set<string>>();
  const index = (table: string, column: string) =>
    new Map(rows(table).map((r) => [r[column], r]));
  const conversations = index("conversations", "conversation_id");
  for (const row of rows("conversations")) {
    const owner =
      typeof row.session_key === "string"
        ? agentKey.exec(row.session_key)?.[1]
        : undefined;
    if (!owner) fail("conversations (missing canonical session key)");
    owners.set(row, owner);
    agents.add(owner);
    const session = String(row.session_id);
    const group = sessions.get(session) ?? new Set();
    group.add(owner);
    sessions.set(session, group);
  }
  const ownerOf = (table: string, row: Row | undefined): string =>
    (row && owners.get(row)) || fail(table);
  const assign = (
    table: string,
    parent: string,
    key: string,
    parentKey: string
  ) => {
    const lookup = index(parent, parentKey);
    for (const row of rows(table))
      owners.set(row, ownerOf(table, lookup.get(row[key])));
  };
  for (const table of data.tables)
    if (
      table.name !== "conversations" &&
      table.columns.includes("conversation_id")
    ) {
      for (const row of table.rows)
        owners.set(
          row,
          ownerOf(table.name, conversations.get(row.conversation_id))
        );
    }
  assign("message_parts", "messages", "message_id", "message_id");
  assign("focus_brief_sources", "focus_briefs", "brief_id", "brief_id");
  assign(
    "pending_summary_node_children",
    "pending_summary_nodes",
    "node_id",
    "node_id"
  );
  assign(
    "pending_summary_node_messages",
    "pending_summary_nodes",
    "node_id",
    "node_id"
  );
  assign("summary_messages", "summaries", "summary_id", "summary_id");
  assign("summary_parents", "summaries", "summary_id", "summary_id");
  assign("messages_fts", "messages", "rowid", "message_id");
  assign("summaries_fts", "summaries", "summary_id", "summary_id");
  assign("summaries_fts_cjk", "summaries", "summary_id", "summary_id");
  for (const row of rows("turn_advancements")) {
    const owner =
      typeof row.session_key === "string"
        ? agentKey.exec(row.session_key)?.[1]
        : undefined;
    let resolved = owner;
    if (!resolved) {
      const session = sessions.get(String(row.session_id));
      if (!session || session.size !== 1) fail("turn_advancements");
      resolved = [...session][0];
    }
    agents.add(resolved);
    owners.set(row, resolved);
  }
  const list = (
    value: SQLInputValue | undefined,
    table: string
  ): Array<string | number> => {
    let parsed: unknown;
    try {
      parsed = JSON.parse(String(value));
    } catch {
      fail(table);
    }
    if (
      !Array.isArray(parsed) ||
      parsed.some((x) => !(typeof x === "string" || typeof x === "number"))
    )
      fail(table);
    return parsed;
  };
  const messages = index("messages", "message_id"),
    summaries = index("summaries", "summary_id"),
    files = index("large_files", "file_id");
  for (const row of rows("lcm_expansion_grants")) {
    const candidates = new Set<string>();
    for (const id of list(
      row.allowed_conversation_ids,
      "lcm_expansion_grants"
    )) {
      const target = conversations.get(id);
      if (target) candidates.add(ownerOf("lcm_expansion_grants", target));
    }
    for (const id of list(row.allowed_summary_ids, "lcm_expansion_grants")) {
      const target = summaries.get(id);
      if (target) candidates.add(ownerOf("lcm_expansion_grants", target));
    }
    const canonicalIssuer =
      typeof row.issuer_session_id === "string"
        ? agentKey.exec(row.issuer_session_id)?.[1]
        : undefined;
    if (canonicalIssuer) {
      agents.add(canonicalIssuer);
      candidates.add(canonicalIssuer);
    } else {
      const issuer = sessions.get(String(row.issuer_session_id));
      if (!issuer || issuer.size !== 1)
        fail("lcm_expansion_grants (ambiguous or missing issuer session)");
      candidates.add([...issuer][0]);
    }
    if (candidates.size !== 1)
      fail("lcm_expansion_grants (ambiguous or cross-agent grant)");
    owners.set(row, [...candidates][0]);
  }
  const grants = index("lcm_expansion_grants", "grant_id");
  for (const binding of rows("lcm_expansion_grant_bindings")) {
    const owner =
      typeof binding.session_key === "string"
        ? agentKey.exec(binding.session_key)?.[1]
        : undefined;
    if (!owner) fail("lcm_expansion_grant_bindings authority");
    const grant = grants.get(binding.grant_id);
    if (grant && ownerOf("lcm_expansion_grant_bindings", grant) !== owner)
      fail("lcm_expansion_grant_bindings cross-agent grant");
    agents.add(owner);
    owners.set(binding, owner);
  }
  const refs: Record<string, Array<[string, string, string]>> = {
    context_items: [
      ["messages", "message_id", "message_id"],
      ["summaries", "summary_id", "summary_id"],
    ],
    message_transcript_anchor_trust: [["messages", "message_id", "message_id"]],
    conversation_transcript_gaps: [
      ["messages", "prev_message_id", "message_id"],
      ["messages", "next_message_id", "message_id"],
    ],
    focus_brief_sources: [["summaries", "summary_id", "summary_id"]],
    pending_summary_nodes: [
      ["pending_compaction_batches", "batch_id", "batch_id"],
      ["summaries", "canonical_summary_id", "summary_id"],
    ],
    pending_summary_node_children: [
      ["pending_summary_nodes", "child_node_id", "node_id"],
      ["summaries", "child_summary_id", "summary_id"],
    ],
    pending_summary_node_messages: [["messages", "message_id", "message_id"]],
    summary_messages: [["messages", "message_id", "message_id"]],
    summary_parents: [["summaries", "parent_summary_id", "summary_id"]],
  };
  for (const table of data.tables)
    for (const row of table.rows) {
      if (table.name === "lcm_migration_state") continue;
      const owner = ownerOf(table.name, row);
      for (const [target, column, key] of refs[table.name] ?? []) {
        if (
          table.name === "focus_brief_sources" &&
          column === "summary_id" &&
          !index(target, key).has(row[column])
        )
          continue;
        if (
          row[column] !== null &&
          row[column] !== undefined &&
          ownerOf(table.name, index(target, key).get(row[column])) !== owner
        )
          fail(table.name + " cross-agent edge");
      }
      for (const column of ["session_key", "generator_session_key"]) {
        const key = row[column];
        if (
          key !== null &&
          key !== undefined &&
          (typeof key !== "string" || agentKey.exec(key)?.[1] !== owner)
        )
          fail(table.name + " session authority");
      }
      if (table.name === "pending_compaction_batches") {
        let target: Record<string, unknown>;
        try {
          target = JSON.parse(String(row.session_target_json));
        } catch {
          fail(table.name);
        }
        if (
          !target ||
          typeof target !== "object" ||
          Array.isArray(target) ||
          (target.agentId !== undefined && target.agentId !== owner) ||
          (target.sessionKey !== undefined &&
            (typeof target.sessionKey !== "string" ||
              agentKey.exec(target.sessionKey)?.[1] !== owner))
        )
          fail(table.name + " target authority");
      }
      const fileIds =
        table.name === "summaries"
          ? list(row.file_ids, table.name)
          : table.name === "messages" && row.large_content
          ? [row.large_content]
          : [];
      for (const id of fileIds)
        if (ownerOf(table.name, files.get(id)) !== owner)
          fail(table.name + " file authority");
    }
  const partitions = new Map<string, SqliteExport>();
  for (const agent of [...agents].sort())
    partitions.set(agent, {
      version: 1,
      ...(data.sequences ? { sequences: { ...data.sequences } } : {}),
      tables: data.tables.map((table) => ({
        ...table,
        columns: [...table.columns],
        rows: table.rows
          .filter(
            (row) =>
              table.name === "lcm_migration_state" || owners.get(row) === agent
          )
          .map((row) => ({ ...row })),
      })),
    });
  // This assertion catches an unhandled future live table rather than silently dropping its rows.
  const sourceRows = data.tables
    .filter((t) => t.name !== "lcm_migration_state")
    .reduce((n, t) => n + t.rows.length, 0);
  const assigned = [...partitions.values()].reduce(
    (n, p) =>
      n +
      p.tables
        .filter((t) => t.name !== "lcm_migration_state")
        .reduce((m, t) => m + t.rows.length, 0),
    0
  );
  if (sourceRows !== assigned) fail("row accounting");
  return partitions;
}
export function partitionManifest(
  source: SqliteExport,
  parts: Map<string, SqliteExport>
) {
  return {
    sourceHash: exportHash(source),
    agents: [...parts].map(([agentId, data]) => ({
      agentId,
      sourceHash: exportHash(data),
      rows: data.tables.reduce((n, t) => n + t.rows.length, 0),
    })),
  };
}
