import { randomUUID, createHash } from "node:crypto";
import { table, Conflict } from "./database.mjs";
import { insertRows } from "./transfer.mjs";
import {
  parseUtcTimestamp,
  serializeOpenClawSenderMetadata,
  parseOpenClawSenderMetadata,
  buildMessageIdentityHash,
  buildLikeSearchPlan,
  containsCjk,
  createFallbackSnippet,
  compileSafeSearchRegex,
} from "./.generated/upstream.mjs";
const now = () => new Date().toISOString();
const fingerprint = (v) =>
  createHash("sha256").update(JSON.stringify(v)).digest("hex");
const camel = (s) => s.replace(/_([a-z])/g, (_, c) => c.toUpperCase());
export function record(row) {
  if (!row) return null;
  return Object.fromEntries(
    Object.entries(row)
      .filter(
        ([key]) =>
          !["identity_hash", "stable_event_key", "archive_cause"].includes(key),
      )
      .map(([key, v]) => [
        key === "openclaw_sender_metadata"
          ? "openClawSenderMetadata"
          : camel(key),
        /_at$/.test(key) && v != null
          ? parseUtcTimestamp(v)
          : key === "openclaw_sender_metadata"
            ? parseOpenClawSenderMetadata(v)
            : key === "file_ids" && typeof v === "string"
              ? JSON.parse(v)
              : key === "active"
                ? v === 1
                : v,
      ]),
  );
}
const snake = (s) =>
  s
    .replace(/[A-Z]/g, (c) => "_" + c.toLowerCase())
    .replace("open_claw", "openclaw");
function raw(input) {
  return Object.fromEntries(
    Object.entries(input)
      .filter(([, v]) => v !== undefined)
      .map(([k, v]) => [snake(k), v instanceof Date ? v.toISOString() : v]),
  );
}
function integer(value, min = 0, max = 10000) {
  if (!Number.isSafeInteger(value) || value < min || value > max)
    throw new Error("Invalid bounded integer");
  return value;
}
// Authority comes from the gateway/operator, never a tool's organization/agent args.
// No unscoped variant of a read method is provided.
export class PostgresStore {
  constructor(database, authority) {
    if (!authority?.organizationId || !authority?.agentId)
      throw new Error("Native organization and agent authority required");
    this.database = database;
    this.authority = Object.freeze({
      organizationId: authority.organizationId,
      agentId: authority.agentId,
    });
  }
  get scope() {
    return [this.authority.organizationId, this.authority.agentId];
  }
  async rows(name, condition = "TRUE", args = [], extra = "") {
    return (
      await this.database.query(
        `SELECT data FROM ${table(name)} WHERE organization_id=$1 AND agent_id=$2 AND ${condition} ${extra}`,
        [...this.scope, ...args],
      )
    ).rows.map((r) => r.data);
  }
  async getConversation(cid) {
    return record(
      (await this.rows("conversations", "conversation_id=$3", [cid]))[0],
    );
  }
  async getConversationBySessionKey(key) {
    return record(
      (
        await this.rows(
          "conversations",
          "data->>'session_key'=$3 AND (data->>'active')::int=1",
          [key],
        )
      )[0],
    );
  }
  async getConversationBySessionId(id) {
    return record(
      (
        await this.rows(
          "conversations",
          "data->>'session_id'=$3",
          [id],
          "ORDER BY (data->>'active')::int DESC,conversation_id DESC LIMIT 1",
        )
      )[0],
    );
  }
  async getConversationFamilyIds({ sessionKey }) {
    return (
      await this.rows("conversations", "data->>'session_key'=$3", [sessionKey])
    ).map((r) => r.conversation_id);
  }
  async listActiveConversations(limit = 100) {
    return (
      await this.rows(
        "conversations",
        "(data->>'active')::int=1",
        [],
        `ORDER BY conversation_id LIMIT ${integer(limit, 1, 500)}`,
      )
    ).map(record);
  }
  async createConversation(input) {
    if (!input.sessionKey?.startsWith(`agent:${this.authority.agentId}:`))
      throw new Error("Session outside native agent authority");
    const cid = Number(
      (
        await this.database.query(
          `SELECT nextval('${table("conversation_ids")}') AS id`,
        )
      ).rows[0].id,
    );
    const data = {
      conversation_id: cid,
      session_id: input.sessionId,
      session_key: input.sessionKey,
      active: input.active === false ? 0 : 1,
      archived_at: null,
      archive_cause: null,
      title: input.title ?? null,
      bootstrapped_at: null,
      created_at: now(),
      updated_at: now(),
    };
    await insertRows(this.database, "conversations", ...this.scope, cid, [
      data,
    ]);
    return record(data);
  }
  async revision(cid) {
    const row = (
      await this.database.query(
        `SELECT revision FROM ${table("conversations")} WHERE organization_id=$1 AND agent_id=$2 AND conversation_id=$3`,
        [...this.scope, cid],
      )
    ).rows[0];
    if (!row) throw new Error("Conversation outside native agent authority");
    return row.revision;
  }
  async mutate(cid, version, operation) {
    return this.database.transaction(async () => {
      const updated = await this.database.query(
        `UPDATE ${table("conversations")} SET revision=revision+1 WHERE organization_id=$1 AND agent_id=$2 AND conversation_id=$3 AND revision=$4 RETURNING next_seq`,
        [...this.scope, cid, version],
      );
      if (!updated.rowCount)
        throw new Conflict(
          "Conversation changed or outside authority; reload and recompute",
        );
      return operation(updated.rows[0]);
    });
  }
  async withTransaction(operation) {
    return this.database.transaction(operation);
  }
  async getMessages(cid, { afterSeq = -1, limit } = {}) {
    return (
      await this.rows(
        "messages",
        "conversation_id=$3 AND seq>$4",
        [cid, afterSeq],
        `ORDER BY seq${limit == null ? "" : ` LIMIT ${integer(limit, 0, 1000000)}`}`,
      )
    ).map(record);
  }
  async getLastMessages(cid, count) {
    return (
      await this.rows(
        "messages",
        "conversation_id=$3",
        [cid],
        `ORDER BY seq DESC LIMIT ${integer(count, 0, 10000)}`,
      )
    )
      .reverse()
      .map(record);
  }
  async getLastMessage(cid) {
    return (await this.getLastMessages(cid, 1))[0] ?? null;
  }
  async getMessageById(id) {
    return record((await this.rows("messages", "message_id=$3", [id]))[0]);
  }
  async getMessageCount(cid) {
    return Number(
      (
        await this.database.query(
          `SELECT count(*) AS n FROM ${table("messages")} WHERE organization_id=$1 AND agent_id=$2 AND conversation_id=$3`,
          [...this.scope, cid],
        )
      ).rows[0].n,
    );
  }
  async getMaxSeq(cid) {
    return Number(
      (
        await this.database.query(
          `SELECT coalesce(max(seq),-1) AS n FROM ${table("messages")} WHERE organization_id=$1 AND agent_id=$2 AND conversation_id=$3`,
          [...this.scope, cid],
        )
      ).rows[0].n,
    );
  }
  async getMessageParts(id) {
    return (
      await this.auxiliary("message_parts", (d) => d.message_id === id)
    ).map(record);
  }
  async isTrustedTranscriptAnchor(cid, id) {
    return (
      (
        await this.auxiliary(
          "message_transcript_anchor_trust",
          (d) =>
            d.conversation_id === cid &&
            d.transcript_entry_id === id &&
            ["verified", "repaired"].includes(d.trust_state),
        )
      ).length > 0
    );
  }
  async auxiliary(name, predicate = () => true) {
    return (
      await this.database.query(
        `SELECT data FROM ${table("auxiliary_rows")} WHERE organization_id=$1 AND agent_id=$2 AND table_name=$3`,
        [...this.scope, name],
      )
    ).rows
      .map((r) => r.data)
      .filter(predicate);
  }
  async getSummary(id) {
    return record((await this.rows("summaries", "summary_id=$3", [id]))[0]);
  }
  async getSummariesByConversation(cid) {
    return (
      await this.rows(
        "summaries",
        "conversation_id=$3",
        [cid],
        "ORDER BY summary_id",
      )
    ).map(record);
  }
  async getSummaryMessages(id) {
    return (
      await this.rows(
        "summary_messages",
        "summary_id=$3",
        [id],
        "ORDER BY ordinal",
      )
    ).map((r) => r.message_id);
  }
  async getSummaryParents(id) {
    const result = await this.database.query(
      `SELECT s.data FROM ${table("summary_parents")} e JOIN ${table("summaries")} s USING(organization_id,agent_id,conversation_id) WHERE e.organization_id=$1 AND e.agent_id=$2 AND e.summary_id=$3 AND s.summary_id=e.parent_summary_id ORDER BY e.ordinal`,
      [...this.scope, id],
    );
    return result.rows.map((r) => record(r.data));
  }
  async getSummaryChildren(id) {
    const result = await this.database.query(
      `SELECT s.data FROM ${table("summary_parents")} e JOIN ${table("summaries")} s USING(organization_id,agent_id,conversation_id,summary_id) WHERE e.organization_id=$1 AND e.agent_id=$2 AND e.parent_summary_id=$3 ORDER BY e.ordinal`,
      [...this.scope, id],
    );
    return result.rows.map((r) => record(r.data));
  }
  async expand(id) {
    // UNION deduplicates shared descendants and terminates even on corrupt cycles.
    const result = await this.database.query(
      `WITH RECURSIVE nodes(summary_id,conversation_id) AS (
      SELECT summary_id,conversation_id FROM ${table("summaries")} WHERE organization_id=$1 AND agent_id=$2 AND summary_id=$3
      UNION SELECT e.parent_summary_id,e.conversation_id FROM ${table("summary_parents")} e JOIN nodes n ON n.summary_id=e.summary_id AND n.conversation_id=e.conversation_id WHERE e.organization_id=$1 AND e.agent_id=$2
    ) SELECT DISTINCT m.seq,m.data FROM nodes n JOIN ${table("summary_messages")} e ON e.summary_id=n.summary_id AND e.conversation_id=n.conversation_id JOIN ${table("messages")} m ON m.organization_id=e.organization_id AND m.agent_id=e.agent_id AND m.conversation_id=e.conversation_id AND m.message_id=e.message_id WHERE e.organization_id=$1 AND e.agent_id=$2 ORDER BY m.seq`,
      [...this.scope, id],
    );
    return result.rows.map((r) => record(r.data));
  }
  async getSummaryMessageSeqRange(id) {
    const messages = await this.expand(id);
    return {
      minSeq: messages[0]?.seq ?? null,
      maxSeq: messages.at(-1)?.seq ?? null,
    };
  }
  async getContextItems(cid) {
    return (
      await this.rows(
        "context_items",
        "conversation_id=$3",
        [cid],
        "ORDER BY ordinal",
      )
    ).map(record);
  }
  async getContextTokenCount(cid) {
    const result = await this.database.query(
      `SELECT coalesce(sum(coalesce((m.data->>'token_count')::int,(s.data->>'token_count')::int)),0) AS n FROM ${table("context_items")} c LEFT JOIN ${table("messages")} m USING(organization_id,agent_id,conversation_id,message_id) LEFT JOIN ${table("summaries")} s USING(organization_id,agent_id,conversation_id,summary_id) WHERE c.organization_id=$1 AND c.agent_id=$2 AND c.conversation_id=$3`,
      [...this.scope, cid],
    );
    return Number(result.rows[0].n);
  }
  async contextSnapshot(cid) {
    // Resolves all items in one network trip and one primary statement snapshot.
    const result = await this.database.query(
      `SELECT c.data AS item,coalesce(m.data,s.data) AS value FROM ${table("context_items")} c LEFT JOIN ${table("messages")} m USING(organization_id,agent_id,conversation_id,message_id) LEFT JOIN ${table("summaries")} s USING(organization_id,agent_id,conversation_id,summary_id) WHERE c.organization_id=$1 AND c.agent_id=$2 AND c.conversation_id=$3 ORDER BY c.ordinal`,
      [...this.scope, cid],
    );
    return result.rows.map((r) => ({
      item: record(r.item),
      value: record(r.value),
    }));
  }
  async recentTail(cid, count = 80, maxBytes = 256 * 1024) {
    integer(count, 1, 10000);
    integer(maxBytes, 2, 1024 * 1024);
    const result = await this.database.query(
      `WITH tail AS (SELECT seq,data,octet_length(data::text)+1 AS bytes FROM ${table("messages")} WHERE organization_id=$1 AND agent_id=$2 AND conversation_id=$3 ORDER BY seq DESC LIMIT $4), bounded AS (SELECT *,sum(bytes) OVER(ORDER BY seq DESC) AS total FROM tail) SELECT data FROM bounded WHERE total<=$5-1 ORDER BY seq`,
      [...this.scope, cid, count, maxBytes],
    );
    // SQL bounds the raw JSON envelope conservatively. Enforce the exact UTF-8
    // serialized public result too, including metadata and array punctuation.
    const tail = [];
    let bytes = 2;
    for (const row of result.rows.toReversed()) {
      const message = record(row.data);
      const size =
        Buffer.byteLength(JSON.stringify(message)) + (tail.length ? 1 : 0);
      if (bytes + size > maxBytes) break;
      tail.unshift(message);
      bytes += size;
    }
    return tail;
  }
  async getLargeFile(id) {
    return record((await this.rows("large_files", "file_id=$3", [id]))[0]);
  }
  async getLargeFilesByConversation(cid) {
    return (await this.rows("large_files", "conversation_id=$3", [cid])).map(
      record,
    );
  }
  async insertLargeFile(input) {
    const data = {
      file_name: null,
      mime_type: null,
      byte_size: null,
      line_count: null,
      exploration_summary: null,
      ...raw(input),
      created_at: now(),
    };
    await insertRows(
      this.database,
      "large_files",
      ...this.scope,
      input.conversationId,
      [data],
    );
    return record(data);
  }
  async commitTurn({
    conversationId: cid,
    expectedRevision,
    receiptKey,
    messages,
    cursor = null,
    advancement = null,
  }) {
    integer(messages.length, 0, 1000);
    const digest = fingerprint({ messages, cursor, advancement });
    return this.database.transaction(async () => {
      const receipt = (
        await this.database.query(
          `SELECT fingerprint,result FROM ${table("turn_receipts")} WHERE organization_id=$1 AND agent_id=$2 AND conversation_id=$3 AND receipt_key=$4`,
          [...this.scope, cid, receiptKey],
        )
      ).rows[0];
      if (receipt) {
        if (receipt.fingerprint !== digest)
          throw new Error("Idempotency key reused for different turn");
        return receipt.result;
      }
      return this.mutate(
        cid,
        expectedRevision,
        async ({ next_seq: nextSeq }) => {
          // Receipts fence whole turns; envelope IDs dedup across distinct receipt keys.
          const existing = await this.rows(
            "messages",
            "conversation_id=$3 AND (data->>'transcript_entry_id'=ANY($4::text[]) OR data->>'stable_event_key'=ANY($5::text[]))",
            [
              cid,
              messages.map((m) => m.transcriptEntryId).filter(Boolean),
              messages.map((m) => m.stableEventKey).filter(Boolean),
            ],
          );
          const envelopeIds = new Map(
            existing
              .filter((r) => r.transcript_entry_id)
              .map((r) => [r.transcript_entry_id, r]),
          );
          const eventKeys = new Map(
            existing
              .filter((r) => r.stable_event_key)
              .map((r) => [r.stable_event_key, r]),
          );
          const inputs = [];
          for (const message of messages) {
            const envelope =
              message.transcriptEntryId &&
              envelopeIds.get(message.transcriptEntryId);
            if (envelope) {
              if (
                envelope.content !== message.content ||
                envelope.role !== message.role
              )
                throw new Error(
                  "Transcript entry reused for different content",
                );
              continue;
            }
            const event =
              message.stableEventKey && eventKeys.get(message.stableEventKey);
            if (
              event &&
              event.content === message.content &&
              event.role === message.role
            )
              continue;
            // The pinned store preserves content on stable-event collisions by dropping the conflicting key.
            const input = event
              ? { ...message, stableEventKey: null }
              : message;
            inputs.push(input);
            if (input.transcriptEntryId)
              envelopeIds.set(input.transcriptEntryId, input);
            if (input.stableEventKey)
              eventKeys.set(input.stableEventKey, input);
          }
          const ids = (
            await this.database.query(
              `SELECT nextval('${table("message_ids")}') AS id FROM generate_series(1,$1::int)`,
              [inputs.length],
            )
          ).rows.map((r) => Number(r.id));
          const rows = inputs.map((m, i) => ({
            message_id: ids[i],
            conversation_id: cid,
            seq: nextSeq + i,
            role: m.role,
            content: m.content,
            token_count: m.tokenCount,
            created_at: m.createdAt
              ? new Date(m.createdAt).toISOString()
              : now(),
            large_content: null,
            identity_hash: buildMessageIdentityHash(m.role, m.content),
            transcript_entry_id: m.transcriptEntryId ?? null,
            stable_event_key: m.stableEventKey ?? null,
            openclaw_sender_metadata: serializeOpenClawSenderMetadata(
              m.role === "user" ? m.openClawSenderMetadata : null,
            ),
          }));
          await insertRows(this.database, "messages", ...this.scope, cid, rows);
          for (const [index, input] of inputs.entries()) {
            for (const [ordinal, part] of (input.parts ?? []).entries()) {
              const data = {
                part_id: randomUUID(),
                message_id: ids[index],
                session_id: part.sessionId,
                part_type: part.partType,
                ordinal: part.ordinal ?? ordinal,
                text_content: part.textContent ?? null,
                tool_call_id: part.toolCallId ?? null,
                tool_name: part.toolName ?? null,
                tool_input: part.toolInput ?? null,
                tool_output: part.toolOutput ?? null,
                metadata: part.metadata ?? null,
              };
              if (
                !data.session_id ||
                ![
                  "text",
                  "reasoning",
                  "tool",
                  "patch",
                  "file",
                  "subtask",
                  "compaction",
                  "step_start",
                  "step_finish",
                  "snapshot",
                  "agent",
                  "retry",
                ].includes(data.part_type)
              )
                throw new Error("Invalid structured message part");
              await this.database.query(
                `INSERT INTO ${table("auxiliary_rows")} VALUES($1,'message_parts',$3,$2,$4,$5)`,
                [...this.scope, JSON.stringify([data.part_id]), cid, data],
              );
            }
          }
          const context = await this.getContextItems(cid);
          await insertRows(
            this.database,
            "context_items",
            ...this.scope,
            cid,
            rows.map((m, i) => ({
              conversation_id: cid,
              ordinal: context.length + i,
              item_type: "message",
              message_id: m.message_id,
              summary_id: null,
              created_at: m.created_at,
            })),
          );
          await this.database.query(
            `UPDATE ${table("conversations")} SET next_seq=$4 WHERE organization_id=$1 AND agent_id=$2 AND conversation_id=$3`,
            [...this.scope, cid, nextSeq + rows.length],
          );
          if (cursor != null)
            await this.database.query(
              `INSERT INTO ${table("auxiliary_rows")} VALUES($1,'conversation_transcript_cursors',$3,$2,$4,$5) ON CONFLICT(organization_id,table_name,row_key) DO UPDATE SET data=excluded.data`,
              [
                ...this.scope,
                JSON.stringify([cid]),
                cid,
                {
                  conversation_id: cid,
                  mode: "cursor_v1",
                  cursor: cursor.cursor,
                  frontier_entry_id: cursor.frontierEntryId ?? null,
                  frontier_seq: cursor.frontierSeq ?? null,
                  origin: cursor.origin ?? "fresh",
                  created_at: now(),
                  updated_at: now(),
                },
              ],
            );
          if (advancement) {
            const data = {
              advancement_key: receiptKey,
              payload_hash: digest,
              session_id: advancement.sessionId,
              session_key: advancement.sessionKey ?? null,
              admission_entry_id: advancement.admissionEntryId,
              terminal_entry_id: advancement.terminalEntryId,
              message_count: messages.length,
              committed_at: now(),
            };
            if (
              !data.session_id ||
              !data.admission_entry_id ||
              !data.terminal_entry_id
            )
              throw new Error("Incomplete native turn receipt");
            await this.database.query(
              `INSERT INTO ${table("auxiliary_rows")} VALUES($1,'turn_advancements',$3,$2,$4,$5)`,
              [...this.scope, JSON.stringify([receiptKey]), cid, data],
            );
          }
          const result = {
            inserted: rows.length,
            messageIds: ids,
            revision: expectedRevision + 1,
          };
          await this.database.query(
            `INSERT INTO ${table("turn_receipts")} VALUES($1,$2,$3,$4,$5,$6)`,
            [...this.scope, cid, receiptKey, digest, result],
          );
          return result;
        },
      );
    });
  }
  async commitCompaction({
    conversationId: cid,
    expectedRevision,
    summary,
    messageIds = [],
    sourceSummaryIds = [],
    startOrdinal,
    endOrdinal,
  }) {
    integer(startOrdinal);
    integer(endOrdinal, startOrdinal);
    return this.mutate(cid, expectedRevision, async () => {
      const context = await this.getContextItems(cid);
      if (endOrdinal >= context.length)
        throw new Error("Compaction range outside context");
      const source = context.slice(startOrdinal, endOrdinal + 1);
      if (
        JSON.stringify(
          source
            .filter((c) => c.itemType === "message")
            .map((c) => c.messageId),
        ) !== JSON.stringify(messageIds) ||
        JSON.stringify(
          source
            .filter((c) => c.itemType === "summary")
            .map((c) => c.summaryId),
        ) !== JSON.stringify(sourceSummaryIds)
      )
        throw new Error("Compaction source does not match fenced context");
      const data = {
        summary_id: summary.summaryId,
        conversation_id: cid,
        kind: sourceSummaryIds.length ? "condensed" : "leaf",
        depth: summary.depth ?? 0,
        content: summary.content,
        token_count: summary.tokenCount,
        file_ids: JSON.stringify(summary.fileIds ?? []),
        earliest_at: null,
        latest_at: null,
        descendant_count: messageIds.length,
        descendant_token_count: 0,
        source_message_token_count: 0,
        model: summary.model ?? "unknown",
        ...raw(summary),
        conversation_id: cid,
        created_at: now(),
      };
      data.file_ids = JSON.stringify(summary.fileIds ?? []);
      const parents = sourceSummaryIds.length
        ? await this.rows(
            "summaries",
            "conversation_id=$3 AND summary_id=ANY($4::text[])",
            [cid, sourceSummaryIds],
          )
        : [];
      if (
        parents.length !== sourceSummaryIds.length ||
        parents.some((p) => p.depth >= data.depth)
      )
        throw new Error(
          "Summary depth must exceed all sources (acyclic graph)",
        );
      await insertRows(this.database, "summaries", ...this.scope, cid, [data]);
      await insertRows(
        this.database,
        "summary_messages",
        ...this.scope,
        cid,
        messageIds.map((id, i) => ({
          summary_id: data.summary_id,
          message_id: id,
          ordinal: i,
        })),
      );
      await insertRows(
        this.database,
        "summary_parents",
        ...this.scope,
        cid,
        sourceSummaryIds.map((id, i) => ({
          summary_id: data.summary_id,
          parent_summary_id: id,
          ordinal: i,
        })),
      );
      const replacement = {
        conversationId: cid,
        itemType: "summary",
        messageId: null,
        summaryId: data.summary_id,
        createdAt: new Date(data.created_at),
      };
      const items = [
        ...context.slice(0, startOrdinal),
        replacement,
        ...context.slice(endOrdinal + 1),
      ].map((item, i) => ({ ...raw(item), ordinal: i }));
      await this.database.query(
        `DELETE FROM ${table("context_items")} WHERE organization_id=$1 AND agent_id=$2 AND conversation_id=$3`,
        [...this.scope, cid],
      );
      await insertRows(
        this.database,
        "context_items",
        ...this.scope,
        cid,
        items,
      );
      return record(data);
    });
  }
  async archiveConversation(conversationId, cause, expectedRevision) {
    if (
      ![
        "manual-reset",
        "session-deleted",
        "session-end",
        "rollover-fallback",
        "cron-rotation",
      ].includes(cause)
    )
      throw new Error("Invalid archive cause");
    return this.mutate(conversationId, expectedRevision, async () => {
      await this.database.query(
        `UPDATE ${table("conversations")} SET data=data || $4::jsonb WHERE organization_id=$1 AND agent_id=$2 AND conversation_id=$3`,
        [
          ...this.scope,
          conversationId,
          {
            active: 0,
            archived_at: now(),
            archive_cause: cause,
            updated_at: now(),
          },
        ],
      );
    });
  }
  async deleteConversation(conversationId, expectedRevision) {
    return this.mutate(conversationId, expectedRevision, async () => {
      await this.database.query(
        `DELETE FROM ${table("conversations")} WHERE organization_id=$1 AND agent_id=$2 AND conversation_id=$3`,
        [...this.scope, conversationId],
      );
    });
  }
  async searchMessages(input) {
    return this.search("messages", input);
  }
  async searchSummaries(input) {
    return this.search("summaries", input);
  }
  async search(name, input) {
    const limit = integer(input.limit ?? 50, 1, 500);
    const timestampText =
      name === "messages"
        ? "data->>'created_at'"
        : "coalesce(data->>'latest_at',data->>'created_at')";
    // Imported SQLite timestamps without offsets are UTC, independent of the
    // backend selected by transaction pooling or its current TimeZone setting.
    const timestamp = `btrim(${timestampText})`;
    const stamp = `(CASE WHEN ${timestamp} ~ '([zZ]|[+-][0-9]{2}:[0-9]{2})$' THEN ${timestamp}::timestamptz ELSE ${timestamp}::timestamp AT TIME ZONE 'UTC' END)`;
    const args = [...this.scope];
    const bind = (v) => {
      args.push(v);
      return `$${args.length}`;
    };
    let where = `organization_id=$1 AND agent_id=$2`;
    if (input.conversationIds != null) {
      const ids = [
        ...new Set(
          input.conversationIds.filter(Number.isFinite).map(Math.trunc),
        ),
      ];
      // Nonempty native family lists take precedence over the current physical
      // conversation. An explicitly empty/invalid list remains fail-closed.
      if (!ids.length) return [];
      where += ` AND conversation_id=ANY(${bind(ids)}::bigint[])`;
    } else if (input.conversationId != null)
      where += ` AND conversation_id=${bind(input.conversationId)}`;
    if (input.since) where += ` AND ${stamp}>=${bind(input.since)}`;
    if (input.before) where += ` AND ${stamp}<${bind(input.before)}`;
    const regex =
      input.mode === "regex" ? compileSafeSearchRegex(input.query) : null;
    if (input.mode === "regex" && !regex) return [];
    let terms = [],
      rank = "0",
      snippet = "content";
    let order = `${stamp} DESC`;
    if (input.mode === "regex") {
      // Keep upstream ECMAScript regex semantics and its 10k candidate ceiling.
    } else if (input.mode === "literal" || containsCjk(input.query)) {
      terms = buildLikeSearchPlan("content", input.query).terms;
      if (!terms.length) return [];
      for (const term of terms)
        where += ` AND strpos(lower(content),${bind(term)})>0`;
    } else if (input.mode === "full_text") {
      // Upstream sanitizes operators into literal terms and keeps quoted phrases.
      // Each phrase uses simple configuration (no stemming/stop words); AND phrases.
      const phrases = [...input.query.matchAll(/"([^"]+)"|(\S+)/g)]
        .map((m) => (m[1] ?? m[2]).replaceAll('"', ""))
        .filter(Boolean);
      if (!phrases.length) return [];
      const tsquery = phrases
        .map((p) => `phraseto_tsquery('simple',${bind(p)})`)
        .join(" && ");
      where += ` AND search @@ (${tsquery})`;
      rank = `ts_rank_cd(search,(${tsquery}))`;
      snippet = `ts_headline('simple',content,(${tsquery}),'MaxWords=24,MinWords=8,StartSel="",StopSel=""')`;
      order =
        input.sort === "relevance"
          ? `${rank} DESC,${stamp} DESC`
          : input.sort === "hybrid"
            ? `(${rank}/(1+greatest(0,extract(epoch FROM (now()-${stamp}))/3600)*0.001)) DESC,${stamp} DESC`
            : `${stamp} DESC,${rank} DESC`;
    } else throw new Error("Unsupported search mode");
    const result = await this.database.query(
      `SELECT data,${rank} AS rank,${snippet} AS snippet FROM ${table(name)} WHERE ${where} ORDER BY ${order},${name === "messages" ? "message_id" : "summary_id"} LIMIT ${regex ? 10000 : limit}`,
      args,
    );
    const output = [];
    for (const r of result.rows) {
      const match = regex?.exec(r.data.content);
      if (regex && !match) continue;
      const data = record(r.data);
      output.push({
        ...(name === "messages"
          ? { messageId: data.messageId, role: data.role }
          : { summaryId: data.summaryId, kind: data.kind }),
        conversationId: data.conversationId,
        createdAt:
          name === "summaries"
            ? (data.latestAt ?? data.createdAt)
            : data.createdAt,
        snippet:
          match?.[0] ??
          (terms.length
            ? createFallbackSnippet(r.data.content, terms)
            : r.snippet),
        rank: Number(r.rank),
      });
      if (output.length >= limit) break;
    }
    return output;
  }
}
export function openExperimentalStore({ backend, database, authority }) {
  if (backend !== "postgres")
    throw new Error("Explicit experimental backend=postgres required");
  return new PostgresStore(database, authority);
}
