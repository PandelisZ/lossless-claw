// A single statement provides one MVCC snapshot. No session state or SQL
// transaction remains open while the pinned upstream code runs or calls inference.
import { table } from "./database.mjs";
import { record } from "./store.mjs";
export class SnapshotLimit extends Error {}
export async function operationSnapshot(
  store,
  conversationId,
  { maxRows = 10000, maxBytes = 16 * 1024 * 1024 } = {},
) {
  if (
    !Number.isSafeInteger(maxRows) ||
    maxRows < 1 ||
    maxRows > 10000 ||
    !Number.isSafeInteger(maxBytes) ||
    maxBytes < 1 ||
    maxBytes > 16 * 1024 * 1024
  )
    throw new Error("Invalid snapshot limits");
  const result = await store.database.query(
    `WITH RECURSIVE
    owner AS MATERIALIZED (SELECT * FROM ${table("conversations")} WHERE organization_id=$1 AND agent_id=$2 AND conversation_id=$3),
    context AS MATERIALIZED (SELECT c.* FROM ${table("context_items")} c JOIN owner USING(organization_id,agent_id,conversation_id) ORDER BY ordinal LIMIT $4+1),
    nodes(summary_id) AS (
      SELECT summary_id FROM context WHERE summary_id IS NOT NULL
      UNION SELECT e.parent_summary_id FROM ${table("summary_parents")} e JOIN owner USING(organization_id,agent_id,conversation_id) JOIN nodes n ON n.summary_id=e.summary_id
    ),
    summaries AS MATERIALIZED (SELECT s.* FROM ${table("summaries")} s JOIN owner USING(organization_id,agent_id,conversation_id) JOIN nodes USING(summary_id) LIMIT $4+1),
    sources AS MATERIALIZED (SELECT e.* FROM ${table("summary_messages")} e JOIN owner USING(organization_id,agent_id,conversation_id) JOIN summaries USING(summary_id) LIMIT $4+1),
    edges AS MATERIALIZED (SELECT e.* FROM ${table("summary_parents")} e JOIN owner USING(organization_id,agent_id,conversation_id) JOIN summaries USING(summary_id) LIMIT $4+1),
    ids AS (SELECT message_id FROM context WHERE message_id IS NOT NULL UNION SELECT message_id FROM sources),
    messages AS MATERIALIZED (SELECT m.* FROM ${table("messages")} m JOIN owner USING(organization_id,agent_id,conversation_id) JOIN ids USING(message_id) LIMIT $4+1),
    auxiliary AS MATERIALIZED (SELECT a.* FROM ${table("auxiliary_rows")} a JOIN owner USING(organization_id,agent_id,conversation_id)
      WHERE a.table_name IN ('message_parts','message_transcript_anchor_trust') AND (a.data->>'message_id')::bigint IN (SELECT message_id FROM ids) LIMIT $4+1),
    files AS MATERIALIZED (SELECT f.* FROM ${table("large_files")} f JOIN owner o ON f.organization_id=o.organization_id AND f.agent_id=o.agent_id WHERE f.conversation_id=o.conversation_id OR f.file_id IN (SELECT data->>'large_content' FROM messages) LIMIT $4+1),
    payload AS (SELECT jsonb_build_object(
      'conversation',(SELECT data FROM owner),'revision',(SELECT revision FROM owner),
      'context',coalesce((SELECT jsonb_agg(data ORDER BY ordinal) FROM context),'[]'::jsonb),
      'messages',coalesce((SELECT jsonb_agg(data ORDER BY seq) FROM messages),'[]'::jsonb),
      'summaries',coalesce((SELECT jsonb_agg(data) FROM summaries),'[]'::jsonb),
      'sources',coalesce((SELECT jsonb_agg(data ORDER BY ordinal) FROM sources),'[]'::jsonb),
      'edges',coalesce((SELECT jsonb_agg(data ORDER BY ordinal) FROM edges),'[]'::jsonb),
      'auxiliary',coalesce((SELECT jsonb_agg(jsonb_build_object('name',table_name,'data',data)) FROM auxiliary),'[]'::jsonb),
      'files',coalesce((SELECT jsonb_agg(data) FROM files),'[]'::jsonb)) AS data)
    SELECT CASE WHEN octet_length(data::text)>$5 THEN jsonb_build_object('overflow',true) ELSE data END AS data FROM payload`,
    [...store.scope, conversationId, maxRows, maxBytes],
  );
  const data = result.rows[0].data;
  if (
    data.overflow ||
    Object.values(data).some((v) => Array.isArray(v) && v.length > maxRows)
  )
    throw new SnapshotLimit(
      "Reachable operation snapshot exceeds limits; no partial assembly allowed",
    );
  if (!data.conversation)
    throw new Error("Conversation outside native agent authority");
  return new OperationSnapshot(data);
}
class OperationSnapshot {
  #data;
  #messages;
  #summaries;
  #ranges = new Map();
  #closed = false;
  constructor(data) {
    this.#data = data;
    this.#messages = new Map(
      data.messages.map((r) => [r.message_id, record(r)]),
    );
    this.#summaries = new Map(
      data.summaries.map((r) => [r.summary_id, record(r)]),
    );
  }
  close() {
    this.#closed = true;
    this.#data = null;
    this.#messages.clear();
    this.#summaries.clear();
    this.#ranges.clear();
  }
  #check(cid = this.#data?.conversation.conversation_id) {
    if (this.#closed) throw new Error("Operation snapshot is closed");
    if (cid !== this.#data.conversation.conversation_id)
      throw new Error("Operation snapshot crossed conversation authority");
  }
  #copy(value) {
    this.#check();
    return structuredClone(value);
  }
  async revision(cid) {
    this.#check(cid);
    return this.#data.revision;
  }
  async getConversation(cid) {
    this.#check(cid);
    return record(this.#copy(this.#data.conversation));
  }
  async getContextItems(cid) {
    this.#check(cid);
    return this.#data.context.map((r) => record(this.#copy(r)));
  }
  async getMessageById(id) {
    return this.#copy(this.#messages.get(id) ?? null);
  }
  async getSummary(id) {
    return this.#copy(this.#summaries.get(id) ?? null);
  }
  async getMessageParts(id) {
    this.#check();
    return this.#data.auxiliary
      .filter((r) => r.name === "message_parts" && r.data.message_id === id)
      .map((r) => record(this.#copy(r.data)))
      .sort((a, b) => a.ordinal - b.ordinal);
  }
  async isTrustedTranscriptAnchor(cid, id) {
    this.#check(cid);
    return this.#data.auxiliary.some(
      (r) =>
        r.name === "message_transcript_anchor_trust" &&
        r.data.transcript_entry_id === id &&
        ["verified", "repaired"].includes(r.data.trust_state),
    );
  }
  async getLargeFile(id) {
    this.#check();
    return record(
      this.#copy(this.#data.files.find((r) => r.file_id === id) ?? null),
    );
  }
  async getSummaryMessages(id) {
    this.#check();
    return this.#data.sources
      .filter((r) => r.summary_id === id)
      .map((r) => r.message_id);
  }
  async getSummaryParents(id) {
    this.#check();
    return this.#data.edges
      .filter((r) => r.summary_id === id)
      .map((r) => this.#copy(this.#summaries.get(r.parent_summary_id)))
      .filter(Boolean);
  }
  async getSummaryMessageSeqRange(id) {
    this.#check();
    if (!this.#ranges.has(id)) {
      const seen = new Set(),
        seqs = [];
      const visit = (sid) => {
        if (seen.has(sid)) return;
        seen.add(sid);
        for (const edge of this.#data.sources)
          if (edge.summary_id === sid) {
            const message = this.#messages.get(edge.message_id);
            if (message) seqs.push(message.seq);
          }
        for (const edge of this.#data.edges)
          if (edge.summary_id === sid) visit(edge.parent_summary_id);
      };
      visit(id);
      this.#ranges.set(id, {
        minSeq: seqs.length ? Math.min(...seqs) : null,
        maxSeq: seqs.length ? Math.max(...seqs) : null,
      });
    }
    return this.#copy(this.#ranges.get(id));
  }
  async getContextTokenCount(cid) {
    this.#check(cid);
    return this.#data.context.reduce(
      (total, item) =>
        total +
        (this.#messages.get(item.message_id)?.tokenCount ??
          this.#summaries.get(item.summary_id)?.tokenCount ??
          0),
      0,
    );
  }
}
