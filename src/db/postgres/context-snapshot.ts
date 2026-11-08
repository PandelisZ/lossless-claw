import type { StorageDatabase } from "../storage.js";
import { isPostgresDatabase } from "../storage.js";
import {
  toMessageRecord,
  toMessagePartRecord,
  type ConversationStore,
} from "../../store/conversation-store.js";
import {
  toSummaryRecord,
  toContextItemRecord,
  toLargeFileRecord,
  type SummaryStore,
} from "../../store/summary-store.js";
import {
  toFocusBriefRecord,
  type FocusBriefStore,
} from "../../store/focus-brief-store.js";

type MessageRow = Parameters<typeof toMessageRecord>[0];
type PartRow = Parameters<typeof toMessagePartRecord>[0];
type SummaryRow = Parameters<typeof toSummaryRecord>[0];
type ContextRow = Parameters<typeof toContextItemRecord>[0];
type FileRow = Parameters<typeof toLargeFileRecord>[0];
type FocusRow = Parameters<typeof toFocusBriefRecord>[0];
type SourceRow = { summary_id: string; message_id: number; ordinal: number };
type ParentRow = {
  summary_id: string;
  parent_summary_id: string;
  ordinal: number;
};
type Payload = {
  revision: number;
  revisions: { conversation_id: number; revision: number }[];
  context: ContextRow[];
  messages: MessageRow[];
  parts: PartRow[];
  summaries: SummaryRow[];
  sources: SourceRow[];
  edges: ParentRow[];
  files: FileRow[];
  focus: FocusRow[];
  trust: {
    conversation_id: number;
    transcript_entry_id: string;
    trust_state: string;
  }[];
  overflow?: boolean;
};

/** One statement, one MVCC snapshot, no transaction held during assembly/inference. */
export async function loadContextSnapshot(
  db: StorageDatabase,
  conversationId: number,
  {
    maxRows = 10000,
    maxBytes = 16 * 1024 * 1024,
  }: { maxRows?: number; maxBytes?: number } = {}
) {
  if (
    !Number.isSafeInteger(maxRows) ||
    maxRows < 1 ||
    maxRows > 10000 ||
    !Number.isSafeInteger(maxBytes) ||
    maxBytes < 1 ||
    maxBytes > 16 * 1024 * 1024
  )
    throw new Error("Invalid LCM snapshot limits");
  if (!isPostgresDatabase(db))
    throw new Error(
      "PostgreSQL context snapshots require a PostgreSQL database"
    );
  const row = await db
    .prepare(
      `WITH RECURSIVE
 owner AS MATERIALIZED (SELECT revision FROM conversations WHERE conversation_id = ?),
 snap_context AS MATERIALIZED (SELECT * FROM context_items WHERE conversation_id = $1 ORDER BY ordinal LIMIT $2+1),
 nodes(summary_id) AS (
 SELECT summary_id FROM snap_context WHERE summary_id IS NOT NULL
 UNION SELECT p.parent_summary_id FROM summary_parents p JOIN nodes n ON n.summary_id = p.summary_id
 ),
 snap_summaries AS MATERIALIZED (SELECT s.* FROM summaries s JOIN nodes USING(summary_id) LIMIT $2+1),
 snap_sources AS MATERIALIZED (SELECT e.* FROM summary_messages e JOIN snap_summaries USING(summary_id) ORDER BY ordinal LIMIT $2+1),
 snap_edges AS MATERIALIZED (SELECT e.* FROM summary_parents e JOIN snap_summaries USING(summary_id) ORDER BY ordinal LIMIT $2+1),
 ids AS (SELECT message_id FROM snap_context WHERE message_id IS NOT NULL UNION SELECT message_id FROM snap_sources),
 snap_messages AS MATERIALIZED (SELECT m.* FROM messages m JOIN ids USING(message_id) LIMIT $2+1),
 snap_parts AS MATERIALIZED (SELECT p.* FROM message_parts p JOIN ids USING(message_id) ORDER BY ordinal LIMIT $2+1),
 snap_trust AS MATERIALIZED (SELECT t.* FROM message_transcript_anchor_trust t WHERE EXISTS (SELECT 1 FROM snap_messages m WHERE m.conversation_id=t.conversation_id AND m.transcript_entry_id=t.transcript_entry_id) LIMIT $2+1),
 snap_files AS MATERIALIZED (SELECT * FROM large_files WHERE conversation_id = $1 OR conversation_id IN (SELECT conversation_id FROM snap_messages) OR conversation_id IN (SELECT conversation_id FROM snap_summaries) OR file_id IN (SELECT large_content FROM snap_messages) LIMIT $2+1),
 snap_focus AS MATERIALIZED (SELECT * FROM focus_briefs WHERE conversation_id = $1 AND status = 'active' ORDER BY created_at DESC,rowid DESC LIMIT 1),
 snap_revisions AS MATERIALIZED (SELECT conversation_id,revision FROM conversations WHERE conversation_id=$1 OR conversation_id IN (SELECT conversation_id FROM snap_messages) OR conversation_id IN (SELECT conversation_id FROM snap_summaries)),
 payload AS (SELECT jsonb_build_object(
 'revision',(SELECT revision FROM owner),
 'revisions',COALESCE((SELECT jsonb_agg(to_jsonb(r) ORDER BY conversation_id) FROM snap_revisions r),'[]'::jsonb),
 'context',COALESCE((SELECT jsonb_agg(to_jsonb(c) ORDER BY ordinal) FROM snap_context c),'[]'::jsonb),
 'messages',COALESCE((SELECT jsonb_agg(to_jsonb(m)) FROM snap_messages m),'[]'::jsonb),
 'parts',COALESCE((SELECT jsonb_agg(to_jsonb(p) ORDER BY ordinal) FROM snap_parts p),'[]'::jsonb),
 'summaries',COALESCE((SELECT jsonb_agg(to_jsonb(s)) FROM snap_summaries s),'[]'::jsonb),
 'sources',COALESCE((SELECT jsonb_agg(to_jsonb(e) ORDER BY ordinal) FROM snap_sources e),'[]'::jsonb),
 'edges',COALESCE((SELECT jsonb_agg(to_jsonb(e) ORDER BY ordinal) FROM snap_edges e),'[]'::jsonb),
 'trust',COALESCE((SELECT jsonb_agg(to_jsonb(t)) FROM snap_trust t),'[]'::jsonb),
 'files',COALESCE((SELECT jsonb_agg(to_jsonb(f)) FROM snap_files f),'[]'::jsonb),
 'focus',COALESCE((SELECT jsonb_agg(to_jsonb(f)) FROM snap_focus f),'[]'::jsonb)) AS data)
 SELECT CASE WHEN octet_length(data::text) > $3 THEN jsonb_build_object('overflow',true) ELSE data END AS data FROM payload`
    )
    .get(conversationId, maxRows, maxBytes);
  const data = row!.data as Payload;
  function validateNumbers(value: unknown): void {
    if (
      typeof value === "number" &&
      (!Number.isFinite(value) ||
        (Number.isInteger(value) && !Number.isSafeInteger(value)))
    )
      throw new Error("LCM snapshot contains an unsafe numeric value");
    if (value && typeof value === "object")
      for (const child of Object.values(value)) validateNumbers(child);
  }
  validateNumbers(data);
  if (
    data.overflow ||
    Object.values(data).some((v) => Array.isArray(v) && v.length > maxRows)
  )
    throw new Error(
      "LCM operation snapshot exceeds row or serialized JSON byte limit; no partial context returned"
    );
  if (!Number.isSafeInteger(data.revision))
    throw new Error(
      "LCM snapshot conversation is absent or revision is unsafe"
    );
  let closed = false;
  const check = (cid = conversationId) => {
    if (closed || cid !== conversationId)
      throw new Error(
        "LCM snapshot is closed or outside conversation authority"
      );
  };
  const copy = <T>(value: T): T => {
    check();
    return structuredClone(value);
  };
  const messages = new Map(
    data.messages.map((r) => [r.message_id, toMessageRecord(r)])
  );
  const summaries = new Map(
    data.summaries.map((r) => [r.summary_id, toSummaryRecord(r)])
  );
  const facade = <T>(methods: object): T =>
    new Proxy(Object.freeze(methods), {
      get(target, key) {
        if (key in target) return Reflect.get(target, key);
        throw new Error(`Unexpected snapshot method ${String(key)}`);
      },
    }) as T;
  return Object.freeze({
    revision: data.revision,
    revisions: Object.freeze(
      data.revisions.map((r) =>
        Object.freeze({
          conversationId: r.conversation_id,
          revision: r.revision,
        })
      )
    ),
    conversationStore: facade<ConversationStore>({
      async getMessageById(id: number) {
        return copy(messages.get(id) ?? null);
      },
      async getMessageParts(id: number) {
        check();
        return data.parts
          .filter((r) => r.message_id === id)
          .map((r) => toMessagePartRecord(copy(r)));
      },
      async isTrustedTranscriptAnchor(cid: number, id: string) {
        check();
        if (!data.revisions.some((r) => r.conversation_id === cid))
          throw new Error("LCM snapshot anchor outside source authority");
        return data.trust.some(
          (r) =>
            r.conversation_id === cid &&
            r.transcript_entry_id === id &&
            ["verified", "repaired"].includes(r.trust_state)
        );
      },
    }),
    summaryStore: facade<SummaryStore>({
      async getContextTokenCount(cid: number) {
        check(cid);
        return data.context.reduce(
          (total, item) =>
            total +
            (item.message_id != null
              ? messages.get(item.message_id)?.tokenCount ?? 0
              : summaries.get(item.summary_id!)?.tokenCount ?? 0),
          0
        );
      },
      async getContextItems(cid: number) {
        check(cid);
        return data.context.map((r) => toContextItemRecord(copy(r)));
      },
      async getSummary(id: string) {
        return copy(summaries.get(id) ?? null);
      },
      async getSummaryMessages(id: string) {
        check();
        return data.sources
          .filter((e) => e.summary_id === id)
          .map((e) => e.message_id);
      },
      async getSummaryParents(id: string) {
        check();
        return data.edges
          .filter((e) => e.summary_id === id)
          .map((e) => copy(summaries.get(e.parent_summary_id)!))
          .filter(Boolean);
      },
      async getLargeFile(id: string) {
        check();
        const r = data.files.find((r) => r.file_id === id);
        return r ? toLargeFileRecord(copy(r)) : null;
      },
      async getSummaryMessageSeqRange(id: string) {
        check();
        const seen = new Set<string>(),
          seqs: number[] = [];
        function visit(sid: string) {
          if (seen.has(sid)) return;
          seen.add(sid);
          for (const e of data.sources)
            if (e.summary_id === sid) {
              const m = messages.get(e.message_id);
              if (m) seqs.push(m.seq);
            }
          for (const e of data.edges)
            if (e.summary_id === sid) visit(e.parent_summary_id);
        }
        visit(id);
        return {
          minSeq: seqs.length ? Math.min(...seqs) : null,
          maxSeq: seqs.length ? Math.max(...seqs) : null,
        };
      },
    }),
    focusBriefStore: facade<Pick<FocusBriefStore, "getActiveFocusBrief">>({
      async getActiveFocusBrief(cid: number) {
        check(cid);
        return data.focus[0] ? toFocusBriefRecord(copy(data.focus[0])) : null;
      },
    }),
    close() {
      closed = true;
      messages.clear();
      summaries.clear();
      for (const v of Object.values(data)) if (Array.isArray(v)) v.length = 0;
    },
  });
}
