import { createHash } from "node:crypto";
import { DatabaseSync } from "node:sqlite";
import { ContextAssembler } from "../assembler.js";
import { CompactionEngine } from "../compaction.js";
import { runLcmMigrations } from "../db/migration.js";
import {
  ConversationStore,
  type CreateMessageInput,
} from "../store/conversation-store.js";
import { SummaryStore } from "../store/summary-store.js";
import {
  type Document,
  type DocumentTransport,
  TurbopufferTransport,
} from "./turbopuffer-transport.js";

type Row = Record<string, string | number | null>;
export type ConversationTables = Record<string, Row[]>;
type Receipt = { digest: string; result: unknown };
type Manifest = {
  blocks: string[];
  receipts: Record<string, Receipt>;
  parent: string;
};
type Head = { revision: number; commit: string; manifest: Manifest };
type Session = {
  db: DatabaseSync;
  conversations: ConversationStore;
  summaries: SummaryStore;
};
export class StorageConflict extends Error {}
export class UnknownCommit extends Error {}
const hash = (value: unknown) =>
  createHash("sha256").update(JSON.stringify(value)).digest("hex").slice(0, 60);
function pack(value: any): any {
  if (value === undefined) return { type: "undefined" };
  if (value instanceof Date)
    return { type: "date", value: value.toISOString() };
  if (Array.isArray(value)) return { type: "array", value: value.map(pack) };
  if (value && typeof value === "object")
    return {
      type: "object",
      value: Object.entries(value).map(([key, item]) => [key, pack(item)]),
    };
  return value;
}
function unpack(value: any): any {
  if (!value || typeof value !== "object") return value;
  if (value.type === "undefined") return undefined;
  if (value.type === "date") return new Date(value.value);
  if (value.type === "array") return value.value.map(unpack);
  if (value.type === "object")
    return Object.fromEntries(
      value.value.map(([key, item]: [string, unknown]) => [key, unpack(item)]),
    );
  throw new Error("Invalid receipt encoding");
}
const identifier = (value: string) => {
  if (!/^[a-z_]+$/.test(value))
    throw new Error("Invalid table/column identifier");
  return `"${value}"`;
};
function session(): Session {
  const db = new DatabaseSync(":memory:");
  runLcmMigrations(db, { fts5Available: true });
  return {
    db,
    conversations: new ConversationStore(db),
    summaries: new SummaryStore(db),
  };
}
function tables(db: DatabaseSync): ConversationTables {
  const result: ConversationTables = {};
  const names = db
    .prepare(
      "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' AND name NOT LIKE '%_fts%' AND name != 'lcm_migration_state' ORDER BY name",
    )
    .all();
  for (const entry of names) {
    const name = String(entry.name);
    const columns = db.prepare(`PRAGMA table_info(${identifier(name)})`).all();
    const keys = columns
      .filter((column) => Number(column.pk) > 0)
      .sort((a, b) => Number(a.pk) - Number(b.pk))
      .map((column) => identifier(String(column.name)));
    result[name] = db
      .prepare(
        `SELECT * FROM ${identifier(name)}${keys.length ? ` ORDER BY ${keys.join(",")}` : ""}`,
      )
      .all() as Row[];
  }
  return result;
}
function hydrate(data: ConversationTables): Session {
  const target = session();
  try {
    target.db.exec("PRAGMA foreign_keys=OFF; BEGIN");
    for (const [name, rows] of Object.entries(data)) {
      for (const row of rows) {
        const columns = Object.keys(row);
        const insert = target.db.prepare(
          `INSERT INTO ${identifier(name)} (${columns.map(identifier).join(",")}) VALUES (${columns.map(() => "?").join(",")})`,
        );
        insert.run(...columns.map((column) => row[column]));
      }
    }
    target.db.exec(
      "INSERT INTO messages_fts(rowid,content) SELECT message_id,content FROM messages; INSERT INTO summaries_fts(summary_id,content) SELECT summary_id,content FROM summaries; INSERT INTO summaries_fts_cjk(summary_id,content) SELECT summary_id,content FROM summaries; COMMIT; PRAGMA foreign_keys=ON",
    );
    if (target.db.prepare("PRAGMA foreign_key_check").all().length)
      throw new Error("Snapshot has dangling graph references");
    return target;
  } catch (error) {
    target.db.close();
    throw error;
  }
}

/** An operation-scoped projection. Its SQLite database is never persisted or canonical. */
export class ContextOperation {
  constructor(
    public local: Session,
    readonly head: Head,
    readonly conversationId: number,
  ) {}
  assemble(input: Parameters<ContextAssembler["assemble"]>[0]) {
    this.check(input.conversationId);
    return new ContextAssembler(
      this.local.conversations,
      this.local.summaries,
      "UTC",
    ).assemble(input);
  }
  check(id: number) {
    if (id !== this.conversationId)
      throw new Error("Conversation outside authority");
  }
  close() {
    this.local.db.close();
  }
}

export type TurbopufferStorageOptions = {
  backend: "turbopuffer";
  namespacePrefix: string;
  organizationId: string;
  agentId: string;
  conversationId: number;
  apiKey?: string;
  region?: string;
  transport?: DocumentTransport;
  maxRows?: number;
  maxBytes?: number;
};

/** Experimental canonical remote store for context operations; no implicit config activation. */
export class TurbopufferContextStorage {
  readonly transport: DocumentTransport;
  readonly namespace: string;
  readonly conversationId: number;
  #root: string;
  #maxRows: number;
  #maxBytes: number;
  #agentId: string;
  constructor(options: TurbopufferStorageOptions) {
    if (
      !options.organizationId ||
      !options.agentId ||
      !Number.isSafeInteger(options.conversationId) ||
      options.conversationId < 1
    )
      throw new Error(
        "Explicit organization, native agent and conversation authority required",
      );
    if (!/^[A-Za-z0-9_.-]{1,50}$/.test(options.namespacePrefix))
      throw new Error("Invalid namespace prefix");
    this.namespace = `${options.namespacePrefix}-${hash([options.organizationId, options.agentId]).slice(0, 24)}`;
    this.conversationId = options.conversationId;
    this.#agentId = options.agentId;
    this.#root = `h:${hash(options.conversationId)}`;
    this.#maxRows = options.maxRows ?? 10000;
    this.#maxBytes = options.maxBytes ?? 16 * 1024 * 1024;
    if (
      !Number.isSafeInteger(this.#maxRows) ||
      this.#maxRows < 1 ||
      this.#maxRows > 10000 ||
      !Number.isSafeInteger(this.#maxBytes) ||
      this.#maxBytes < 1 ||
      this.#maxBytes > 16 * 1024 * 1024
    )
      throw new Error("Invalid operation snapshot limits");
    if (!options.transport && !options.apiKey)
      throw new Error("Turbopuffer credential required");
    this.transport =
      options.transport ??
      new TurbopufferTransport({
        apiKey: options.apiKey!,
        namespace: this.namespace,
        region: options.region,
      });
  }
  async initialize() {
    await this.transport.write({
      upsert_rows: [
        {
          id: this.#root,
          kind: "head",
          conversation: String(this.conversationId),
          payload: JSON.stringify({ blocks: [], receipts: {}, parent: "" }),
          version: 0,
          commit: "",
        },
      ],
      upsert_condition: ["id", "Eq", null],
    });
  }
  async head(): Promise<Head> {
    const [row] = await this.transport.read([this.#root]);
    if (
      !row ||
      row.kind !== "head" ||
      row.conversation !== String(this.conversationId)
    )
      throw new Error("Conversation head missing");
    const manifest: Manifest = JSON.parse(row.payload);
    if (
      row.commit &&
      row.commit !== `c:${hash([this.conversationId, manifest])}`
    )
      throw new Error("Manifest hash mismatch");
    return { revision: row.version!, commit: row.commit!, manifest };
  }
  async snapshot(): Promise<ContextOperation> {
    const head = await this.head();
    if (head.manifest.blocks.length > 512)
      throw new Error("Manifest block limit exceeded");
    const blocks = await this.transport.read(head.manifest.blocks);
    if (blocks.length !== head.manifest.blocks.length)
      throw new Error("Committed block missing");
    const data: ConversationTables = {};
    let count = 0,
      bytes = 0;
    const ordered = new Map(blocks.map((row) => [row.id, row]));
    for (const id of head.manifest.blocks) {
      const row = ordered.get(id)!;
      if (
        row.kind !== "block" ||
        row.conversation !== String(this.conversationId) ||
        row.id !== `b:${hash([this.conversationId, row.payload])}`
      )
        throw new Error("Block hash/authority mismatch");
      bytes += Buffer.byteLength(row.payload);
      const block: { table: string; rows: Row[] } = JSON.parse(row.payload);
      count += block.rows.length;
      if (count > this.#maxRows || bytes > this.#maxBytes)
        throw new Error("Snapshot limit exceeded; no partial assembly");
      (data[block.table] ??= []).push(...block.rows);
    }
    const local = hydrate(data);
    const owner = await local.conversations.getConversation(
      this.conversationId,
    );
    if (!owner && head.revision !== 0) {
      local.db.close();
      throw new Error("Committed conversation missing");
    }
    return new ContextOperation(local, head, this.conversationId);
  }
  async #publish<T>(
    operation: ContextOperation,
    token: string,
    digest: string,
    result: T,
  ): Promise<T> {
    const exported = tables(operation.local.db);
    const documents: Document[] = [];
    let count = 0,
      bytes = 0;
    for (const [name, rows] of Object.entries(exported)) {
      count += rows.length;
      for (let at = 0; at < rows.length; at += 32) {
        const payload = JSON.stringify({
          table: name,
          rows: rows.slice(at, at + 32),
        });
        bytes += Buffer.byteLength(payload);
        documents.push({
          id: `b:${hash([this.conversationId, payload])}`,
          kind: "block",
          conversation: String(this.conversationId),
          payload,
        });
      }
    }
    if (count > this.#maxRows || bytes > this.#maxBytes)
      throw new Error("Snapshot limit exceeded; history retained");
    const manifest: Manifest = {
      blocks: documents.map((row) => row.id),
      parent: operation.head.commit,
      receipts: {
        ...operation.head.manifest.receipts,
        [token]: { digest, result: pack(result) },
      },
    };
    if (
      manifest.blocks.length > 512 ||
      Object.keys(manifest.receipts).length > 512 ||
      Buffer.byteLength(JSON.stringify(manifest)) > 256 * 1024
    )
      throw new Error("Manifest/receipt limit exceeded; history retained");
    const payload = JSON.stringify(manifest);
    const commit = `c:${hash([this.conversationId, manifest])}`;
    const old = new Set(operation.head.manifest.blocks);
    await this.transport.write({
      upsert_rows: [
        ...documents.filter((row) => !old.has(row.id)),
        {
          id: commit,
          kind: "commit",
          conversation: String(this.conversationId),
          payload,
        },
      ],
      upsert_condition: ["id", "Eq", null],
    });
    let affected: number;
    try {
      affected = await this.transport.write({
        patch_rows: [
          {
            id: this.#root,
            payload,
            commit,
            version: operation.head.revision + 1,
          },
        ],
        patch_condition: [
          "And",
          [
            ["version", "Eq", operation.head.revision],
            ["commit", "Eq", operation.head.commit],
          ],
        ],
      });
    } catch {
      const current = await this.head();
      if (current.manifest.receipts[token]?.digest === digest)
        return unpack(current.manifest.receipts[token].result) as T;
      if (current.revision === operation.head.revision)
        throw new UnknownCommit(
          "Commit may still be in flight; reconcile before retry",
        );
      throw new StorageConflict("Another conversation writer committed");
    }
    if (affected !== 1) {
      const current = await this.head();
      if (current.manifest.receipts[token]?.digest !== digest)
        throw new StorageConflict("Another conversation writer committed");
      return unpack(current.manifest.receipts[token].result) as T;
    }
    return result;
  }
  async mutate<T>(
    token: string,
    request: unknown,
    run: (operation: ContextOperation) => Promise<T>,
  ): Promise<T> {
    if (!token || token.length > 128)
      throw new Error("Bounded stable operation token required");
    const digest = hash(request);
    const operation = await this.snapshot();
    try {
      const receipt = operation.head.manifest.receipts[token];
      if (receipt) {
        if (receipt.digest !== digest)
          throw new StorageConflict(
            "Idempotency token reused with different input",
          );
        return unpack(receipt.result) as T;
      }
      const result = await run(operation);
      return await this.#publish(operation, token, digest, result);
    } finally {
      operation.close();
    }
  }
  async importTables(data: ConversationTables, token = "initial-import") {
    return this.mutate(token, { type: "import", data }, async (operation) => {
      if (operation.head.revision !== 0)
        throw new Error("Import requires an empty conversation");
      const messages = new Set(
        (data.messages ?? []).map((row) => row.message_id),
      );
      const summaries = new Set(
        (data.summaries ?? []).map((row) => row.summary_id),
      );
      for (const [name, rows] of Object.entries(data)) {
        for (const row of rows) {
          let owned = false;
          if (row.conversation_id != null) {
            if (row.conversation_id !== this.conversationId)
              throw new Error("Import crossed conversation authority");
            owned = true;
          }
          if (row.message_id != null && name !== "messages") {
            if (!messages.has(row.message_id))
              throw new Error("Import crossed message authority");
            owned = true;
          }
          if (row.summary_id != null && name !== "summaries") {
            if (!summaries.has(row.summary_id))
              throw new Error("Import crossed summary authority");
            owned = true;
          }
          if (
            row.parent_summary_id != null &&
            !summaries.has(row.parent_summary_id)
          )
            throw new Error("Import crossed summary source authority");
          if (!owned)
            throw new Error("Import row has no supported authority link");
        }
      }
      const local = hydrate(data);
      try {
        const owner = await local.conversations.getConversation(
          this.conversationId,
        );
        if (
          !owner?.sessionKey?.startsWith(`agent:${this.#agentId}:`) ||
          local.db.prepare("SELECT COUNT(*) AS n FROM conversations").get()
            ?.n !== 1
        )
          throw new Error("Import crossed native agent authority");
        for (const row of local.db
          .prepare(
            "SELECT conversation_id FROM messages UNION SELECT conversation_id FROM summaries UNION SELECT conversation_id FROM context_items",
          )
          .all())
          if (row.conversation_id !== this.conversationId)
            throw new Error("Import crossed conversation authority");
        operation.local.db.close();
        operation.local = local;
      } catch (error) {
        local.db.close();
        throw error;
      }
      return { imported: true };
    });
  }
  async assemble(input: Parameters<ContextAssembler["assemble"]>[0]) {
    const operation = await this.snapshot();
    try {
      return await operation.assemble(input);
    } finally {
      operation.close();
    }
  }
  async compactLeaf(
    token: string,
    config: ConstructorParameters<typeof CompactionEngine>[2],
    input: Parameters<CompactionEngine["compactLeaf"]>[0],
  ) {
    return this.mutate(
      token,
      { type: "compactLeaf", ...input, summarize: undefined, config },
      async (operation) => {
        operation.check(input.conversationId);
        return new CompactionEngine(
          operation.local.conversations,
          operation.local.summaries,
          config,
        ).compactLeaf(input);
      },
    );
  }
  async appendTurn(
    token: string,
    messages: Omit<CreateMessageInput, "conversationId" | "seq">[],
  ) {
    return this.mutate(
      token,
      { type: "appendTurn", messages },
      async (operation) => {
        const { conversations, summaries } = operation.local;
        return conversations.withTransaction(async () => {
          const max = await conversations.getMaxSeq(this.conversationId);
          const added = await conversations.createMessagesBulk(
            messages.map((message, index) => ({
              ...message,
              conversationId: this.conversationId,
              seq: max + index + 1,
            })),
          );
          await summaries.appendContextMessages(
            this.conversationId,
            added.map((message) => message.messageId),
          );
          return added;
        });
      },
    );
  }
  async searchMessages(
    input: Parameters<ConversationStore["searchMessages"]>[0],
  ) {
    const operation = await this.snapshot();
    try {
      operation.check(input.conversationId ?? this.conversationId);
      if (input.conversationIds?.some((id) => id !== this.conversationId))
        throw new Error("Conversation outside authority");
      return await operation.local.conversations.searchMessages({
        ...input,
        conversationId: this.conversationId,
      });
    } finally {
      operation.close();
    }
  }
}

/** Existing engine stays SQLite by default. Opt-in consumers import this factory. */
export function createContextStorage(options?: {
  backend?: "sqlite";
  db: DatabaseSync;
}): {
  backend: "sqlite";
  conversations: ConversationStore;
  summaries: SummaryStore;
};
export function createContextStorage(
  options: TurbopufferStorageOptions,
): TurbopufferContextStorage;
export function createContextStorage(
  options?:
    { backend?: "sqlite"; db: DatabaseSync } | TurbopufferStorageOptions,
) {
  if (options?.backend === "turbopuffer")
    return new TurbopufferContextStorage(options);
  if (!options?.db)
    throw new Error("SQLite DatabaseSync required for default backend");
  return {
    backend: "sqlite" as const,
    conversations: new ConversationStore(options.db),
    summaries: new SummaryStore(options.db),
  };
}
