// Execute the exact pinned assembler/compactor against asynchronous PG stores.
// No gateway registration: engine bootstrap still has direct SQLite dependencies.
import { CompactionEngine, ContextAssembler } from "./.generated/upstream.mjs";
import { Conflict } from "./database.mjs";
import { operationSnapshot } from "./snapshot.mjs";
const bindProxy = (target, overrides) =>
  new Proxy(target, {
    get(object, key) {
      const v = overrides[key] ?? object[key];
      return typeof v === "function" ? v.bind(object) : v;
    },
  });
export function upstreamAssembler(store) {
  return new ContextAssembler(store, store, "UTC");
}
export function upstreamCompactor(
  store,
  conversationId,
  config,
  { reads = store, onCommitted } = {},
) {
  let version;
  let stage;
  const summary = bindProxy(reads, {
    async getContextItems(cid) {
      if (cid !== conversationId)
        throw new Error("Compaction crossed conversation");
      version = await reads.revision(cid);
      return reads.getContextItems(cid);
    },
    async getDistinctDepthsInContext(cid, options = {}) {
      const items = await reads.getContextItems(cid);
      const records = await Promise.all(
        items
          .filter(
            (i) =>
              i.summaryId &&
              (!Number.isFinite(options.maxOrdinalExclusive) ||
                i.ordinal < Math.floor(options.maxOrdinalExclusive)),
          )
          .map((i) => reads.getSummary(i.summaryId)),
      );
      return [...new Set(records.filter(Boolean).map((r) => r.depth))].sort(
        (a, b) => a - b,
      );
    },
    async withTransaction(operation) {
      if (stage || version == null)
        throw new Error("Compaction requires a fenced context snapshot");
      stage = {};
      try {
        await operation();
        if (!stage.summary || !stage.replacement)
          throw new Error("Incomplete upstream compaction write set");
        await store.commitCompaction({
          conversationId,
          expectedRevision: version,
          summary: stage.summary,
          messageIds: stage.messageIds ?? [],
          sourceSummaryIds: stage.sourceSummaryIds ?? [],
          ...stage.replacement,
        });
        version++;
        onCommitted?.();
      } finally {
        stage = undefined;
      }
    },
    async insertSummary(input) {
      if (!stage || stage.summary)
        throw new Error("Expected one upstream summary per pass");
      stage.summary = input;
      return input;
    },
    async linkSummaryToMessages(id, ids) {
      if (!stage || stage.summary?.summaryId !== id)
        throw new Error("Invalid summary write set");
      stage.messageIds = ids;
    },
    async linkSummaryToParents(id, ids) {
      if (!stage || stage.summary?.summaryId !== id)
        throw new Error("Invalid summary write set");
      stage.sourceSummaryIds = ids;
    },
    async replaceContextRangeWithSummary(input) {
      if (
        !stage ||
        stage.summary?.summaryId !== input.summaryId ||
        input.conversationId !== conversationId
      )
        throw new Conflict("Invalid context write set");
      stage.replacement = {
        startOrdinal: input.startOrdinal,
        endOrdinal: input.endOrdinal,
      };
    },
  });
  return new CompactionEngine(reads, summary, config);
}
export const compactionConfig = {
  contextThreshold: 0.75,
  freshTailCount: 8,
  leafMinFanout: 4,
  condensedMinFanout: 4,
  condensedMinFanoutHard: 2,
  leafChunkTokens: 800,
  leafTargetTokens: 100,
  condensedTargetTokens: 100,
  maxRounds: 1,
  maxSweepIterations: 1,
  summaryMaxOverageFactor: 3,
  timezone: "UTC",
};

// The wrapper has no cache. Even reusing it for a later request loads a new snapshot.
export function batchedUpstreamAssembler(store) {
  return {
    async assemble(input) {
      const snapshot = await operationSnapshot(store, input.conversationId);
      try {
        return await new ContextAssembler(snapshot, snapshot, "UTC").assemble(
          input,
        );
      } finally {
        snapshot.close();
      }
    },
  };
}
export function batchedUpstreamCompactor(store, conversationId, config) {
  const run = async (method, input) => {
    if (input.conversationId !== conversationId)
      throw new Error("Compaction crossed conversation authority");
    let snapshot = await operationSnapshot(store, conversationId);
    let dirty = false;
    // A committed pass invalidates every read. Refresh lazily only if the pinned
    // engine needs another read/pass; no old snapshot can authorize a later write.
    const reads = new Proxy(
      {},
      {
        get(_target, key) {
          return async (...args) => {
            if (dirty) {
              snapshot.close();
              snapshot = await operationSnapshot(store, conversationId);
              dirty = false;
            }
            if (typeof snapshot[key] !== "function")
              throw new Error(`Snapshot does not implement ${String(key)}`);
            return snapshot[key](...args);
          };
        },
      },
    );
    try {
      return await upstreamCompactor(store, conversationId, config, {
        reads,
        onCommitted() {
          dirty = true;
        },
      })[method](input);
    } finally {
      snapshot.close();
    }
  };
  return {
    compactLeaf: (input) => run("compactLeaf", input),
    compactFullSweep: (input) => run("compactFullSweep", input),
  };
}
