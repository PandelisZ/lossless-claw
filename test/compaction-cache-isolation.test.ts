import { afterEach, expect, it } from "vitest";
import {
  cleanupEngineTestState,
  createEngineWithConfig,
  seedBacklogContext,
} from "./helpers.js";
afterEach(cleanupEngineTestState);

it("does not reuse a completed conversation's cached context while another compaction waits on inference", async () => {
  const engine = createEngineWithConfig({
    freshTailCount: 1,
    leafMinFanout: 2,
    leafChunkTokens: 240,
  });
  await seedBacklogContext(engine, "held-compaction", [500, 500, 500]);
  const conversations = engine.getConversationStore(),
    compaction = engine.getCompactionEngine();
  const heldConversation = (await conversations.getConversationBySessionId(
    "held-compaction"
  ))!;
  const changing = await conversations.getOrCreateConversation(
    "changing-context"
  );
  let entered!: () => void, release!: () => void;
  const atInference = new Promise<void>((resolve) => {
      entered = resolve;
    }),
    gate = new Promise<void>((resolve) => {
      release = resolve;
    });
  const held = compaction.compactLeaf({
    conversationId: heldConversation.conversationId,
    tokenBudget: 100,
    force: true,
    summarize: async () => {
      entered();
      await gate;
      return "held synthetic summary";
    },
  });
  try {
    await atInference;
    expect(
      (
        await compaction.compactLeaf({
          conversationId: changing.conversationId,
          tokenBudget: 100,
          force: true,
          summarize: async () => "unused",
        })
      ).actionTaken
    ).toBe(false);
    await seedBacklogContext(engine, "changing-context", [500, 500, 500]);
    const next = await compaction.compactLeaf({
      conversationId: changing.conversationId,
      tokenBudget: 100,
      force: true,
      summarize: async () => "fresh context synthetic summary",
    });
    expect(next.actionTaken).toBe(true);
    expect(await conversations.getMessageCount(changing.conversationId)).toBe(
      3
    );
  } finally {
    release();
    await held;
  }
});
