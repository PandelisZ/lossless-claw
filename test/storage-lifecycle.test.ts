import { expect, it } from "vitest";
import { storageLifecycle } from "../src/plugin/storage-lifecycle.js";
it("keeps the final stop intent when restart overlaps an unfinished shutdown", async () => {
  let stopped = false,
    starts = 0,
    release!: () => void;
  const gate = new Promise<void>((resolve) => {
    release = resolve;
  });
  const lifecycle = storageLifecycle({
    stopped: () => stopped,
    setStopped: (value) => {
      stopped = value;
    },
    close: async () => {
      await gate;
    },
    restart: async () => {
      starts++;
    },
  });
  const first = lifecycle.close(),
    restart = lifecycle.restart(),
    final = lifecycle.close();
  release();
  await Promise.all([first, restart, final]);
  expect(stopped).toBe(true);
  expect(starts).toBe(0);
  await lifecycle.restart();
  expect(stopped).toBe(false);
  expect(starts).toBe(1);
});
it("closes resources reopened during an async restart if a later stop supersedes it", async () => {
  let stopped = true,
    closed = 0,
    started!: () => void,
    release!: () => void;
  const entered = new Promise<void>((resolve) => {
      started = resolve;
    }),
    gate = new Promise<void>((resolve) => {
      release = resolve;
    });
  const lifecycle = storageLifecycle({
    stopped: () => stopped,
    setStopped: (value) => {
      stopped = value;
    },
    close: async () => {
      closed++;
    },
    restart: async () => {
      started();
      await gate;
    },
  });
  const restart = lifecycle.restart();
  await entered;
  const stop = lifecycle.close();
  release();
  await Promise.all([restart, stop]);
  expect(stopped).toBe(true);
  expect(closed).toBeGreaterThan(0);
});
