/** Serialize resource teardown/startup only; later shutdown intent always wins. */
export function storageLifecycle(options: {
  stopped: () => boolean;
  setStopped: (value: boolean) => void;
  close: () => Promise<void>;
  restart: () => Promise<void>;
}) {
  let generation = 0,
    tail = Promise.resolve();
  const enqueue = (operation: () => Promise<void>) => {
    const result = tail.then(operation);
    tail = result.catch(() => {});
    return result;
  };
  return {
    close() {
      generation++;
      options.setStopped(true);
      return enqueue(async () => {
        options.setStopped(true);
        await options.close();
      });
    },
    restart() {
      const requested = generation;
      return enqueue(async () => {
        if (requested !== generation || !options.stopped()) return;
        await options.restart();
        if (requested === generation) options.setStopped(false);
        else {
          options.setStopped(true);
          await options.close();
        }
      });
    },
  };
}
