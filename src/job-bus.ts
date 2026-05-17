/**
 * FORK PATCH (rostchri): Module-level singletons for serial submission queue,
 * idempotency dedup, and progress event bus.
 *
 * Why:
 * - ComfyUI is single-GPU, so feeding it more than one prompt at a time only
 *   bloats its internal queue and lets clients race on the WebSocket. We
 *   serialize at the wrapper boundary instead.
 * - HTTP clients retry on timeout; without dedup that means duplicate renders
 *   for a single user click. Idempotency-Key (passed as request.body.id)
 *   collapses retries onto the same in-flight promise.
 * - The wrapper already subscribes to ComfyUI's WS stream; we tee
 *   progress/executing/executed/success/error events to an in-process
 *   EventEmitter so an SSE route can fan them out to UI clients.
 */
import { EventEmitter } from "events";
import { ComfyWSMessage } from "./types";

/** Re-emitted ComfyUI WS messages, keyed by event kind. */
export const progressBus = new EventEmitter();
progressBus.setMaxListeners(100);

/** Type of events relayed via `progressBus`. */
export type ProgressEvent =
  | { kind: "execution_start"; apiId?: string; comfyId: string }
  | { kind: "executing"; apiId?: string; comfyId: string; node?: string | null }
  | {
      kind: "progress";
      apiId?: string;
      comfyId: string;
      value: number;
      max: number;
      node?: string | null;
    }
  | { kind: "executed"; apiId?: string; comfyId: string; node: string }
  | { kind: "execution_success"; apiId?: string; comfyId: string }
  | { kind: "execution_error"; apiId?: string; comfyId: string; error?: any }
  | { kind: "queue_status"; running: number; pending: number }
  | {
      kind: "prompt_meta";
      apiId: string;
      /** node-id → display title (`_meta.title` or class_type as fallback) */
      nodes: Record<string, string>;
    };

export function emitProgress(ev: ProgressEvent): void {
  progressBus.emit("event", ev);
  if (ev.kind !== "queue_status" && ev.apiId) {
    progressBus.emit(`id:${ev.apiId}`, ev);
  }
}

/**
 * Single-flight FIFO queue. Only one `task()` runs at a time across the
 * process. Tasks are awaited in submission order.
 */
class SubmissionQueue {
  private queue: Array<() => Promise<void>> = [];
  private running = false;
  get depth(): number {
    return this.queue.length;
  }
  run<T>(task: () => Promise<T>): Promise<T> {
    return new Promise<T>((resolve, reject) => {
      this.queue.push(async () => {
        try {
          resolve(await task());
        } catch (e) {
          reject(e);
        }
      });
      this.drain();
    });
  }
  private async drain(): Promise<void> {
    if (this.running) return;
    this.running = true;
    try {
      while (this.queue.length > 0) {
        const next = this.queue.shift()!;
        await next();
      }
    } finally {
      this.running = false;
    }
  }
}

export const submissionQueue = new SubmissionQueue();

/**
 * Idempotency map: request.body.id → in-flight Promise of the final response
 * payload. Retries (same id) bypass the queue and await the original promise.
 * Entries are GC-ed 60 s after settlement so genuine reuse of an id later
 * (e.g. after a worker restart) still goes through.
 */
const inflight = new Map<string, Promise<any>>();

export function getInflight(id: string): Promise<any> | undefined {
  return inflight.get(id);
}

/**
 * Per-request map of node-id → display title. Populated once at submission
 * time (after preprocessing). SSE clients that subscribe AFTER the
 * `prompt_meta` event fires get a replay from this cache on connect. Entries
 * are evicted after 10 min so a long-lived process doesn't grow unbounded.
 */
const promptMetaStore = new Map<string, Record<string, string>>();

export function setPromptMeta(
  apiId: string,
  nodes: Record<string, string>
): void {
  promptMetaStore.set(apiId, nodes);
  emitProgress({ kind: "prompt_meta", apiId, nodes });
  const t = setTimeout(() => {
    promptMetaStore.delete(apiId);
  }, 600_000);
  t.unref?.();
}

export function getPromptMeta(apiId: string): Record<string, string> | undefined {
  return promptMetaStore.get(apiId);
}

export function setInflight<T>(id: string, p: Promise<T>): void {
  inflight.set(id, p);
  const cleanup = (): void => {
    const t = setTimeout(() => {
      if (inflight.get(id) === p) inflight.delete(id);
    }, 60_000);
    t.unref?.();
  };
  p.then(cleanup, cleanup);
}

/**
 * Lightweight message-kind dispatcher, called from the central WS handler
 * once per ComfyUI WS frame. The `message.data.prompt_id` here has already
 * been mapped back to the API id by `connectToComfyUIWebsocketStream`
 * (useApiIDs=true).
 */
export function relayWsMessage(message: ComfyWSMessage): void {
  const t = message.type;
  const apiId: string | undefined = message?.data?.prompt_id;
  if (t === "execution_start") {
    if (apiId)
      emitProgress({ kind: "execution_start", apiId, comfyId: apiId });
  } else if (t === "executing") {
    if (apiId)
      emitProgress({
        kind: "executing",
        apiId,
        comfyId: apiId,
        node: (message.data as any)?.node ?? null,
      });
  } else if (t === "progress") {
    if (apiId)
      emitProgress({
        kind: "progress",
        apiId,
        comfyId: apiId,
        value: (message.data as any)?.value ?? 0,
        max: (message.data as any)?.max ?? 0,
        node: (message.data as any)?.node ?? null,
      });
  } else if (t === "executed") {
    if (apiId)
      emitProgress({
        kind: "executed",
        apiId,
        comfyId: apiId,
        node: (message.data as any)?.node ?? "",
      });
  } else if (t === "execution_success") {
    if (apiId)
      emitProgress({ kind: "execution_success", apiId, comfyId: apiId });
  } else if (t === "execution_error") {
    if (apiId)
      emitProgress({
        kind: "execution_error",
        apiId,
        comfyId: apiId,
        error: message.data,
      });
  } else if (t === "status") {
    const exec = (message.data as any)?.status?.exec_info;
    if (exec) {
      emitProgress({
        kind: "queue_status",
        running: 0,
        pending: exec.queue_remaining ?? 0,
      });
    }
  }
}
