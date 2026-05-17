/**
 * FORK PATCH (rostchri): SSE endpoint that relays per-prompt ComfyUI WS
 * events to browser/UI clients.
 *
 * Usage from a client:
 *   const es = new EventSource('/progress/<apiId>');
 *   es.addEventListener('progress', (e) => { ... });
 *   es.addEventListener('done', () => es.close());
 *
 * The stream closes automatically on execution_success or execution_error
 * for the requested apiId; the client should also close it if the POST
 * /prompt response arrives first.
 */
import { FastifyInstance } from "fastify";
import { progressBus, ProgressEvent, getPromptMeta } from "./job-bus";
import config from "./config";
import { fetch } from "undici";
import { getProxyDispatcher } from "./proxy-dispatcher";

export function registerProgressRoute(app: FastifyInstance): void {
  app.get<{ Params: { apiId: string } }>(
    "/progress/:apiId",
    async (request, reply) => {
      const { apiId } = request.params;
      const log = app.log.child({ apiId, route: "/progress" });
      log.info("SSE progress client subscribed");

      reply.hijack();
      const raw = reply.raw;
      raw.writeHead(200, {
        "Content-Type": "text/event-stream; charset=utf-8",
        "Cache-Control": "no-cache, no-transform",
        Connection: "keep-alive",
        "X-Accel-Buffering": "no",
      });
      raw.write(": connected\n\n");

      const send = (event: string, data: unknown): void => {
        try {
          raw.write(`event: ${event}\n`);
          raw.write(`data: ${JSON.stringify(data)}\n\n`);
        } catch (e) {
          /* socket gone; cleanup runs from close handler */
        }
      };

      // Initial queue snapshot so the UI can show "you are #N in queue"
      // before any WS event fires.
      try {
        const r = await fetch(`${config.comfyURL}/queue`, {
          dispatcher: getProxyDispatcher(),
        });
        if (r.ok) {
          const q = (await r.json()) as any;
          send("queue", {
            running: q.queue_running?.length ?? 0,
            pending: q.queue_pending?.length ?? 0,
          });
        }
      } catch (e: any) {
        log.debug({ err: e.message }, "initial queue snapshot failed");
      }

      // Replay the node-title map if the prompt was already submitted before
      // this client subscribed (likely race: POST /workflow fires the meta
      // event a few ms before EventSource is fully connected). Future
      // subscribers also receive it as a normal `prompt_meta` event.
      const cachedMeta = getPromptMeta(apiId);
      if (cachedMeta) {
        send("prompt_meta", { apiId, nodes: cachedMeta });
      }

      const onEvent = (ev: ProgressEvent): void => {
        if (ev.kind === "queue_status") {
          send("queue", { pending: ev.pending });
          return;
        }
        if (ev.apiId !== apiId) return;
        send(ev.kind, ev);
        if (
          ev.kind === "execution_success" ||
          ev.kind === "execution_error"
        ) {
          send("done", { apiId, status: ev.kind });
          cleanup();
        }
      };

      const heartbeat = setInterval(() => {
        try {
          raw.write(`: hb ${Date.now()}\n\n`);
        } catch {
          clearInterval(heartbeat);
        }
      }, 15_000);

      const cleanup = (): void => {
        clearInterval(heartbeat);
        progressBus.removeListener("event", onEvent);
        try {
          raw.end();
        } catch {
          /* ignore */
        }
      };

      progressBus.on("event", onEvent);
      request.raw.on("close", () => {
        log.info("SSE progress client disconnected");
        cleanup();
      });
    }
  );
}
