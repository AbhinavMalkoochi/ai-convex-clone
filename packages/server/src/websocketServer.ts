import type { DatabaseSchema } from "@acx/sdk";

import { encodeServerMessage, parseClientMessage } from "./protocol";
import { type SessionId, SyncEngine } from "./syncEngine";

export type SyncWebSocketServerOptions = {
  port: number;
  schema: DatabaseSchema;
};

export class SyncWebSocketServer {
  private readonly engine: SyncEngine;
  private server: Bun.Server | null = null;

  constructor(private readonly options: SyncWebSocketServerOptions) {
    this.engine = new SyncEngine(options.schema);
  }

  async start(): Promise<void> {
    await this.engine.init();

    this.server = Bun.serve<{ sessionId: SessionId }>({
      port: this.options.port,
      fetch(request, server) {
        const upgraded = server.upgrade(request, {
          data: { sessionId: crypto.randomUUID() },
        });
        if (upgraded) {
          return undefined;
        }

        return new Response("websocket upgrade required", { status: 426 });
      },
      websocket: {
        open: (ws) => {
          this.engine.registerSession(ws.data.sessionId);
        },
        message: async (ws, payload) => {
          const send = (encoded: string) => {
            ws.send(encoded);
          };

          const raw = typeof payload === "string" ? payload : Buffer.from(payload).toString("utf8");
          await handleIncomingMessage(this.engine, ws.data.sessionId, raw, send);
        },
        close: (ws) => {
          this.engine.unregisterSession(ws.data.sessionId);
        },
      },
    });
  }

  stop(): void {
    this.server?.stop();
    this.server = null;
  }
}

export const handleIncomingMessage = async (
  engine: SyncEngine,
  sessionId: SessionId,
  raw: string,
  send: (encoded: string) => void,
): Promise<void> => {
  try {
    const incoming = parseClientMessage(raw);
    const outbound = await engine.process(sessionId, incoming);
    for (const item of outbound) {
      if (item.sessionId === sessionId) {
        send(encodeServerMessage(item.message));
      }
    }
  } catch {
    send(
      encodeServerMessage({
        type: "error",
        requestId: "unknown",
        ok: false,
        code: "BAD_REQUEST",
        message: "invalid message payload",
      }),
    );
  }
};
