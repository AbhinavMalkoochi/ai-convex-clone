import {
  ConvexLikeClient,
  type DatabaseSchema,
  type DocumentRecord,
  InMemoryStorageAdapter,
  type StorageAdapter,
} from "@acx/sdk";

import type { SyncClientMessage, SyncServerMessage } from "./protocol";

export type SessionId = string;

export type OutboundMessage = {
  sessionId: SessionId;
  message: SyncServerMessage;
};

export class SyncEngine {
  private readonly subscriptions = new Map<string, Set<SessionId>>();
  private readonly sessions = new Set<SessionId>();
  private readonly client: ConvexLikeClient;

  constructor(
    schema: DatabaseSchema,
    private readonly storage: StorageAdapter = new InMemoryStorageAdapter(),
  ) {
    this.client = new ConvexLikeClient(schema, this.storage);
  }

  async init(): Promise<void> {
    await this.client.init();
  }

  registerSession(sessionId: SessionId): void {
    this.sessions.add(sessionId);
  }

  unregisterSession(sessionId: SessionId): void {
    this.sessions.delete(sessionId);
    for (const [, subscribers] of this.subscriptions) {
      subscribers.delete(sessionId);
    }
  }

  async process(sessionId: SessionId, message: SyncClientMessage): Promise<OutboundMessage[]> {
    if (!this.sessions.has(sessionId)) {
      return [
        {
          sessionId,
          message: {
            type: "error",
            requestId: message.requestId,
            ok: false,
            code: "BAD_REQUEST",
            message: "session is not registered",
          },
        },
      ];
    }

    try {
      switch (message.type) {
        case "subscribe": {
          const subscribers = this.subscriptions.get(message.table) ?? new Set<SessionId>();
          subscribers.add(sessionId);
          this.subscriptions.set(message.table, subscribers);

          const documents = await this.client.list(message.table);
          return [
            {
              sessionId,
              message: {
                type: "ack",
                requestId: message.requestId,
                event: "subscribe",
                ok: true,
              },
            },
            {
              sessionId,
              message: {
                type: "snapshot",
                table: message.table,
                documents,
              },
            },
          ];
        }
        case "unsubscribe": {
          const subscribers = this.subscriptions.get(message.table);
          subscribers?.delete(sessionId);
          return [
            {
              sessionId,
              message: {
                type: "ack",
                requestId: message.requestId,
                event: "unsubscribe",
                ok: true,
              },
            },
          ];
        }
        case "insert": {
          const inserted = await this.client.insert(message.table, message.value);
          const out: OutboundMessage[] = [
            {
              sessionId,
              message: {
                type: "result",
                requestId: message.requestId,
                op: "insert",
                ok: true,
                payload: inserted,
              },
            },
          ];

          out.push(...this.broadcastInsert(message.table, inserted));
          return out;
        }
        case "delete": {
          await this.storage.writeBatch(message.table, [{ kind: "delete", id: message.id }]);
          const out: OutboundMessage[] = [
            {
              sessionId,
              message: {
                type: "result",
                requestId: message.requestId,
                op: "delete",
                ok: true,
                payload: { id: message.id },
              },
            },
          ];

          out.push(...this.broadcastDelete(message.table, message.id));
          return out;
        }
        case "get": {
          const doc = await this.client.get(message.table, message.id);
          return [
            {
              sessionId,
              message: {
                type: "result",
                requestId: message.requestId,
                op: "get",
                ok: true,
                payload: doc,
              },
            },
          ];
        }
        case "list": {
          const docs = await this.client.list(message.table);
          return [
            {
              sessionId,
              message: {
                type: "result",
                requestId: message.requestId,
                op: "list",
                ok: true,
                payload: docs,
              },
            },
          ];
        }
        case "ping": {
          return [
            {
              sessionId,
              message: {
                type: "pong",
                requestId: message.requestId,
                sentAt: message.sentAt,
                receivedAt: Date.now(),
              },
            },
          ];
        }
      }
    } catch (error) {
      const messageText = error instanceof Error ? error.message : "internal error";
      return [
        {
          sessionId,
          message: {
            type: "error",
            requestId: message.requestId,
            ok: false,
            code: classifyError(messageText),
            message: messageText,
          },
        },
      ];
    }
  }

  private broadcastInsert(table: string, document: DocumentRecord): OutboundMessage[] {
    return this.broadcast(table, {
      type: "change",
      table,
      op: "insert",
      document,
    });
  }

  private broadcastDelete(table: string, id: string): OutboundMessage[] {
    return this.broadcast(table, {
      type: "change",
      table,
      op: "delete",
      id,
    });
  }

  private broadcast(table: string, message: SyncServerMessage): OutboundMessage[] {
    const subscribers = this.subscriptions.get(table);
    if (!subscribers) {
      return [];
    }

    const outbound: OutboundMessage[] = [];
    for (const sessionId of subscribers) {
      if (this.sessions.has(sessionId)) {
        outbound.push({ sessionId, message });
      }
    }
    return outbound;
  }
}

const classifyError = (
  errorMessage: string,
): "BAD_REQUEST" | "NOT_FOUND" | "SCHEMA_VIOLATION" | "INTERNAL" => {
  if (errorMessage.includes("not found")) {
    return "NOT_FOUND";
  }
  if (errorMessage.includes("schema violation")) {
    return "SCHEMA_VIOLATION";
  }
  if (errorMessage.includes("table already exists") || errorMessage.includes("session")) {
    return "BAD_REQUEST";
  }
  return "INTERNAL";
};
