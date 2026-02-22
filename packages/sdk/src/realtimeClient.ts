import type { DocumentRecord, NewDocumentRecord } from "./client";

type SyncClientMessage =
  | { type: "subscribe"; requestId: string; table: string }
  | { type: "unsubscribe"; requestId: string; table: string }
  | { type: "insert"; requestId: string; table: string; value: NewDocumentRecord }
  | { type: "delete"; requestId: string; table: string; id: string }
  | { type: "get"; requestId: string; table: string; id: string }
  | { type: "list"; requestId: string; table: string }
  | { type: "ping"; requestId: string; sentAt: number };

type SyncServerMessage =
  | { type: "ack"; requestId: string; event: "subscribe" | "unsubscribe"; ok: true }
  | { type: "result"; requestId: string; op: string; ok: true; payload: unknown }
  | { type: "snapshot"; table: string; documents: DocumentRecord[] }
  | {
      type: "change";
      table: string;
      op: "insert" | "delete";
      document?: DocumentRecord;
      id?: string;
    }
  | { type: "pong"; requestId: string; sentAt: number; receivedAt: number }
  | { type: "error"; requestId: string; ok: false; code: string; message: string };

export interface RealtimeTransport {
  send(payload: string): void;
  close(): void;
  onMessage(handler: (payload: string) => void): void;
}

export type RealtimeTransportFactory = () => RealtimeTransport;

type TableListener = (event: { type: "snapshot" | "change"; data: SyncServerMessage }) => void;

export class RealtimeClient {
  private readonly transport: RealtimeTransport;
  private readonly listeners = new Map<string, Set<TableListener>>();
  private requestCounter = 0;

  constructor(factory: RealtimeTransportFactory) {
    this.transport = factory();
    this.transport.onMessage((raw) => this.handleIncoming(raw));
  }

  subscribe(table: string, listener: TableListener): () => void {
    const tableListeners = this.listeners.get(table) ?? new Set<TableListener>();
    tableListeners.add(listener);
    this.listeners.set(table, tableListeners);

    this.send({
      type: "subscribe",
      requestId: this.nextRequestId(),
      table,
    });

    return () => {
      const current = this.listeners.get(table);
      current?.delete(listener);
      if (current && current.size === 0) {
        this.listeners.delete(table);
        this.send({
          type: "unsubscribe",
          requestId: this.nextRequestId(),
          table,
        });
      }
    };
  }

  insert(table: string, value: NewDocumentRecord): void {
    this.send({
      type: "insert",
      requestId: this.nextRequestId(),
      table,
      value,
    });
  }

  close(): void {
    this.transport.close();
  }

  private handleIncoming(raw: string): void {
    const message = JSON.parse(raw) as SyncServerMessage;
    if (message.type !== "snapshot" && message.type !== "change") {
      return;
    }

    const tableListeners = this.listeners.get(message.table);
    if (!tableListeners) {
      return;
    }

    for (const listener of tableListeners) {
      listener({ type: message.type, data: message });
    }
  }

  private send(message: SyncClientMessage): void {
    this.transport.send(JSON.stringify(message));
  }

  private nextRequestId(): string {
    this.requestCounter += 1;
    return `req_${this.requestCounter}`;
  }
}

export class BrowserWebSocketTransport implements RealtimeTransport {
  private handler: ((payload: string) => void) | null = null;
  private readonly ws: WebSocket;

  constructor(url: string) {
    this.ws = new WebSocket(url);
    this.ws.onmessage = (event) => {
      if (typeof event.data === "string" && this.handler) {
        this.handler(event.data);
      }
    };
  }

  send(payload: string): void {
    this.ws.send(payload);
  }

  close(): void {
    this.ws.close();
  }

  onMessage(handler: (payload: string) => void): void {
    this.handler = handler;
  }
}
