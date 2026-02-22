import type { DocumentRecord, JsonValue, NewDocumentRecord } from "@acx/sdk";

export type SyncClientMessage =
  | {
      type: "subscribe";
      requestId: string;
      table: string;
    }
  | {
      type: "unsubscribe";
      requestId: string;
      table: string;
    }
  | {
      type: "insert";
      requestId: string;
      table: string;
      value: NewDocumentRecord;
    }
  | {
      type: "delete";
      requestId: string;
      table: string;
      id: string;
    }
  | {
      type: "get";
      requestId: string;
      table: string;
      id: string;
    }
  | {
      type: "list";
      requestId: string;
      table: string;
    }
  | {
      type: "ping";
      requestId: string;
      sentAt: number;
    };

export type SyncServerMessage =
  | {
      type: "ack";
      requestId: string;
      event: "subscribe" | "unsubscribe";
      ok: true;
    }
  | {
      type: "result";
      requestId: string;
      op: "insert" | "delete" | "get" | "list";
      ok: true;
      payload: JsonValue;
    }
  | {
      type: "snapshot";
      table: string;
      documents: DocumentRecord[];
    }
  | {
      type: "change";
      table: string;
      op: "insert" | "delete";
      document?: DocumentRecord;
      id?: string;
    }
  | {
      type: "pong";
      requestId: string;
      sentAt: number;
      receivedAt: number;
    }
  | {
      type: "error";
      requestId: string;
      ok: false;
      code: "BAD_REQUEST" | "NOT_FOUND" | "SCHEMA_VIOLATION" | "INTERNAL";
      message: string;
      details?: JsonValue;
    };

export const parseClientMessage = (raw: string): SyncClientMessage => {
  const parsed = JSON.parse(raw) as SyncClientMessage;
  return parsed;
};

export const encodeServerMessage = (message: SyncServerMessage): string => {
  return JSON.stringify(message);
};
