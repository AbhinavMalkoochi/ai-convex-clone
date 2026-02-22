export type JsonValue =
  | string
  | number
  | boolean
  | null
  | { [key: string]: JsonValue }
  | JsonValue[];

export type FieldType = "string" | "number" | "boolean" | "object" | "array" | "null";

export type SchemaField = {
  required: boolean;
  type: FieldType;
};

export type CollectionSchema = Record<string, SchemaField>;
export type DatabaseSchema = Record<string, CollectionSchema>;

type FieldOptions = {
  required?: boolean;
};

const createField = (type: FieldType, options: FieldOptions = {}): SchemaField => ({
  required: options.required ?? true,
  type,
});

export const s = {
  string: (options?: FieldOptions): SchemaField => createField("string", options),
  number: (options?: FieldOptions): SchemaField => createField("number", options),
  boolean: (options?: FieldOptions): SchemaField => createField("boolean", options),
  object: (options?: FieldOptions): SchemaField => createField("object", options),
  array: (options?: FieldOptions): SchemaField => createField("array", options),
  null: (options?: FieldOptions): SchemaField => createField("null", options),
} as const;

export const defineCollection = <T extends CollectionSchema>(schema: T): T => schema;

export const defineSchema = <T extends DatabaseSchema>(schema: T): T => schema;

export const toSchemaJson = (schema: DatabaseSchema): string => JSON.stringify(schema);

export const fromSchemaJson = (payload: string): DatabaseSchema => {
  const parsed = JSON.parse(payload) as DatabaseSchema;
  return parsed;
};

export { ConvexLikeClient, InMemoryStorageAdapter } from "./client";
export type { DocumentRecord, NewDocumentRecord, StorageAdapter, WriteOperation } from "./client";
export { BrowserWebSocketTransport, RealtimeClient } from "./realtimeClient";
export type { RealtimeTransport, RealtimeTransportFactory } from "./realtimeClient";
