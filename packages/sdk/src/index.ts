export type JsonValue =
  | string
  | number
  | boolean
  | null
  | { [key: string]: JsonValue }
  | JsonValue[];

export type SchemaField = {
  required: boolean;
  type: "string" | "number" | "boolean" | "object" | "array" | "null";
};

export type CollectionSchema = Record<string, SchemaField>;
