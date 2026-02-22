import type { CollectionSchema, DatabaseSchema, FieldType, JsonValue } from "./index";

export type DocumentRecord = {
  id: string;
  revision: number;
  fields: Record<string, JsonValue>;
};

export type NewDocumentRecord = {
  id?: string;
  fields: Record<string, JsonValue>;
};

export type WriteOperation =
  | { kind: "put"; value: NewDocumentRecord }
  | { kind: "delete"; id: string };

export interface StorageAdapter {
  createTable(table: string, schema: CollectionSchema): Promise<void>;
  writeBatch(table: string, ops: WriteOperation[]): Promise<DocumentRecord[]>;
  get(table: string, id: string): Promise<DocumentRecord>;
  list(table: string): Promise<DocumentRecord[]>;
}

export class InMemoryStorageAdapter implements StorageAdapter {
  private readonly tables = new Map<
    string,
    { schema: CollectionSchema; docs: Map<string, DocumentRecord> }
  >();
  private nextRevision = 1;

  async createTable(table: string, schema: CollectionSchema): Promise<void> {
    if (this.tables.has(table)) {
      throw new Error(`table already exists: ${table}`);
    }

    this.tables.set(table, { schema, docs: new Map() });
  }

  async writeBatch(table: string, ops: WriteOperation[]): Promise<DocumentRecord[]> {
    const target = this.tables.get(table);
    if (!target) {
      throw new Error(`table not found: ${table}`);
    }

    const draft = new Map(target.docs);
    const written: DocumentRecord[] = [];

    for (const op of ops) {
      if (op.kind === "put") {
        validateBySchema(target.schema, op.value.fields);
        const id = op.value.id ?? crypto.randomUUID();
        const doc: DocumentRecord = {
          id,
          revision: this.nextRevision++,
          fields: op.value.fields,
        };
        draft.set(id, doc);
        written.push(doc);
        continue;
      }

      const deleted = draft.delete(op.id);
      if (!deleted) {
        throw new Error(`document not found: ${op.id}`);
      }
    }

    target.docs.clear();
    for (const [id, doc] of draft.entries()) {
      target.docs.set(id, doc);
    }

    return written;
  }

  async get(table: string, id: string): Promise<DocumentRecord> {
    const target = this.tables.get(table);
    if (!target) {
      throw new Error(`table not found: ${table}`);
    }

    const doc = target.docs.get(id);
    if (!doc) {
      throw new Error(`document not found: ${id}`);
    }

    return doc;
  }

  async list(table: string): Promise<DocumentRecord[]> {
    const target = this.tables.get(table);
    if (!target) {
      throw new Error(`table not found: ${table}`);
    }

    return [...target.docs.values()].sort((left, right) => left.id.localeCompare(right.id));
  }
}

export class ConvexLikeClient {
  constructor(
    private readonly schema: DatabaseSchema,
    private readonly storage: StorageAdapter,
  ) {}

  async init(): Promise<void> {
    for (const [table, collectionSchema] of Object.entries(this.schema)) {
      await this.storage.createTable(table, collectionSchema);
    }
  }

  async insert(table: string, value: NewDocumentRecord): Promise<DocumentRecord> {
    const [doc] = await this.storage.writeBatch(table, [{ kind: "put", value }]);
    return doc;
  }

  async get(table: string, id: string): Promise<DocumentRecord> {
    return this.storage.get(table, id);
  }

  async list(table: string): Promise<DocumentRecord[]> {
    return this.storage.list(table);
  }
}

const validateBySchema = (schema: CollectionSchema, fields: Record<string, JsonValue>): void => {
  for (const [fieldName, fieldSchema] of Object.entries(schema)) {
    if (fieldSchema.required && !(fieldName in fields)) {
      throw new Error(`schema violation: missing required field '${fieldName}'`);
    }
  }

  for (const [key, value] of Object.entries(fields)) {
    const expected = schema[key];
    if (!expected) {
      continue;
    }

    if (!isType(expected.type, value)) {
      throw new Error(`schema violation: field '${key}' expected ${expected.type}`);
    }
  }
};

const isType = (type: FieldType, value: JsonValue): boolean => {
  switch (type) {
    case "string":
      return typeof value === "string";
    case "number":
      return typeof value === "number";
    case "boolean":
      return typeof value === "boolean";
    case "null":
      return value === null;
    case "array":
      return Array.isArray(value);
    case "object":
      return typeof value === "object" && value !== null && !Array.isArray(value);
  }
};
