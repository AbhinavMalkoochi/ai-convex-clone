import { describe, expect, test } from "bun:test";

import type { CollectionSchema } from "../src/index";

describe("schema types", () => {
  test("accepts collection schema values", () => {
    const schema: CollectionSchema = {
      name: { required: true, type: "string" },
      age: { required: false, type: "number" },
    };

    expect(schema.name.type).toBe("string");
  });
});
