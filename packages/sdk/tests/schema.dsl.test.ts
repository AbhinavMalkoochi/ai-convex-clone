import { describe, expect, test } from "bun:test";

import { defineCollection, defineSchema, fromSchemaJson, s, toSchemaJson } from "../src/index";

describe("schema dsl", () => {
  test("builds a typed schema and round-trips JSON", () => {
    const users = defineCollection({
      name: s.string(),
      age: s.number({ required: false }),
      isAdmin: s.boolean({ required: false }),
    });

    const db = defineSchema({ users });
    const json = toSchemaJson(db);
    const restored = fromSchemaJson(json);

    expect(restored.users.name.type).toBe("string");
    expect(restored.users.age.required).toBe(false);
  });
});
