import { describe, expect, test } from "bun:test";

import { ConvexLikeClient, InMemoryStorageAdapter } from "../src/client";
import { defineCollection, defineSchema, s } from "../src/index";

describe("ConvexLikeClient", () => {
  test("initializes, inserts and gets documents", async () => {
    const schema = defineSchema({
      users: defineCollection({
        name: s.string(),
        age: s.number({ required: false }),
      }),
    });

    const client = new ConvexLikeClient(schema, new InMemoryStorageAdapter());
    await client.init();

    const inserted = await client.insert("users", {
      id: "u_1",
      fields: { name: "Ada" },
    });

    expect(inserted.id).toBe("u_1");
    const fetched = await client.get("users", "u_1");
    expect(fetched.fields.name).toBe("Ada");
  });

  test("rejects writes violating schema", async () => {
    const schema = defineSchema({
      users: defineCollection({
        name: s.string(),
      }),
    });

    const client = new ConvexLikeClient(schema, new InMemoryStorageAdapter());
    await client.init();

    await expect(
      client.insert("users", {
        id: "u_2",
        fields: { name: true },
      }),
    ).rejects.toThrow("schema violation");
  });
});
