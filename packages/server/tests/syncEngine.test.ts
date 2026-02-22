import { describe, expect, test } from "bun:test";

import { defineCollection, defineSchema, s } from "@acx/sdk";

import { SyncEngine } from "../src/syncEngine";

describe("SyncEngine", () => {
  test("returns snapshot on subscribe and broadcasts inserts", async () => {
    const schema = defineSchema({
      users: defineCollection({
        name: s.string(),
      }),
    });

    const engine = new SyncEngine(schema);
    await engine.init();
    engine.registerSession("s1");
    engine.registerSession("s2");

    const subOne = await engine.process("s1", {
      type: "subscribe",
      requestId: "req_sub_1",
      table: "users",
    });
    const subTwo = await engine.process("s2", {
      type: "subscribe",
      requestId: "req_sub_2",
      table: "users",
    });

    expect(subOne[1]?.message.type).toBe("snapshot");
    expect(subTwo[1]?.message.type).toBe("snapshot");

    const insertOut = await engine.process("s1", {
      type: "insert",
      requestId: "req_insert_1",
      table: "users",
      value: {
        id: "u_1",
        fields: { name: "Ada" },
      },
    });

    const changeEvents = insertOut.filter((x) => x.message.type === "change");
    expect(changeEvents.length).toBe(2);
  });

  test("maps schema violations to protocol errors", async () => {
    const schema = defineSchema({
      users: defineCollection({
        name: s.string(),
      }),
    });

    const engine = new SyncEngine(schema);
    await engine.init();
    engine.registerSession("s1");

    const out = await engine.process("s1", {
      type: "insert",
      requestId: "req_bad_insert",
      table: "users",
      value: {
        id: "u_bad",
        fields: { name: true },
      },
    });

    expect(out.length).toBe(1);
    expect(out[0]?.message.type).toBe("error");
    if (out[0]?.message.type === "error") {
      expect(out[0].message.code).toBe("SCHEMA_VIOLATION");
    }
  });
});
