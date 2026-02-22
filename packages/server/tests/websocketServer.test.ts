import { describe, expect, test } from "bun:test";

import { defineCollection, defineSchema, s } from "@acx/sdk";

import { SyncEngine } from "../src/syncEngine";
import { handleIncomingMessage } from "../src/websocketServer";

describe("websocket message handler", () => {
  test("returns bad request when payload is invalid json", async () => {
    const schema = defineSchema({
      users: defineCollection({ name: s.string() }),
    });

    const engine = new SyncEngine(schema);
    await engine.init();
    engine.registerSession("session_1");

    const sent: string[] = [];
    await handleIncomingMessage(engine, "session_1", "{bad-json", (m) => sent.push(m));

    expect(sent.length).toBe(1);
    expect(sent[0]).toContain("BAD_REQUEST");
  });

  test("processes subscribe messages and emits snapshot", async () => {
    const schema = defineSchema({
      users: defineCollection({ name: s.string() }),
    });

    const engine = new SyncEngine(schema);
    await engine.init();
    engine.registerSession("session_1");

    const sent: string[] = [];
    await handleIncomingMessage(
      engine,
      "session_1",
      JSON.stringify({
        type: "subscribe",
        requestId: "req_1",
        table: "users",
      }),
      (m) => sent.push(m),
    );

    expect(sent.length).toBe(2);
    expect(sent[0]).toContain("ack");
    expect(sent[1]).toContain("snapshot");
  });
});
