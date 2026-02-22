import { describe, expect, test } from "bun:test";

import {
  RealtimeClient,
  type RealtimeTransport,
  defineCollection,
  defineSchema,
  s,
} from "@acx/sdk";

import { SyncEngine } from "../src/syncEngine";
import { handleIncomingMessage } from "../src/websocketServer";

class InProcessTransport implements RealtimeTransport {
  private handler: ((payload: string) => void) | null = null;

  constructor(private readonly onSend: (payload: string) => Promise<void>) {}

  send(payload: string): void {
    void this.onSend(payload);
  }

  close(): void {}

  onMessage(handler: (payload: string) => void): void {
    this.handler = handler;
  }

  receive(payload: string): void {
    this.handler?.(payload);
  }
}

describe("sync e2e", () => {
  test("realtime client receives snapshot and change events", async () => {
    const schema = defineSchema({
      users: defineCollection({ name: s.string() }),
    });

    const engine = new SyncEngine(schema);
    await engine.init();

    const sessionId = "session_e2e_1";
    engine.registerSession(sessionId);

    let transport: InProcessTransport | null = null;
    transport = new InProcessTransport(async (payload: string) => {
      await handleIncomingMessage(engine, sessionId, payload, (out) => transport?.receive(out));
    });

    const client = new RealtimeClient(() => transport as InProcessTransport);

    const events: string[] = [];
    client.subscribe("users", (event) => {
      events.push(event.type);
    });

    await Bun.sleep(5);

    client.insert("users", {
      id: "u_e2e",
      fields: { name: "Ada" },
    });

    await Bun.sleep(5);

    expect(events).toContain("snapshot");
    expect(events).toContain("change");
  });
});
