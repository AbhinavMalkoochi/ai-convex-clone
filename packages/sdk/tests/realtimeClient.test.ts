import { describe, expect, test } from "bun:test";

import { RealtimeClient, type RealtimeTransport } from "../src/realtimeClient";

class FakeTransport implements RealtimeTransport {
  sent: string[] = [];
  closed = false;
  private handler: ((payload: string) => void) | null = null;

  send(payload: string): void {
    this.sent.push(payload);
  }

  close(): void {
    this.closed = true;
  }

  onMessage(handler: (payload: string) => void): void {
    this.handler = handler;
  }

  receive(payload: string): void {
    this.handler?.(payload);
  }
}

describe("RealtimeClient", () => {
  test("subscribes, routes snapshots, and unsubscribes", () => {
    const transport = new FakeTransport();
    const client = new RealtimeClient(() => transport);

    const received: string[] = [];
    const off = client.subscribe("users", (event) => {
      received.push(event.type);
    });

    expect(transport.sent[0]).toContain("subscribe");

    transport.receive(
      JSON.stringify({
        type: "snapshot",
        table: "users",
        documents: [],
      }),
    );

    expect(received).toEqual(["snapshot"]);

    off();
    expect(transport.sent[1]).toContain("unsubscribe");
  });

  test("sends insert operations", () => {
    const transport = new FakeTransport();
    const client = new RealtimeClient(() => transport);

    client.insert("users", { fields: { name: "Ada" } });

    expect(transport.sent[0]).toContain("insert");
  });
});
