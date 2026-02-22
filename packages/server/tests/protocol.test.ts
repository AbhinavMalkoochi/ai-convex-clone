import { describe, expect, test } from "bun:test";

import { encodeServerMessage, parseClientMessage } from "../src/protocol";

describe("sync protocol", () => {
  test("parses insert messages", () => {
    const parsed = parseClientMessage(
      JSON.stringify({
        type: "insert",
        requestId: "req_1",
        table: "users",
        value: { fields: { name: "Ada" } },
      }),
    );

    expect(parsed.type).toBe("insert");
    expect(parsed.requestId).toBe("req_1");
  });

  test("encodes error server messages", () => {
    const encoded = encodeServerMessage({
      type: "error",
      requestId: "req_2",
      ok: false,
      code: "BAD_REQUEST",
      message: "invalid payload",
    });

    expect(encoded).toContain("BAD_REQUEST");
  });
});
