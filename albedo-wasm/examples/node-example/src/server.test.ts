import { describe, test, expect, afterAll } from "bun:test";
import { bucket, server } from "./server";
import { env } from "bun";

describe("server", () => {
  test("allows for insertion", async () => {
    // Placeholder test
    const req = new Request("http://localhost:3000/todos", {
      method: "POST",
      body: JSON.stringify({ title: "Test Todo" }),
      headers: { "Content-Type": "application/json" },
    });

    const res = await server.handle(req);
    const data = (await res.json()) as any;

    expect(res.status).toBe(200);
    expect(data.title).toBe("Test Todo");
  });

  test("allows for fetching todos", async () => {
    const req = new Request("http://localhost:3000/todos", {
      method: "GET",
    });

    const res = await server.handle(req);
    const data = (await res.json()) as any[];

    expect(res.status).toBe(200);
    expect(Array.isArray(data)).toBe(true);
  });

  afterAll(() => {
    bucket.close();
    Bun.file(env.DB_FILE || "example-bucket.bucket").unlink();
  });
});
