import { test, expect } from "bun:test";

test("package can be imported", async () => {
  const { serialize, deserialize } = await import("../dist/index.js");

  expect(typeof serialize).toBe("function");
  expect(typeof deserialize).toBe("function");
});

test("BSON serialization works", async () => {
  const { serialize, deserialize } = await import("../dist/index.js");

  const data = { name: "test", value: 42 };
  const serialized = serialize(data);
  const deserialized = deserialize(serialized);

  expect(deserialized).toEqual(data);
});
