import { describe, expect, test } from "bun:test";
import { Bucket } from "./albedo-wasm";

describe("Albedo WASM", () => {
  describe("Bucket.open", () => {
    test("should open an in-memory bucket", () => {
      const bucket = Bucket.open(":memory:");
      expect(bucket).toBeInstanceOf(Bucket);
      bucket.close();
    });

    test("should open a file-based bucket", async () => {
      const bucket = Bucket.open("test-bucket.albedo");
      expect(bucket).toBeInstanceOf(Bucket);
      bucket.close();
      const file = Bun.file("test-bucket.albedo");
      expect(await file.exists()).toBe(true);
      // file.delete();
    });
  });
});
