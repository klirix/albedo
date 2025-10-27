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
      file.delete();
    });
  });

  describe("Bucket.insert and Bucket.all", () => {
    test("Insertion works and retrieves all items", async () => {
      const bucket = Bucket.open("test-bucket.albedo");
      expect(bucket).toBeInstanceOf(Bucket);

      console.time("insertion");
      for (let i = 0; i < 10000; i++) {
        bucket.insert({ hello: `world${i}`, _id: i, date: new Date() });
      }
      console.timeEnd("insertion");

      console.time("all");
      const allItems = Array.from(bucket.all({}, {}));
      console.timeEnd("all");
      console.time("all2");
      const allItems2 = Array.from(bucket.all({}, {}));
      console.timeEnd("all2");
      console.time("idx");
      const idxItems = Array.from(bucket.all({ _id: { $eq: 5555 } }, {}));
      console.timeEnd("idx");
      expect(allItems.length).toBe(10000);
      expect(idxItems.length).toBe(1);
      expect(idxItems[0]?._id).toBe(5555);

      bucket.close();
      const file = Bun.file("test-bucket.albedo");
      expect(await file.exists()).toBe(true);
      file.delete();
    });
  });
});
