import { describe, expect, test } from "bun:test";
import { Database } from "bun:sqlite";
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

      console.time("insertion x 100k");
      for (let i = 0; i < 100000; i++) {
        bucket.insert({
          hello: `world${i}`,
          _id: i,
          date: new Date(),
          counter: i,
        });
      }
      console.timeEnd("insertion x 100k");

      console.time("read all docs");
      const allItems = Array.from(bucket.all({}, {}));
      console.timeEnd("read all docs");
      console.time("read all docs warmed up pages");
      const allItems2 = Array.from(bucket.all({}, {}));
      console.timeEnd("read all docs warmed up pages");

      console.time("scan docs warmed up pages");
      const scanItems = Array.from(bucket.all({ counter: 55555 }, {}));
      console.timeEnd("scan docs warmed up pages");
      console.time("idx fetching");
      let idxItems = Array.from(bucket.all({ _id: { $eq: 55555 } }, {}));
      console.timeEnd("idx fetching");
      console.time("idx fetching warmed up pages");
      idxItems = Array.from(bucket.all({ _id: { $eq: 55555 } }, {}));
      console.timeEnd("idx fetching warmed up pages");
      expect(allItems.length).toBe(100000);
      expect(idxItems.length).toBe(1);
      expect(idxItems[0]?._id).toBe(55555);

      bucket.close();
      const file = Bun.file("test-bucket.albedo");
      expect(await file.exists()).toBe(true);
      file.delete();

      console.log(" == test sqlite for comparison ==");
      const db = new Database("test-bucket-sqlite.db");
      db.run(
        "CREATE TABLE items ( doc BLOB ); create index idx_id on items (json_extract(doc, '$._id'));"
      );
      console.time("sqlite insertion x 100k");
      const insertStmt = db.prepare(
        "INSERT INTO items (doc) VALUES (json(?));"
      );

      console.time("sqlite insertion x 100k");
      for (let i = 0; i < 100000; i++) {
        insertStmt.run(
          JSON.stringify({
            hello: `world${i}`,
            _id: i,
            date: new Date(),
            counter: i,
          })
        );
      }
      console.timeEnd("sqlite insertion x 100k");

      const allSqliteItemsStmt = db.prepare<{ doc: string }, []>(
        "SELECT doc FROM items where json_extract(doc, '$._id') != 20000;"
      );
      console.time("sqlite read all docs");
      const allSqliteItems = allSqliteItemsStmt
        .all()
        .map(({ doc }) => JSON.parse(doc));
      console.timeEnd("sqlite read all docs");
      console.log(allSqliteItems[0]);
      console.time("sqlite read all docs warmed up pages");
      allSqliteItemsStmt.all().map(({ doc }) => JSON.parse(doc));
      console.timeEnd("sqlite read all docs warmed up pages");

      const scanSqliteItemsStmt = db.prepare<{ doc: string }, [number]>(
        "SELECT doc FROM items WHERE json_extract(doc, '$.counter') = ?;"
      );
      console.time("sqlite scan docs warmed up pages");
      let scanSqliteItems = JSON.stringify(scanSqliteItemsStmt.get(55555)!.doc);
      console.timeEnd("sqlite scan docs warmed up pages");

      const idxSqliteItemsStmt = db.prepare(
        "SELECT doc FROM items WHERE json_extract(doc, '$._id') = ?;"
      );
      console.time("sqlite idx fetching");
      let idxSqliteItems = idxSqliteItemsStmt.get(55555);
      console.timeEnd("sqlite idx fetching");
      console.time("sqlite idx fetching warmed up pages");
      idxSqliteItems = idxSqliteItemsStmt.get(55555);
      console.timeEnd("sqlite idx fetching warmed up pages");

      db.close();
      const sqliteFile = Bun.file("test-bucket-sqlite.db");
      expect(await sqliteFile.exists()).toBe(true);
      sqliteFile.delete();
    });
  });
});
