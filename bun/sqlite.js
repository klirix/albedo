import { Database } from "bun:sqlite";
const db = new Database("./test.db");
// db.exec("PRAGMA enable_load_extension = 1");
// db.exec("SELECT load_extension('json5');");
// db.loadExtension("json1");
db.exec("CREATE TABLE if not exists test (id INTEGER PRIMARY KEY, doc BLOB)");

console.time("insert");
const insert = db.prepare("INSERT INTO test (id, doc) VALUES (?, json(?));");
for (let i = 0; i < 10000; i++) {
  insert.run(i, `{"name": "test-${i}", "age": 10}`);
  if (i % 1000 == 1 && Math.floor(i / 1000) != 0) console.timeLog("insert", i);
}
console.timeEnd("insert");

console.time("sql all");
const res = db
  .prepare(
    "select * from test where json_extract(doc, '$.name') == \"test-5555\";"
  )
  .all();

console.timeEnd("sql all");
// console.log(res.slice(0, 10));
console.time("sql create idx");
db.exec(
  "create index if not exists idx_name on test (json_extract(doc, '$.name'));"
);
console.timeEnd("sql create idx");

console.time("sql idx");
const res2 = db
  .prepare(
    "select * from test where json_extract(doc, '$.name') == \"test-5555\";"
  )
  .all();
console.log(res2[0], res2.length);
console.timeEnd("sql idx");

db.close();
Bun.file("./test.db").delete();
