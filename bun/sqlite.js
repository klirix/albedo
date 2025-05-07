import DB from "better-sqlite3";
const db = DB("./test.db");
// db.exec("PRAGMA enable_load_extension = 1");
// db.exec("SELECT load_extension('json5');");
// db.loadExtension("json1");
db.exec("CREATE TABLE if not exists test (id INTEGER PRIMARY KEY, doc BLOB)");

// console.time("insert");
// const insert = db.prepare("INSERT INTO test (id, doc) VALUES (?, jsonb(?));");
// for (let i = 0; i < 16000; i++) {
//   insert.run(i, `{"name": "test-${i}", "age": 10 }`);
//   if (i % 1000 == 1 && Math.floor(i / 1000) != 0) console.timeLog("insert", i);
// }
// console.timeEnd("insert");

console.time("sql all");
const res = db
  .prepare(
    "select * from test where jsonb_extract(doc, '$.age') == 10 order by jsonb_extract(doc, '$.age');"
  )
  .all();

console.timeEnd("sql all");
// console.log(res.slice(0, 10));

console.time("sql all");
const res2 = db
  .prepare(
    "select * from test where jsonb_extract(doc, '$.age') == 10 order by jsonb_extract(doc, '$.age');"
  )
  .all();
console.log(res2[0], res2.length);
console.timeEnd("sql all");
