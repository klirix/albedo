import { Bucket } from "./albedo";
import * as sql from "bun:sqlite";

const db = new sql.Database("./test.db");

db.exec(`
CREATE TABLE IF NOT EXISTS test (
  id INTEGER PRIMARY KEY,
  name TEXT,
  age INTEGER
);`);

db.exec(`
CREATE TABLE IF NOT EXISTS test2 (
  id INTEGER PRIMARY KEY,
  name TEXT,
  age INTEGER
);`);

db.exec(`create index if not exists idx_name on test2(name);`);
db.exec(`create index if not exists idx_name2 on test(name);`);

// const bucket = Bucket.open("./test.bucket");

// // bucket.insert({
// //   name: "new",
// //   age: 10,
// // });

// // bucket.update({ i: { $gt: 14000 } }, (doc) => {
// //   doc.name = "updated";
// //   return doc;
// // });

// // console.log(bucket.get({ i: 1000 }));

// // bucket.delete({ i: 1000 });

// console.log(bucket.all({ i: { $gt: 14003 } }));

// bucket.close();
