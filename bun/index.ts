import { ObjectId } from "bson";
import albedo from "./albedo";

const bucket = albedo.Bucket.open("./test.bucket");

// console.time("insert albedo");
// for (let i = 0; i < 10; i++) {
//   bucket.insert({ name: "alice", age: 20 });
// }
// console.timeEnd("insert albedo");

console.time("list outer");
let i = 0;
for (const albedo_clos of bucket.list({}, { sector: { limit: 10 } })) {
  console.log(albedo_clos);
}
console.timeEnd("list outer");
// console.time("delete");
// bucket.delete({
//   _id: new ObjectId("68094109319e2b489d2b9c89"),
// });
// console.timeEnd("delete");
// const afterDelete = Array.from(bucket.list({}));
// console.log("i", afterDelete.length);

// console.time("list outer2");
// for (const albedo_clos of bucket.list({})) {
// }
// console.timeEnd("list outer2");

bucket.close();

// console.time("init sql");
// const sqldb = Database.open("./test.sql.db");
// console.timeEnd("init sql");

// sqldb.run(
//   "CREATE TABLE IF NOT EXISTS test (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)"
// );

// // console.time("insert");
// // for (let i = 0; i < 50000; i++) {
// //   sqldb.run("INSERT INTO test (name, age) VALUES (?, ?)", [`Alice`, 30]);
// // }
// // console.timeEnd("insert");
// console.time("select");
// let stmt = sqldb.prepare("SELECT * FROM test");
// let rows = stmt.all();
// console.timeEnd("select");
// console.log("rows", rows.length);

// console.time("select2");
// stmt = sqldb.prepare("SELECT * FROM test");
// rows = stmt.all();
// console.timeEnd("select2");
