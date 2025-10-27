import { ObjectId } from "bson";
import albedo, { Bucket } from "./albedo";

const bucket = Bucket.open("test-bucket.bucket");

// console.time("insertion");
// const id = new ObjectId();
// for (let i = 0; i < 100000; i++) {
//   bucket.insert({
//     hello: `world${i}`,
//     date: new Date(),
//     _id: i == 49999 ? id : new ObjectId(),
//   });
// }
// console.timeEnd("insertion");
// console.time("insertion2");
// for (let i = 0; i < 100; i++) {
//   bucket.insert({ hello: `world${i}`, date: new Date() });
// }
// console.timeEnd("insertion2");

console.time("insertion2");
for (let i = 0; i < 10000; i++) {
  bucket.insert({ hello: `world${i}`, _id: i, date: new Date() });
}
console.timeEnd("insertion2");

// console.time("index");
// let res = bucket.all({ hello: { $eq: "world1" } }, {});
// console.timeEnd("index");

// console.time("no index");
// const [doc] = bucket.all({ hello: { $eq: "world36666" } }, {});
// console.timeEnd("no index");

// console.time("create index");
// bucket.ensureIndex("hello");
// console.timeEnd("create index");

console.time("no index");
const [doc2] = bucket.all({ hello: { $eq: "world56" } }, {});
console.timeEnd("no index");

console.time("index");
let res2 = bucket.all({ _id: 56 }, {});
console.timeEnd("index");

// bucket.ensureIndex("hello");

// let len = 0;
// for (const doc of bucket.list(
//   { hello: { $gte: "world50", $lte: "world60" } },
//   { sector: { limit: 10 } }
// ))
//   console.log(doc), len++;
// console.log("len", len);

// console.log("res", res2.length);

// console.time("vacuum");
// bucket.vacuum();
// console.timeEnd("vacuum");

bucket.close();
