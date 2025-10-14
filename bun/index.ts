import { ObjectId } from "bson";
import { Bucket } from "./albedo";

const bucket = Bucket.open("./test.bucket");

const file = Bun.file("libalbedo.dylib");
const buffer = await file.arrayBuffer();

// console.time("insertion");
// const id = new ObjectId();
// for (let i = 0; i < 100000; i++) {
//   bucket.insert({
//     hello: `world${i}`,
//     date: new Date(),
//     binary: buffer,
//     _id: i == 49999 ? id : new ObjectId(),
//   });
// }
// console.timeEnd("insertion");
// console.time("insertion2");
// for (let i = 0; i < 100000; i++) {
//   bucket.insert({ hello: `world${i}`, date: new Date() });
// }
// console.timeEnd("insertion2");

console.time("no index");
const [doc] = bucket.all({ hello: { $eq: "world36666" } }, {});
console.timeEnd("no index");

console.time("index");
let res = bucket.all({ _id: doc._id }, {});
console.timeEnd("index");

console.time("no index");
const [doc2] = bucket.all({ hello: { $eq: "world56666" } }, {});
console.timeEnd("no index");

console.time("index");
let res2 = bucket.all({ _id: doc2._id }, {});
console.timeEnd("index");

let len = 0;
for (const doc of bucket.list({}, {})) len++;
console.log("len", len);

console.log("res", res.length);

bucket.close();
