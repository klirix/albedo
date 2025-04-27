import { ObjectId } from "bson";
import albedo from "./albedo";

const bucket = albedo.Bucket.open("./test.bucket");

// console.time("insert");
// for (let i = 0; i < 40000; i++) {
//   bucket.insert({
//     _id: new ObjectId(),
//     name: "new",
//     age: 10,
//     i: i + 10000,
//   });
//   if (i % 1000 === 0) {
//     console.log("inserted", i);
//   }
// }
// console.timeEnd("insert");

console.time("find");
const res = Array.from(bucket.list({ i: { $gt: 0 } }));
console.timeEnd("find");

console.log(res[0], res.length);

console.time("find");
const res2 = Array.from(bucket.list({ i: { $gt: 0 } }));
console.timeEnd("find");

bucket.close();
