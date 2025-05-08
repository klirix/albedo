import { ObjectId } from "bson";
import { Bucket } from "./albedo";

const bucket = Bucket.open("./test.bucket");

// console.time("insert");
// for (let i = 0; i < 16000; i++) {
//   bucket.insert({
//     name: `test-${i}`,
//     age: 10,
//   });
//   if (i % 1000 == 1 && Math.floor(i / 1000) != 0) console.timeLog("insert", i);
// }
// console.timeEnd("insert");

// bucket.get({});

// bucket.list({});

console.time("list");
let res = bucket.all(
  { name: { $gt: "test-1000" } },
  { sector: { limit: 100 } }
);
console.timeEnd("list");

console.time("list");
res = bucket.all(
  { id: new ObjectId("681c5275e5b72f6b6aa53eb2") },
  { sector: { limit: 1 } }
);
console.timeEnd("list");
console.log("res", res.slice(0, 10), res.length);

// console.time("all + serialize");
// bucket.all({});
// console.timeEnd("all + serialize");

bucket.close();
