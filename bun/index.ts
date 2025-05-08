import { ObjectId } from "bson";
import { Bucket } from "./albedo";

const bucket = Bucket.open("./test.bucket");

const testBytes = await Bun.file("test.db").bytes();

console.time("insert");
// for (let i = 0; i < 100; i++) {
//   bucket.insert({
//     name: `test-${i}`,
//     data: testBytes,
//     age: 10,
//   });
//   if (i % 10 == 1 && Math.floor(i / 10) != 0) console.timeLog("insert", i);
// }
console.timeEnd("insert");

// bucket.get({});

// bucket.list({});

// console.time("list");
// let res = bucket.all({}, {});
// console.timeEnd("list");

console.time("list");
let res = bucket.all({}, { sector: { limit: 1 } });
console.timeEnd("list");
console.time("list");
res = bucket.all({}, { sector: { limit: 3, offset: 5 } });
console.timeEnd("list");
console.log("res", res.slice(0, 10), res.length);

// console.time("all + serialize");
// bucket.all({});
// console.timeEnd("all + serialize");

bucket.close();
