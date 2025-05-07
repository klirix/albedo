import { Bucket } from "./albedo";
import * as sql from "bun:sqlite";

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

bucket.get({ age: 10, name: "test-1000" });

bucket.get({ age: 10, name: "test-1000" });

bucket.get({ age: 10, name: "test-1000" });

console.time("all");
bucket.all({ age: 10 }, { sector: { limit: 500 } });
console.timeEnd("all");

bucket.close();
