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

bucket.all({ name: "test-1000" });

console.time("all + serialize");
bucket.all({ name: "test-1000" });
console.timeEnd("all + serialize");

bucket.close();
