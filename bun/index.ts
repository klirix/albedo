import { Bucket } from "./albedo";
import * as sql from "bun:sqlite";

const bucket = Bucket.open("./test.bucket");

for (let i = 0; i < 300; i++)
  bucket.insert({
    name: `test-${i}`,
    age: 10,
  });

// bucket.update({ i: { $gt: 14000 } }, (doc) => {
//   doc.name = "updated";
//   return doc;
// });

// console.log(bucket.get({ i: 1000 }));

// bucket.delete({ i: 1000 });

console.log(bucket.all({}));

bucket.close();
