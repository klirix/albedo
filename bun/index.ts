import { ObjectId } from "bson";
import albedo from "./albedo";

const bucket = albedo.Bucket.open("./test.bucket");

// for (let i = 0; i < 10000; i++) {
//   bucket.insert({
//     _id: new ObjectId(),
//     name: "new",
//     age: 10,
//     i: i,
//   });
// }

console.time("find");
const res = Array.from(
  bucket.list(
    { name: "new" },
    { sort: { asc: "_id" }, sector: { offset: 9000, limit: 1 } }
  )
);
console.timeEnd("find");

bucket.close();
