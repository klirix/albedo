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
    { sort: { asc: "i" }, sector: { offset: 10, limit: 10 } }
  )
);
console.timeEnd("find");

console.log(res);

bucket.close();
