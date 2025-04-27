import { ObjectId } from "bson";
import albedo from "./albedo";

const bucket = albedo.Bucket.open("./test.bucket");

// console.time("insert");
// for (let i = 0; i < 40000; i++) {
//   bucket.insert({
//     _id: new ObjectId(),
//     name: "new",
//     age: 10,
//     i: i,
//   });
//   if (i % 1000 === 0) {
//     console.log("inserted", i);
//   }
// }
// console.timeEnd("insert");

// bucket.update({ i: { $gt: 14000 } }, (doc) => ({
//   ...doc,
//   i: doc.i + 1000,
// }));

console.log(bucket.get({ _id: new ObjectId("680ea5c38cf7360383bd2b7a") }));

bucket.close();
