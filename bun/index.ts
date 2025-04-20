import { ObjectId } from "bson";
import albedo from "./albedo";

const bucket = albedo.Bucket.open("./test.bucket");

console.time("list outer");
for (const albedo_clos of bucket.list({})) {
}
console.timeEnd("list outer");

console.time("list outer2");
for (const albedo_clos of bucket.list({})) {
}
console.timeEnd("list outer2");

bucket.close();
