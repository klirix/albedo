import { ObjectId } from "bson";
import albedo from "./albedo";

const bucket = albedo.Bucket.open("./test.bucket");

console.time("list outer");
let i = 0;
for (const albedo_clos of bucket.list({}, { sector: { limit: 1 } })) {
}
bucket.insert({ data: "hello" });
console.timeEnd("list outer");
const afterDelete = Array.from(bucket.list({}));
console.log("i", afterDelete.length);

bucket.close();
