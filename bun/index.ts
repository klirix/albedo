import { ObjectId } from "bson";
import { Bucket } from "./albedo";

const bucket = Bucket.open("./test.bucket");

const file = Bun.file("libalbedo.dylib");

// bucket.insert({ hellow: "world", name: "hello" });

console.time("open");
bucket.all({}, {});
console.timeEnd("open");
console.time("open");
let res = bucket.all({}, {});
console.timeEnd("open");

console.log("res", res.length);

bucket.close();
