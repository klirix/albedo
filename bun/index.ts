import { ObjectId } from "bson";
import { Bucket } from "./albedo";

const bucket = Bucket.open("./test.bucket");

const file = Bun.file("libalbedo.dylib");

let res = bucket.all({}, {});

console.log("res", res.length);

bucket.close();
