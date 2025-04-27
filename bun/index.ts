import { Bucket } from "./albedo";

const bucket = Bucket.open("./test.bucket");

bucket.insert({
  name: "new",
  age: 10,
});

bucket.update({ i: { $gt: 14000 } }, (doc) => {
  doc.name = "updated";
  return doc;
});

console.log(bucket.get({ i: 1000 }));

bucket.delete({ i: 1000 });

console.log(bucket.all().length);

bucket.close();
