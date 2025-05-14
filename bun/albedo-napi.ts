const albedo = require("./libalbedo.node");

type Scalar = string | number | boolean | null;
type Filter =
  | Scalar
  | { $eq: Scalar }
  | { $gt: Scalar }
  | { $lt: Scalar }
  | { $ne: Scalar }
  | { $in: Scalar[] };

export type Query = {
  query?: Record<string, Filter>;
  sort?: { asc: string } | { desc: string };
  sector?: { offset?: number; limit?: number };
  projection?: { omit?: string[] } | { pick?: string[] };
};

export class Bucket {
  handle = null;
  constructor(path: string) {
    this.handle = albedo.open(path);
  }

  all(query: Query): any[] {
    return albedo.all(this.handle, query);
  }

  *list(query: Query["query"], options: Omit<Query, "query"> = {}) {
    const iter = albedo.list(this.handle, {
      query,
      sort: options.sort,
      sector: options.sector,
    });
    try {
      let data = albedo.iter_next(iter);
      while (data != null) {
        yield data;
        data = albedo.iter_next(iter);
      }
    } finally {
      albedo.iter_close(iter);
    }
  }

  insert(data: Record<string, unknown>) {
    console.log(albedo.insert(this.handle, data));
  }

  delete(query: Query["query"]) {
    albedo.delete(this.handle, { query });
  }

  close() {
    albedo.close(this.handle);
  }
}

const bucket = new Bucket("./test.bucket");

console.time("list");
Array.from(
  bucket.list({ name: { $eq: "test-1000" } }, { sector: { limit: 2 } })
);
console.timeEnd("list");

console.time("list");
Array.from(
  bucket.list({ name: { $eq: "test-1000" } }, { sector: { limit: 100 } })
);
console.timeEnd("list");

console.time("list");
let res = Array.from(
  bucket.list({ name: { $eq: "test-1000" } }, { sector: { limit: 100 } })
);
console.log("res", res.slice(0, 10), res.length);
console.timeEnd("list");

const objid = new albedo.ObjectId();
console.log("albedo objid", `${objid}`);
// console.time("list");
// bucket.all({ query: {} });
// console.timeEnd("list");
// console.time("list");
// bucket.all({ query: {} });
// console.timeEnd("list");
// console.time("list");
// bucket.all({ query: {} });
// console.timeEnd("list");

bucket.close();
