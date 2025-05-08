import {
  dlopen,
  ptr,
  suffix,
  FFIType,
  type Pointer,
  read,
  toBuffer,
} from "bun:ffi";
import { BSON, ObjectId } from "bson";

const { symbols: albedo } = dlopen(`${__dirname}/libalbedo.${suffix}`, {
  albedo_version: {
    args: [],
    returns: "u32",
  },
  albedo_open: {
    args: [FFIType.cstring, FFIType.pointer],
    returns: "u8",
  },
  albedo_insert: {
    args: [FFIType.pointer, FFIType.pointer],
    returns: "u8",
  },
  albedo_list: {
    args: [FFIType.pointer, FFIType.pointer, FFIType.pointer],
    returns: "u8",
  },
  albedo_delete: {
    args: [FFIType.pointer, FFIType.pointer],
    returns: "u8",
  },
  albedo_data: {
    args: [FFIType.pointer, FFIType.pointer],
    returns: "u8",
  },
  albedo_next: {
    args: [FFIType.pointer],
    returns: "u8",
  },
  albedo_close_iterator: {
    args: [FFIType.pointer],
    returns: "u8",
  },
  albedo_close: {
    args: [FFIType.pointer],
    returns: "u8",
  },
});

type Scalar = string | number | Date | boolean | null | ObjectId;
type Filter =
  | Scalar
  | { $eq: Scalar }
  | { $gt: Scalar }
  | { $lt: Scalar }
  | { $ne: Scalar }
  | { $in: Scalar[] }
  | { $between: Scalar[] };

export type Query = {
  query?: Record<string, Filter>;
  sort?: { asc: string } | { desc: string };
  sector?: { offset?: number; limit?: number };
  projection?: { omit?: string[] } | { pick?: string[] };
};
/// {
///  "query": {"field.path": {"$eq": "value"}}, // Flat field.path -> filter
///  "sort": {"asc": "field"} | {"desc": "field"}, // Sort by field, only one field allowed
///  "sector": {"offset": 0, "limit": 10},  // Offset and limit for pagination
///  "projection": {"omit": ["path"] } | {"pick": ["path"]} // Projection of fields wither
/// }

export class Bucket {
  constructor(private pointer: Pointer) {}

  insert(data: BSON.Document) {
    const dataBuf = BSON.serialize(data);
    const dataPtr = ptr(dataBuf);
    const result = albedo.albedo_insert(this.pointer, dataPtr);
    if (result !== 0) {
      throw new Error("Failed to insert into Albedo database");
    }
  }

  static open(path: string) {
    const dbPtr = new BigUint64Array(1); // 8 bytes for a pointer
    const dbPtrPtr = ptr(dbPtr);
    const result = albedo.albedo_open(Buffer.from(`${path}\0`), dbPtrPtr);

    if (result !== 0) {
      throw new Error("Failed to open Albedo database");
    }
    const pointer = read.ptr(dbPtrPtr);
    return new Bucket(pointer as Pointer); // Pass the actual pointer value
  }

  close() {
    const result = albedo.albedo_close(this.pointer);
    if (result !== 0) {
      throw new Error("Failed to close Albedo database");
    }
  }

  delete(query: Query["query"], options: { sector?: Query["sector"] } = {}) {
    const queryBuf = BSON.serialize({ query });
    const queryPtr = ptr(queryBuf);
    const result = albedo.albedo_delete(this.pointer, queryPtr);
    if (result !== 0) {
      throw new Error("Failed to delete from Albedo database");
    }
  }

  *list(
    query: Query["query"] = {},
    options: {
      sort?: Query["sort"];
      sector?: Query["sector"];
    } = {}
  ): Generator<BSON.Document, void, boolean | undefined> {
    // console.time("serialize");
    const finalQuery = {
      query,
      ...(options.sort ? { sort: options.sort } : {}),
      ...(options.sector ? { sector: options.sector } : {}),
    };
    const queryBuf = BSON.serialize(finalQuery);
    // console.timeEnd("serialize");

    const queryPtr = ptr(queryBuf);
    const iterPtr = new BigInt64Array(1); // 8 bytes for a pointer
    const iterPtrPtr = ptr(iterPtr);
    console.time("list");
    const res = albedo.albedo_list(this.pointer, queryPtr, iterPtrPtr);
    console.timeEnd("list");
    // console.log("res", res);
    if (res !== 0) {
      throw new Error("Failed to list Albedo database");
    }
    const iterHandle = read.ptr(iterPtrPtr) as Pointer;
    const dataPtrPtr = ptr(new BigInt64Array(1));

    while (albedo.albedo_next(iterHandle) === 2) {
      const res = albedo.albedo_data(iterHandle, dataPtrPtr);
      const dataPtr = read.ptr(dataPtrPtr) as Pointer;
      const size = read.u32(dataPtr);

      if (res !== 0) {
        throw new Error("Failed to get data from Albedo database");
      }
      const shouldQuit = yield BSON.deserialize(toBuffer(dataPtr, 0, size));

      if (shouldQuit) {
        break;
      }
    }
    albedo.albedo_close_iterator(iterHandle);
  }

  all(query: Query["query"] = {}, options: Query = {}) {
    const result: BSON.Document[] = [];
    for (const doc of this.list(query, options)) {
      result.push(doc);
    }
    return result;
  }

  get(query: Query["query"], options: Query = {}) {
    const result = this.list(query, options).next(true);
    if (result.done) {
      return null;
    }
    return result.value;
  }

  update(
    query: Query["query"],
    updateFunc: (doc: BSON.Document) => BSON.Document
  ) {
    for (const doc of this.list(query, {})) {
      this.delete({ _id: doc._id });
      this.insert(updateFunc(doc));
    }
  }
}

export default {
  Bucket,
  version() {
    return albedo.albedo_version();
  },
  albedo_open: albedo.albedo_open,
  albedo_close: albedo.albedo_close,
};
