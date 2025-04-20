import { dlopen, ptr, suffix, FFIType, type Pointer, read } from "bun:ffi";
import { BSON, ObjectId } from "bson";

const { symbols: albedo } = dlopen(`libalbedo.${suffix}`, {
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
  albedo_data_size: {
    args: [FFIType.pointer],
    returns: "u32",
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

type Scalar = string | number | boolean | null | ObjectId;
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
/// {
///  "query": {"field.path": {"$eq": "value"}}, // Flat field.path -> filter
///  "sort": {"asc": "field"} | {"desc": "field"}, // Sort by field, only one field allowed
///  "sector": {"offset": 0, "limit": 10},  // Offset and limit for pagination
///  "projection": {"omit": ["path"] } | {"pick": ["path"]} // Projection of fields wither
/// }

class Bucket {
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

  *list(
    query: Query["query"] = {},
    sort: Query["sort"] = undefined,
    sector: Query["sector"] = undefined
  ) {
    const queryBuf = BSON.serialize({
      query,
      ...(sort ? sort : {}),
      ...(sector ? sector : {}),
    });
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

    while (albedo.albedo_next(iterHandle) === 2) {
      const size = albedo.albedo_data_size(iterHandle);
      if (size === 0) {
        console.error(
          new Error("Failed to get data size from Albedo database")
        );
        continue;
      }
      const dataPtr = new Uint8Array(size);
      const res = albedo.albedo_data(iterHandle, ptr(dataPtr));
      if (res !== 0) {
        throw new Error("Failed to get data from Albedo database");
      }
      yield BSON.deserialize(dataPtr);
    }
    albedo.albedo_close_iterator(iterHandle);
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
