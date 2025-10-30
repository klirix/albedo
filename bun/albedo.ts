import {
  dlopen,
  ptr,
  suffix,
  FFIType,
  JSCallback,
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
  albedo_ensure_index: {
    args: [FFIType.pointer, FFIType.cstring, FFIType.uint8_t],
    returns: "u8",
  },
  albedo_drop_index: {
    args: [FFIType.pointer, FFIType.cstring],
    returns: "u8",
  },
  albedo_delete: {
    args: [FFIType.pointer, FFIType.pointer],
    returns: "u8",
  },
  albedo_data: {
    args: [FFIType.pointer, FFIType.pointer, FFIType.pointer],
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
  albedo_vacuum: {
    args: [FFIType.pointer],
    returns: "u8",
  },
  albedo_close: {
    args: [FFIType.pointer],
    returns: "u8",
  },
  albedo_flush: {
    args: [FFIType.pointer],
    returns: "u8",
  },
  albedo_set_replication_callback: {
    args: [FFIType.pointer, FFIType.function, FFIType.pointer],
    returns: "u8",
  },
  albedo_apply_batch: {
    args: [FFIType.pointer, FFIType.pointer, FFIType.u32, FFIType.u32],
    returns: "u8",
  },
});

const ResultCode = {
  OK: 0,
  Error: 1,
  HasData: 2,
  EOS: 3,
  OutOfMemory: 4,
  FileNotFound: 5,
  NotFound: 6,
  InvalidFormat: 7,
} as const;

const ReplicationError = {
  OK: 0,
  NetworkError: 1,
  DiskFull: 2,
  InvalidFormat: 3,
  ReplicaUnavailable: 4,
  TimeoutError: 5,
  UnknownError: 255,
} as const;

export { ReplicationError };

type Path = string;
type Scalar = string | number | Date | boolean | null | ObjectId;
type Filter =
  | Scalar
  | { $eq: Scalar }
  | { $gt: Scalar }
  | { $gte: Scalar }
  | { $lt: Scalar }
  | { $lte: Scalar }
  | { $ne: Scalar }
  | { $in: Scalar[] }
  | { $between: [Scalar, Scalar] };

export type Query = {
  query?: Record<Path, Filter>;
  sort?: { asc: Path } | { desc: Path };
  sector?: { offset?: number; limit?: number };
  projection?: { omit?: Path[] } | { pick?: Path[] };
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

  vacuum() {
    const result = albedo.albedo_vacuum(this.pointer);
    if (result !== 0) {
      throw new Error("Failed to vacuum Albedo database");
    }
  }

  flush() {
    const result = albedo.albedo_flush(this.pointer);
    if (result !== 0) {
      throw new Error("Failed to flush Albedo database");
    }
  }

  /**
   * Set a replication callback to receive page changes from this bucket.
   * The callback receives batches of changed pages after each flush.
   *
   * @param callback - Function called with batched page data: (data: Uint8Array, pageCount: number) => void
   *                   Buffer format: [BucketHeader (64 bytes)][Page1 (8192)][Page2 (8192)]...
   */
  /**
   * Set a replication callback to receive page changes from this bucket.
   * The callback receives batches of changed pages after each flush.
   *
   * @param callback - Function called with batched page data that returns an error code
   *                   - (data: Uint8Array, pageCount: number) => number
   *                   - Return 0 for success, non-zero error code for failure
   *                   - On failure, pages will be retried on next sync
   *                   Buffer format: [BucketHeader (64 bytes)][Page1 (8192)][Page2 (8192)]...
   */
  setReplicationCallback(
    callback: (data: Uint8Array, pageCount: number) => number
  ) {
    // Create a JSCallback that matches the C signature:
    // fn(context: ?*anyopaque, data: [*]const u8, data_size: u32, page_count: u32) u8
    const jsCallback = new JSCallback(
      (
        contextPtr: any,
        dataPtr: any,
        dataSize: number,
        pageCount: number
      ): number => {
        try {
          // Convert raw pointer to Uint8Array
          const data = toBuffer(dataPtr, 0, dataSize);
          const errorCode = callback(data, pageCount);
          return errorCode; // 0 = success, >0 = error
        } catch (err) {
          console.error("Replication callback error:", err);
          return ReplicationError.UnknownError;
        }
      },
      {
        args: [FFIType.pointer, FFIType.pointer, FFIType.u32, FFIType.u32],
        returns: FFIType.u8, // Changed from void to u8
      }
    );

    // Store reference to prevent GC
    (this as any)._replicationCallback = jsCallback;

    const result = albedo.albedo_set_replication_callback(
      this.pointer,
      jsCallback,
      null
    );

    if (result !== 0) {
      throw new Error("Failed to set replication callback");
    }
  }

  /**
   * Apply a batch of replicated pages to this bucket (for replicas).
   * This is used to receive page changes from a primary bucket.
   *
   * @param data - Raw data: BucketHeader (64 bytes) + N pages (8192 bytes each)
   * @param pageCount - Number of pages in the batch
   */
  applyReplicatedBatch(data: Uint8Array, pageCount: number) {
    const expectedSize = 64 + pageCount * 8192; // BucketHeader.byteSize + (page_count * DEFAULT_PAGE_SIZE)
    if (data.length !== expectedSize) {
      throw new Error(
        `Data must be exactly ${expectedSize} bytes (64 byte header + ${pageCount} pages), got ${data.length}`
      );
    }

    const dataPtr = ptr(data);
    const result = albedo.albedo_apply_batch(
      this.pointer,
      dataPtr,
      data.length,
      pageCount
    );

    if (result === ResultCode.InvalidFormat) {
      throw new Error("Invalid data format");
    }
    if (result !== ResultCode.OK) {
      throw new Error("Failed to apply replicated batch");
    }
  }

  /**
   * Apply a replicated page to this bucket (for replicas).
   * This is used to receive page changes from a primary bucket.
   *
   * @param pageId - The page ID to apply
   * @param data - Raw data: BucketHeader (64 bytes) + Page (8192 bytes) = 8256 bytes
   */
  static defaultIndexOptions = {
    unique: false,
    sparse: false,
    reverse: false,
  };

  ensureIndex(field: string, options = Bucket.defaultIndexOptions) {
    var optionFlags = 0;
    optionFlags |= Number(options.reverse) << 0;
    optionFlags |= Number(options.sparse) << 1;
    optionFlags |= Number(options.unique) << 2;
    const res = albedo.albedo_ensure_index(
      this.pointer,
      Buffer.from(`${field}\0`),
      optionFlags
    );
    if (res !== 0) {
      throw new Error("Failed to create index in Albedo database");
    }
  }

  dropIndex(field: string) {
    const res = albedo.albedo_drop_index(
      this.pointer,
      Buffer.from(`${field}\0`)
    );
    if (res === ResultCode.OK) {
      return true;
    }
    if (res === ResultCode.NotFound) {
      return false;
    }
    throw new Error("Failed to drop index in Albedo database");
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
    const finalQuery: Query = { query };
    if (options.sort) finalQuery.sort = options.sort;
    if (options.sector) finalQuery.sector = options.sector;

    const queryBuf = BSON.serialize(finalQuery);
    // console.timeEnd("serialize");

    const queryPtr = ptr(queryBuf);
    const iterPtr = new BigInt64Array(1); // 8 bytes for a pointer
    const iterPtrPtr = ptr(iterPtr);
    // console.time("list");
    const res = albedo.albedo_list(this.pointer, queryPtr, iterPtrPtr);
    // console.timeEnd("list");
    // console.log("res", res);
    if (res !== 0) {
      throw new Error("Failed to list Albedo database");
    }
    const iterHandle = read.ptr(iterPtrPtr) as Pointer;
    const dataPtrPtr = ptr(new BigInt64Array(1));

    while (true) {
      const res = albedo.albedo_data(iterHandle, dataPtrPtr);

      if (res === ResultCode.EOS) {
        break;
      }
      if (res > ResultCode.Error) {
        console.log("res", res);
        throw new Error("Failed to get data from Albedo database");
      }
      const ptr = read.ptr(dataPtrPtr) as Pointer;
      const size = read.u32(ptr);
      // console.log("res", dataPtrPtr.toString(16), sizeArr[0], i);

      // IMPORTANT: Copy the buffer before deserializing, since the arena will be freed
      // when the iterator is closed. toBuffer() creates a view, not a copy.
      const buffer = Buffer.from(toBuffer(ptr, 0, size));
      const shouldQuit = yield BSON.deserialize(buffer);

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
};
