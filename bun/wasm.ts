import * as fs from "fs";
import path from "path";
import { randomFillSync } from "crypto";
import { BSON, type ObjectId } from "bson";

const wasmBuffer = await Bun.file(
  path.join(__dirname, "../zig-out/bin/albedo.wasm")
).arrayBuffer();
const module = await WebAssembly.compile(wasmBuffer);

type WasmHandleInfo = {
  fd: number;
  path: string;
  wasmPtr: number;
  wasmLen: number;
};

const handles = new Map<string, WasmHandleInfo>();
let nextHandleId = 1;
const encoder = new TextEncoder();
const decoder = new TextDecoder();

const ERROR_OK = 0;
const ERROR_NOT_FOUND = 1;
const ERROR_PERMISSION = 2;
const ERROR_ALREADY_EXISTS = 3;
const ERROR_UNEXPECTED = 4;

type WasmExports = WebAssembly.Exports & {
  memory: WebAssembly.Memory;
  albedo_insert: (bucketHandle: number, dataHandle: number) => number;
  albedo_open: (stringHandle: number, bucketOutHandle: number) => number;
  albedo_close: (bucketHandle: number) => number;
  albedo_malloc: (size: number) => number;
  albedo_free: (ptr: number, size: number) => void;
};

let wasmInstance: WebAssembly.Instance | null = null;
let wasmExports: WasmExports | null = null;
let wasmMemory: WebAssembly.Memory | null = null;

function getWasmExports(): WasmExports | null {
  if (wasmExports) return wasmExports;
  if (!wasmInstance) return null;
  wasmExports = wasmInstance.exports as WasmExports;
  wasmMemory = wasmExports.memory;
  return wasmExports;
}

function getMemory(): WebAssembly.Memory | null {
  if (wasmMemory) return wasmMemory;
  const exports = getWasmExports();
  if (!exports) return null;
  wasmMemory = exports.memory;
  return wasmMemory;
}

function getMemoryBuffer(): ArrayBuffer | null {
  const memory = getMemory();
  return memory ? memory.buffer : null;
}

function decodeString(ptr: number, len: number): string {
  const buffer = getMemoryBuffer();
  if (!buffer) return "";
  return decoder.decode(new Uint8Array(buffer, ptr, len));
}

function writeBytes(ptr: number, bytes: Uint8Array): boolean {
  const buffer = getMemoryBuffer();
  if (!buffer) return false;
  new Uint8Array(buffer, ptr, bytes.length).set(bytes);
  return true;
}

function writeHandleStruct(
  outPtr: number,
  handlePtr: number,
  handleLen: number
): boolean {
  const buffer = getMemoryBuffer();
  if (!buffer) return false;
  const view = new DataView(buffer);
  view.setUint32(outPtr, handlePtr, true);
  view.setUint32(outPtr + 4, handleLen, true);
  return true;
}

function writeUsize(outPtr: number, value: number): boolean {
  const buffer = getMemoryBuffer();
  if (!buffer) return false;
  const view = new DataView(buffer);
  view.setUint32(outPtr, value, true);
  return true;
}

function writeI64(outPtr: number, value: bigint): boolean {
  const buffer = getMemoryBuffer();
  if (!buffer) return false;
  const view = new DataView(buffer);
  view.setBigInt64(outPtr, value, true);
  return true;
}

function readInt32(ptr: number): number {
  const buffer = getMemoryBuffer();
  if (!buffer) return 0;
  const view = new DataView(buffer);
  return view.getInt32(ptr, true);
}

function readPtr(ptr: number): number {
  const buffer = getMemoryBuffer();
  if (!buffer) return 0;
  const view = new DataView(buffer);
  return view.getUint32(ptr, true);
}

function toNumber(value: number | bigint): number {
  return typeof value === "bigint" ? Number(value) : value;
}

function mapFsError(err: unknown): number {
  if (!(err instanceof Error)) return ERROR_UNEXPECTED;
  const code = (err as NodeJS.ErrnoException).code;
  switch (code) {
    case "ENOENT":
      return ERROR_NOT_FOUND;
    case "EACCES":
    case "EPERM":
      return ERROR_PERMISSION;
    case "EEXIST":
      return ERROR_ALREADY_EXISTS;
    default:
      return ERROR_UNEXPECTED;
  }
}

function resolveFilePath(p: string): string {
  return path.isAbsolute(p) ? p : path.join(process.cwd(), p);
}

function allocHandleBytes(
  bytes: Uint8Array
): { ptr: number; len: number } | null {
  const exports = getWasmExports();
  if (!exports) return null;
  const ptr = exports.albedo_malloc(bytes.length);
  if (ptr === 0) return null;
  if (!writeBytes(ptr, bytes)) {
    exports.albedo_free(ptr, bytes.length);
    return null;
  }
  return { ptr, len: bytes.length };
}

function freeHandle(info: WasmHandleInfo): void {
  const exports = getWasmExports();
  if (!exports) return;
  exports.albedo_free(info.wasmPtr, info.wasmLen);
}

const envImports = {
  wasm_open_file(
    pathPtr: number,
    pathLen: number,
    readFlag: number,
    writeFlag: number,
    createFlag: number,
    truncateFlag: number,
    outHandlePtr: number
  ): number {
    try {
      const exports = getWasmExports();
      if (!exports) return ERROR_UNEXPECTED;
      const filePath = decodeString(pathPtr, pathLen);
      const resolved = resolveFilePath(filePath);
      const read = Boolean(readFlag);
      const write = Boolean(writeFlag);
      const create = Boolean(createFlag);
      const truncate = Boolean(truncateFlag);
      const constants = fs.constants;
      let flags = 0;
      if (read && write) {
        flags |= constants.O_RDWR;
      } else if (write) {
        flags |= constants.O_WRONLY;
      } else {
        flags |= constants.O_RDONLY;
      }
      if (create) flags |= constants.O_CREAT;
      if (truncate) flags |= constants.O_TRUNC;

      const fd = fs.openSync(resolved, flags, 0o666);
      const handleId = `h${nextHandleId++}`;
      const bytes = encoder.encode(handleId);
      const alloc = allocHandleBytes(bytes);
      if (!alloc) {
        fs.closeSync(fd);
        return ERROR_UNEXPECTED;
      }
      if (!writeHandleStruct(outHandlePtr, alloc.ptr, alloc.len)) {
        fs.closeSync(fd);
        exports.albedo_free(alloc.ptr, alloc.len);
        return ERROR_UNEXPECTED;
      }
      handles.set(handleId, {
        fd,
        path: resolved,
        wasmPtr: alloc.ptr,
        wasmLen: alloc.len,
      });
      return ERROR_OK;
    } catch (err) {
      return mapFsError(err);
    }
  },
  wasm_close_file(handlePtr: number, handleLen: number): number {
    try {
      const handleId = decodeString(handlePtr, handleLen);
      const info = handles.get(handleId);
      if (!info) return ERROR_NOT_FOUND;
      fs.closeSync(info.fd);
      freeHandle(info);
      handles.delete(handleId);
      return ERROR_OK;
    } catch (err) {
      return mapFsError(err);
    }
  },
  wasm_pread(
    handlePtr: number,
    handleLen: number,
    offset: number | bigint,
    destPtr: number,
    destLen: number,
    outReadPtr: number
  ): number {
    try {
      const handleId = decodeString(handlePtr, handleLen);
      const info = handles.get(handleId);
      if (!info) return ERROR_NOT_FOUND;
      const buffer = getMemoryBuffer();
      if (!buffer) return ERROR_UNEXPECTED;
      const dest = Buffer.from(buffer, destPtr, destLen);
      const bytesRead = fs.readSync(
        info.fd,
        dest,
        0,
        destLen,
        toNumber(offset)
      );
      if (!writeUsize(outReadPtr, bytesRead)) return ERROR_UNEXPECTED;
      return ERROR_OK;
    } catch (err) {
      return mapFsError(err);
    }
  },
  wasm_pwrite(
    handlePtr: number,
    handleLen: number,
    offset: number | bigint,
    srcPtr: number,
    srcLen: number,
    outWrittenPtr: number
  ): number {
    try {
      const handleId = decodeString(handlePtr, handleLen);
      const info = handles.get(handleId);
      if (!info) return ERROR_NOT_FOUND;
      const buffer = getMemoryBuffer();
      if (!buffer) return ERROR_UNEXPECTED;
      const src = Buffer.from(buffer, srcPtr, srcLen);
      const bytesWritten = fs.writeSync(
        info.fd,
        src,
        0,
        srcLen,
        toNumber(offset)
      );
      if (!writeUsize(outWrittenPtr, bytesWritten)) return ERROR_UNEXPECTED;
      return ERROR_OK;
    } catch (err) {
      return mapFsError(err);
    }
  },
  wasm_sync(handlePtr: number, handleLen: number): number {
    try {
      const handleId = decodeString(handlePtr, handleLen);
      const info = handles.get(handleId);
      if (!info) return ERROR_NOT_FOUND;
      fs.fsyncSync(info.fd);
      return ERROR_OK;
    } catch (err) {
      return mapFsError(err);
    }
  },
  wasm_delete_file(pathPtr: number, pathLen: number): number {
    try {
      const filePath = decodeString(pathPtr, pathLen);
      const resolved = resolveFilePath(filePath);
      fs.unlinkSync(resolved);
      return ERROR_OK;
    } catch (err) {
      return mapFsError(err);
    }
  },
  wasm_rename_file(
    oldPtr: number,
    oldLen: number,
    newPtr: number,
    newLen: number
  ): number {
    try {
      const oldPath = resolveFilePath(decodeString(oldPtr, oldLen));
      const newPath = resolveFilePath(decodeString(newPtr, newLen));
      fs.renameSync(oldPath, newPath);
      return ERROR_OK;
    } catch (err) {
      return mapFsError(err);
    }
  },
  wasm_random_bytes(destPtr: number, destLen: number): number {
    try {
      const buffer = getMemoryBuffer();
      if (!buffer) return ERROR_UNEXPECTED;
      const dest = Buffer.from(buffer, destPtr, destLen);
      randomFillSync(dest);
      return ERROR_OK;
    } catch (err) {
      return mapFsError(err);
    }
  },
  wasm_now_seconds(outPtr: number): number {
    const seconds = BigInt(Math.floor(Date.now() / 1000));
    return writeI64(outPtr, seconds) ? ERROR_OK : ERROR_UNEXPECTED;
  },
};

const instance = await WebAssembly.instantiate(module, { env: envImports });
wasmInstance = instance;
wasmExports = wasmInstance.exports as WasmExports;
wasmMemory = wasmExports.memory;

console.log("Albedo WASM module loaded:", Object.keys(wasmInstance.exports));

export { wasmInstance };

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
  constructor(private pointer: number) {}

  insert(data: BSON.Document) {
    const dataBuf = BSON.serialize(data);
    const dataPtr = allocHandleBytes(dataBuf);
    if (!dataPtr) {
      throw new Error("Failed to allocate memory for Albedo insert");
    }
    const result = wasmExports!.albedo_insert(this.pointer, dataPtr.ptr);
    wasmExports!.albedo_free(dataPtr.ptr, dataPtr.len);
    if (result !== 0) {
      throw new Error("Failed to insert into Albedo database");
    }
  }

  static open(path: string) {
    const dbPtr = new BigUint64Array(1); // 4 bytes for a pointer on 32-bit, 8 bytes on 64-bit
    const dbPtrPtr = wasmExports!.albedo_malloc(4);
    const encodedPath = encoder.encode(path + "\0");
    const pathAlloc = allocHandleBytes(encodedPath);
    if (!pathAlloc) {
      throw new Error("Failed to allocate memory for Albedo open path");
    }
    const result = wasmExports!.albedo_open(pathAlloc.ptr, dbPtrPtr);

    if (result !== 0) {
      throw new Error("Failed to open Albedo database");
    }
    const pointer = readPtr(dbPtrPtr);
    return new Bucket(pointer); // Pass the actual pointer value
  }

  close() {
    const result = wasmExports!.albedo_close(this.pointer);
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
      const shouldQuit = yield BSON.deserialize(toBuffer(ptr, 0, size));

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

const bucket = Bucket.open(":memory:");

bucket.close();
