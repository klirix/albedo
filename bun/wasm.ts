import { join } from "path";
import fs from "fs";
import { randomFillSync } from "node:crypto";
import { BSON, ObjectId } from "bson";

const textDecoder = new TextDecoder();
const textEncoder = new TextEncoder();

type HandleInfo = {
  fd: number;
};

const handles = new Map<string, HandleInfo>();
let nextHandleId = 1;

let wasmInstance: WebAssembly.Instance | undefined;
let memory: WebAssembly.Memory | undefined;
let scratchBase = 0;
let scratchCapacity = 0;
const pageBytes = 64 * 1024;

function mapErrorCode(err: unknown): number {
  if (typeof err === "object" && err !== null) {
    const code = (err as { code?: string }).code;
    switch (code) {
      case "ENOENT":
        return 1;
      case "EACCES":
      case "EPERM":
        return 2;
      case "EEXIST":
        return 3;
    }
  }
  return 4;
}

function ensureMemory(): WebAssembly.Memory {
  if (!memory) {
    if (!wasmInstance) {
      throw new Error("WASM instance not initialised");
    }
    const exportedMemory = (wasmInstance.exports as Record<string, unknown>)
      .memory as WebAssembly.Memory | undefined;
    if (!exportedMemory) {
      throw new Error("WASM module did not export memory");
    }
    memory = exportedMemory;
    const currentBytes = memory.buffer.byteLength;
    scratchBase = currentBytes;
    memory.grow(1); // reserve one page as scratch
    scratchCapacity = pageBytes;
  }
  return memory!;
}

function readString(ptr: number, len: number): string {
  if (len === 0) return "";
  const mem = ensureMemory();
  const bytes = new Uint8Array(mem.buffer, ptr, len);
  return textDecoder.decode(bytes);
}

function readHandleKey(ptr: number, len: number): string | undefined {
  const key = readString(ptr, len);
  return handles.has(key) ? key : undefined;
}

function writeHandleString(value: string): { ptr: number; len: number } {
  const mem = ensureMemory();
  const encoded = textEncoder.encode(value);
  if (encoded.length > scratchCapacity) {
    const additionalPages = Math.ceil(
      (encoded.length - scratchCapacity) / pageBytes
    );
    mem.grow(additionalPages);
    scratchCapacity += additionalPages * pageBytes;
  }
  const target = new Uint8Array(mem.buffer, scratchBase, encoded.length);
  target.set(encoded);
  return { ptr: scratchBase, len: encoded.length };
}

function writeHandleUint8(value: Uint8Array): { ptr: number; len: number } {
  const mem = ensureMemory();
  const encoded = value;
  if (encoded.length > scratchCapacity) {
    const additionalPages = Math.ceil(
      (encoded.length - scratchCapacity) / pageBytes
    );
    mem.grow(additionalPages);
    scratchCapacity += additionalPages * pageBytes;
  }
  const target = new Uint8Array(mem.buffer, scratchBase, encoded.length);
  target.set(encoded);
  return { ptr: scratchBase, len: encoded.length };
}

function writeUint32(ptr: number, value: number) {
  const view = new DataView(ensureMemory().buffer);
  view.setUint32(ptr, value >>> 0, true);
}

function writeBigInt64(ptr: number, value: bigint) {
  const view = new DataView(ensureMemory().buffer);
  view.setBigInt64(ptr, value, true);
}

function registerHandle(fd: number): string {
  const handleId = `fd:${nextHandleId++}`;
  handles.set(handleId, { fd });
  return handleId;
}

function computeFlags(
  read: number,
  write: number,
  create: number,
  truncate: number
): number {
  const c = fs.constants;
  let canRead = read !== 0;
  let canWrite = write !== 0;
  if (create && !canWrite) {
    canWrite = true;
  }
  let flags = 0;
  if (canRead && canWrite) {
    flags |= c.O_RDWR;
  } else if (canWrite) {
    flags |= c.O_WRONLY;
  } else {
    flags |= c.O_RDONLY;
  }
  if (create) flags |= c.O_CREAT;
  if (truncate) flags |= c.O_TRUNC;
  return flags;
}

function wasm_open_file(
  pathPtr: number,
  pathLen: number,
  read: number,
  write: number,
  create: number,
  truncate: number,
  outHandlePtr: number
): number {
  try {
    const path = readString(pathPtr, pathLen);
    const flags = computeFlags(read, write, create, truncate);
    const fd = fs.openSync(path, flags, 0o666);
    const handleKey = registerHandle(fd);
    const handleBytes = writeHandleString(handleKey);
    writeUint32(outHandlePtr, handleBytes.ptr);
    writeUint32(outHandlePtr + 4, handleBytes.len);
    return 0;
  } catch (err) {
    return mapErrorCode(err);
  }
}

function getHandleInfo(handlePtr: number, handleLen: number) {
  const key = readHandleKey(handlePtr, handleLen);
  if (!key) return undefined;
  const info = handles.get(key);
  return info ? { key, info } : undefined;
}

function wasm_close_file(handlePtr: number, handleLen: number): number {
  try {
    const entry = getHandleInfo(handlePtr, handleLen);
    if (!entry) return 4;
    fs.closeSync(entry.info.fd);
    handles.delete(entry.key);
    return 0;
  } catch (err) {
    return mapErrorCode(err);
  }
}

function wasm_pread(
  handlePtr: number,
  handleLen: number,
  offset: number,
  destPtr: number,
  destLen: number,
  outReadPtr: number
): number {
  const entry = getHandleInfo(handlePtr, handleLen);
  if (!entry) return 4;
  try {
    const buffer = Buffer.from(ensureMemory().buffer, destPtr, destLen);
    const bytesRead = fs.readSync(
      entry.info.fd,
      buffer,
      0,
      destLen,
      Number(offset)
    );
    writeUint32(outReadPtr, bytesRead);
    return 0;
  } catch (err) {
    return mapErrorCode(err);
  }
}

function wasm_pwrite(
  handlePtr: number,
  handleLen: number,
  offset: number,
  srcPtr: number,
  srcLen: number,
  outWrittenPtr: number
): number {
  const entry = getHandleInfo(handlePtr, handleLen);
  if (!entry) return 4;
  try {
    const buffer = Buffer.from(ensureMemory().buffer, srcPtr, srcLen);
    const bytesWritten = fs.writeSync(
      entry.info.fd,
      buffer,
      0,
      srcLen,
      Number(offset)
    );
    writeUint32(outWrittenPtr, bytesWritten);
    return 0;
  } catch (err) {
    return mapErrorCode(err);
  }
}

function wasm_sync(handlePtr: number, handleLen: number): number {
  const entry = getHandleInfo(handlePtr, handleLen);
  if (!entry) return 4;
  try {
    fs.fsyncSync(entry.info.fd);
    return 0;
  } catch (err) {
    return mapErrorCode(err);
  }
}

function wasm_delete_file(pathPtr: number, pathLen: number): number {
  try {
    const path = readString(pathPtr, pathLen);
    fs.unlinkSync(path);
    return 0;
  } catch (err) {
    return mapErrorCode(err);
  }
}

function wasm_rename_file(
  oldPtr: number,
  oldLen: number,
  newPtr: number,
  newLen: number
): number {
  try {
    const oldPath = readString(oldPtr, oldLen);
    const newPath = readString(newPtr, newLen);
    fs.renameSync(oldPath, newPath);
    return 0;
  } catch (err) {
    return mapErrorCode(err);
  }
}

function wasm_random_bytes(destPtr: number, destLen: number): number {
  try {
    if (destLen === 0) return 0;
    const view = new Uint8Array(ensureMemory().buffer, destPtr, destLen);
    randomFillSync(view);
    return 0;
  } catch (err) {
    return mapErrorCode(err);
  }
}

function wasm_now_seconds(outPtr: number): number {
  try {
    const seconds = BigInt(Math.floor(Date.now() / 1000));
    writeBigInt64(outPtr, seconds);
    return 0;
  } catch (err) {
    return mapErrorCode(err);
  }
}

const imports = {
  env: {
    wasm_open_file,
    wasm_close_file,
    wasm_pread,
    wasm_pwrite,
    wasm_sync,
    wasm_delete_file,
    wasm_rename_file,
    wasm_random_bytes,
    wasm_now_seconds,
  },
};

const albedoWasm = await Bun.file(
  join(__dirname, "../zig-out/bin/albedo.wasm")
).arrayBuffer();
const module = await WebAssembly.compile(albedoWasm);
const instance = await WebAssembly.instantiate(module, imports);

wasmInstance = instance;

const albedo = instance.exports as Record<string, (...args: any[]) => any>;
ensureMemory();

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
  constructor(private pointer: bigint) {}

  insert(data: BSON.Document) {
    const dataBuf = BSON.serialize(data);
    const result = albedo.albedo_insert(this.pointer, dataBuf);
    if (result !== 0) {
      throw new Error("Failed to insert into Albedo database");
    }
  }

  static open(path: string) {
    const dbPtr = new BigUint64Array(1); // 8 bytes for a pointer
    // const dbPtrPtr = ptr(dbPtr);
    const result = albedo.albedo_open(Buffer.from(`${path}\0`), dbPtr);

    if (result !== 0) {
      throw new Error("Failed to open Albedo database");
    }
    const pointer = dbPtr[0];
    return new Bucket(pointer); // Pass the actual pointer value
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

console.log("Albedo WASM module loaded:", Object.keys(instance.exports));

Bucket.open(":memory:");
