import { BSON, type ObjectId } from "bson";
import { createNodeEnvImports } from "./node-imports";
import type { NodeEnvHelpers, NodeEnvImports } from "./node-imports";
import { createBrowserEnvImports } from "./browser-imports";
import type { BrowserEnvHelpers, BrowserEnvImports } from "./browser-imports";

const wasmUrl = new URL("./albedo.wasm", import.meta.url);

async function loadWasmBytes(url: URL): Promise<ArrayBuffer> {
  if (typeof Bun !== "undefined" && typeof Bun.file === "function") {
    return Bun.file(url).arrayBuffer();
  }
  if (typeof fetch === "function") {
    const response = await fetch(url);
    if (!response.ok) {
      throw new Error(`Failed to fetch WASM module (${response.status})`);
    }
    return response.arrayBuffer();
  }
  const fs = await import("fs/promises");
  const buffer = await fs.readFile(url);
  return buffer.buffer.slice(
    buffer.byteOffset,
    buffer.byteOffset + buffer.byteLength
  );
}

async function compileWasmModule(url: URL): Promise<WebAssembly.Module> {
  if (
    typeof WebAssembly.compileStreaming === "function" &&
    typeof fetch === "function"
  ) {
    try {
      return await WebAssembly.compileStreaming(fetch(url));
    } catch (err) {
      if (!(err instanceof TypeError)) {
        throw err;
      }
      // Fallback to buffered compilation below.
    }
  }
  const bytes = await loadWasmBytes(url);
  return WebAssembly.compile(bytes);
}

function isBrowserEnvironment(): boolean {
  return (
    typeof navigator !== "undefined" &&
    typeof navigator.storage?.getDirectory === "function"
  );
}

type EnvHelpers = NodeEnvHelpers & BrowserEnvHelpers;
type EnvImports = NodeEnvImports | BrowserEnvImports;

async function resolveEnvImports(helpers: EnvHelpers): Promise<EnvImports> {
  if (isBrowserEnvironment()) {
    return await createBrowserEnvImports(helpers);
  }
  return createNodeEnvImports(helpers);
}

const wasmModule = await compileWasmModule(wasmUrl);

const encoder = new TextEncoder();

type WasmExports = WebAssembly.Exports & {
  memory: WebAssembly.Memory;
  albedo_malloc: (size: number) => number;
  albedo_free: (ptr: number, size: number) => void;
  albedo_open: (pathPtr: number, bucketOutPtr: number) => number;
  albedo_close: (bucketHandle: number) => number;
  albedo_insert: (bucketHandle: number, dataPtr: number) => number;
  albedo_vacuum: (bucketHandle: number) => number;
  albedo_ensure_index: (
    bucketHandle: number,
    pathPtr: number,
    optionsByte: number
  ) => number;
  albedo_drop_index: (bucketHandle: number, pathPtr: number) => number;
  albedo_delete: (
    bucketHandle: number,
    queryPtr: number,
    queryLen: number
  ) => number;
  albedo_list: (
    bucketHandle: number,
    queryPtr: number,
    outIterPtr: number
  ) => number;
  albedo_data: (iterHandle: number, outDocPtr: number) => number;
  albedo_close_iterator: (iterHandle: number) => number;
  albedo_version: () => number;
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

function requireExports(): WasmExports {
  const exports = getWasmExports();
  if (!exports) {
    throw new Error("Albedo WASM exports not ready");
  }
  return exports;
}

function encodeCString(value: string): Uint8Array {
  const stringBytes = encoder.encode(value);
  const withNull = new Uint8Array(stringBytes.length + 1);
  withNull.set(stringBytes);
  withNull[withNull.length - 1] = 0;
  return withNull;
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

function readPtr(ptr: number): number {
  const buffer = getMemoryBuffer();
  if (!buffer) return 0;
  const view = new DataView(buffer);
  return view.getUint32(ptr, true);
}

function readU32(ptr: number): number {
  const buffer = getMemoryBuffer();
  if (!buffer) return 0;
  const view = new DataView(buffer);
  return view.getUint32(ptr, true);
}

function mallocZero(size: number): number {
  const exports = requireExports();
  const ptr = exports.albedo_malloc(size);
  if (ptr === 0) {
    throw new Error(`Failed to allocate ${size} bytes in WASM memory`);
  }
  const buffer = getMemoryBuffer();
  if (!buffer) {
    throw new Error("WASM memory unavailable");
  }
  new Uint8Array(buffer, ptr, size).fill(0);
  return ptr;
}

function cloneFromWasm(ptr: number, len: number): Uint8Array {
  const buffer = getMemoryBuffer();
  if (!buffer) {
    throw new Error("WASM memory unavailable");
  }
  const view = new Uint8Array(buffer, ptr, len);
  return new Uint8Array(view);
}

function toNumber(value: number | bigint): number {
  return typeof value === "bigint" ? Number(value) : value;
}

function allocHandleBytes(bytes: Uint8Array): { ptr: number; len: number } {
  const exports = requireExports();
  const ptr = exports.albedo_malloc(bytes.length);
  if (ptr === 0) {
    throw new Error("Failed to allocate WASM memory");
  }
  if (!writeBytes(ptr, bytes)) {
    exports.albedo_free(ptr, bytes.length);
    throw new Error("Failed to copy data into WASM memory");
  }
  return { ptr, len: bytes.length };
}

function freeAlloc(ptr: number, len: number): void {
  if (ptr === 0 || len === 0) return;
  const exports = requireExports();
  exports.albedo_free(ptr, len);
}


const envHelpers: EnvHelpers = {
  getMemoryBuffer,
  writeHandleStruct,
  writeUsize,
  writeI64,
  allocHandleBytes,
  freeAlloc,
  toNumber,
};

const envImports = await resolveEnvImports(envHelpers);

const instance = await WebAssembly.instantiate(wasmModule, { env: envImports });
wasmInstance = instance;
wasmExports = wasmInstance.exports as WasmExports;
wasmMemory = wasmExports.memory;

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
    const exports = requireExports();
    const dataBuf = BSON.serialize(data);
    const dataBytes =
      dataBuf instanceof Uint8Array ? dataBuf : new Uint8Array(dataBuf);
    const dataPtr = allocHandleBytes(dataBytes);
    try {
      const result = exports.albedo_insert(this.pointer, dataPtr.ptr);
      if (result !== ResultCode.OK) {
        throw new Error("Failed to insert into Albedo database");
      }
    } finally {
      freeAlloc(dataPtr.ptr, dataPtr.len);
    }
  }

  static open(path: string) {
    const exports = requireExports();
    const dbPtrPtr = mallocZero(4);
    const pathAlloc = allocHandleBytes(encodeCString(path));
    let bucketPtr = 0;
    try {
      const result = exports.albedo_open(pathAlloc.ptr, dbPtrPtr);
      if (result !== ResultCode.OK) {
        throw new Error("Failed to open Albedo database");
      }
      bucketPtr = readPtr(dbPtrPtr);
      if (bucketPtr === 0) {
        throw new Error("Received null bucket pointer from WASM module");
      }
    } finally {
      freeAlloc(pathAlloc.ptr, pathAlloc.len);
      freeAlloc(dbPtrPtr, 4);
    }
    return new Bucket(bucketPtr);
  }

  close() {
    const exports = requireExports();
    const result = exports.albedo_close(this.pointer);
    if (result !== ResultCode.OK) {
      throw new Error("Failed to close Albedo database");
    }
  }

  vacuum() {
    const exports = requireExports();
    const result = exports.albedo_vacuum(this.pointer);
    if (result !== ResultCode.OK) {
      throw new Error("Failed to vacuum Albedo database");
    }
  }

  static defaultIndexOptions = {
    unique: false,
    sparse: false,
    reverse: false,
  };

  ensureIndex(field: string, options = Bucket.defaultIndexOptions) {
    const exports = requireExports();
    let optionFlags = 0;
    if (options.unique) optionFlags |= 1 << 0;
    if (options.sparse) optionFlags |= 1 << 1;
    if (options.reverse) optionFlags |= 1 << 2;

    const pathAlloc = allocHandleBytes(encodeCString(field));
    try {
      const res = exports.albedo_ensure_index(
        this.pointer,
        pathAlloc.ptr,
        optionFlags
      );
      if (res !== ResultCode.OK) {
        throw new Error("Failed to create index in Albedo database");
      }
    } finally {
      freeAlloc(pathAlloc.ptr, pathAlloc.len);
    }
  }

  dropIndex(field: string) {
    const exports = requireExports();
    const pathAlloc = allocHandleBytes(encodeCString(field));
    try {
      const res = exports.albedo_drop_index(this.pointer, pathAlloc.ptr);
      if (res === ResultCode.OK) {
        return true;
      }
      if (res === ResultCode.NotFound) {
        return false;
      }
      throw new Error("Failed to drop index in Albedo database");
    } finally {
      freeAlloc(pathAlloc.ptr, pathAlloc.len);
    }
  }

  delete(query: Query["query"], _options: { sector?: Query["sector"] } = {}) {
    const exports = requireExports();
    const queryBuf = BSON.serialize({ query });
    const queryBytes =
      queryBuf instanceof Uint8Array ? queryBuf : new Uint8Array(queryBuf);
    if (queryBytes.length > 0xffff) {
      throw new Error("Query payload too large for WASM bridge");
    }
    const queryPtr = allocHandleBytes(queryBytes);
    try {
      const result = exports.albedo_delete(
        this.pointer,
        queryPtr.ptr,
        queryBytes.length
      );
      if (result !== ResultCode.OK) {
        throw new Error("Failed to delete from Albedo database");
      }
    } finally {
      freeAlloc(queryPtr.ptr, queryPtr.len);
    }
  }

  *list(
    query: Query["query"] = {},
    options: {
      sort?: Query["sort"];
      sector?: Query["sector"];
      projection?: Query["projection"];
    } = {}
  ): Generator<BSON.Document, void, boolean | undefined> {
    const exports = requireExports();
    const finalQuery: Query = { query };
    if (options.sort) finalQuery.sort = options.sort;
    if (options.sector) finalQuery.sector = options.sector;
    if (options.projection) finalQuery.projection = options.projection;

    const queryBuf = BSON.serialize(finalQuery);
    const queryBytes =
      queryBuf instanceof Uint8Array ? queryBuf : new Uint8Array(queryBuf);
    const queryAlloc = allocHandleBytes(queryBytes);
    const iterPtrPtr = mallocZero(4);
    let iterHandle = 0;
    try {
      const res = exports.albedo_list(this.pointer, queryAlloc.ptr, iterPtrPtr);
      if (res !== ResultCode.OK) {
        throw new Error("Failed to list Albedo database");
      }
      iterHandle = readPtr(iterPtrPtr);
      if (iterHandle === 0) {
        throw new Error("Received null iterator handle from WASM module");
      }
    } finally {
      freeAlloc(queryAlloc.ptr, queryAlloc.len);
      freeAlloc(iterPtrPtr, 4);
    }

    const dataPtrPtr = mallocZero(4);
    try {
      while (true) {
        const res = exports.albedo_data(iterHandle, dataPtrPtr);
        if (res === ResultCode.EOS) {
          break;
        }
        if (res !== ResultCode.OK) {
          throw new Error("Failed to get data from Albedo database");
        }
        const docPtr = readPtr(dataPtrPtr);
        if (docPtr === 0) {
          throw new Error("Received null document pointer from WASM module");
        }
        const docLen = readU32(docPtr);
        const docBytes = cloneFromWasm(docPtr, docLen);
        const shouldQuit = yield BSON.deserialize(docBytes);
        if (shouldQuit) {
          break;
        }
      }
    } finally {
      if (iterHandle !== 0) {
        exports.albedo_close_iterator(iterHandle);
      }
      freeAlloc(dataPtrPtr, 4);
    }
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
    return requireExports().albedo_version();
  },
};

export { createNodeEnvImports } from "./node-imports";
export type { NodeEnvHelpers, NodeEnvImports } from "./node-imports";
export { createBrowserEnvImports } from "./browser-imports";
export type { BrowserEnvHelpers, BrowserEnvImports } from "./browser-imports";
