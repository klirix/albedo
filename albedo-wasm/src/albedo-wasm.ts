import { serialize, deserialize, type ObjectId } from "./bson";
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
  albedo_transform: (
    bucketHandle: number,
    queryPtr: number,
    outIterPtr: number
  ) => number;
  albedo_transform_data: (iterHandle: number, outDocPtr: number) => number;
  albedo_transform_apply: (iterHandle: number, transformPtr: number) => number;
  albedo_transform_close: (iterHandle: number) => number;
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
  exports: WasmExports;

  constructor(private pointer: number) {
    this.exports = requireExports();
  }

  get memory() {
    const dv = new DataView(this.exports.memory.buffer);
    return dv;
  }

  insert(data: any) {
    const dataBuf = serialize(data);
    const dataBytes =
      dataBuf instanceof Uint8Array ? dataBuf : new Uint8Array(dataBuf);
    const dataPtr = this.exports.albedo_malloc(dataBytes.length);
    new Uint8Array(this.exports.memory.buffer, dataPtr, dataBytes.length).set(
      dataBytes
    );
    try {
      const result = this.exports.albedo_insert(this.pointer, dataPtr);
      if (result !== ResultCode.OK) {
        throw new Error("Failed to insert into Albedo database");
      }
    } finally {
      freeAlloc(dataPtr, dataBytes.length);
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
    const exports = this.exports;
    const queryBuf = serialize({ query });
    const queryBytes =
      queryBuf instanceof Uint8Array ? queryBuf : new Uint8Array(queryBuf);
    if (queryBytes.length > 0xffff) {
      throw new Error("Query payload too large for WASM bridge");
    }
    const len = queryBytes.length;
    const queryPtr = exports.albedo_malloc(len);

    new Uint8Array(this.memory.buffer, queryPtr, len).set(queryBytes);

    try {
      const result = exports.albedo_delete(this.pointer, queryPtr, len);
      if (result !== ResultCode.OK) {
        throw new Error("Failed to delete from Albedo database");
      }
    } finally {
      exports.albedo_free(queryPtr, len);
    }
  }

  *list(
    query: Query["query"] = {},
    options: {
      sort?: Query["sort"];
      sector?: Query["sector"];
      projection?: Query["projection"];
    } = {}
  ): Generator<any, void, boolean | undefined> {
    const exports = this.exports;
    const finalQuery: Query = { query };
    if (options.sort) finalQuery.sort = options.sort;
    if (options.sector) finalQuery.sector = options.sector;
    if (options.projection) finalQuery.projection = options.projection;

    const queryBuf = serialize(finalQuery);
    const queryBytes =
      queryBuf instanceof Uint8Array ? queryBuf : new Uint8Array(queryBuf);
    const len = queryBytes.length;
    const queryPtr = exports.albedo_malloc(len);

    new Uint8Array(this.memory.buffer, queryPtr, len).set(queryBytes);
    const iterPtrPtr = this.exports.albedo_malloc(4);
    let iterHandle = 0;
    try {
      const res = exports.albedo_list(this.pointer, queryPtr, iterPtrPtr);
      if (res !== ResultCode.OK) {
        throw new Error("Failed to list Albedo database");
      }
      iterHandle = this.memory.getUint32(iterPtrPtr, true);
      if (iterHandle === 0) {
        throw new Error("Received null iterator handle from WASM module");
      }
    } finally {
      exports.albedo_free(queryPtr, len);
      exports.albedo_free(iterPtrPtr, 4);
    }

    const dataPtrPtr = this.exports.albedo_malloc(4);
    try {
      while (true) {
        const res = exports.albedo_data(iterHandle, dataPtrPtr);
        if (res === ResultCode.EOS) {
          break;
        }
        if (res !== ResultCode.OK) {
          throw new Error("Failed to get data from Albedo database");
        }
        const docPtr = this.memory.getUint32(dataPtrPtr, true);
        if (docPtr === 0) {
          throw new Error("Received null document pointer from WASM module");
        }
        const docLen = this.memory.getInt32(docPtr, true);
        const shouldQuit = yield deserialize(
          new Uint8Array(this.memory.buffer, docPtr, docLen)
        );
        if (shouldQuit) {
          break;
        }
      }
    } finally {
      if (iterHandle !== 0) {
        exports.albedo_close_iterator(iterHandle);
      }
      exports.albedo_free(dataPtrPtr, 4);
    }
  }

  all(query: Query["query"] = {}, options: Query = {}) {
    return this.list(query, options).toArray();
  }

  get(query: Query["query"], options: Query = {}) {
    const result = this.list(query, options).next(true);
    if (result.done) {
      return null;
    }
    return result.value;
  }

  *transformCursor(
    query: Query["query"] = {}
  ): Generator<any, void, any | null | undefined> {
    const exports = this.exports;
    const finalQuery: Query = { query };

    const queryBuf = serialize(finalQuery);
    const queryBytes =
      queryBuf instanceof Uint8Array ? queryBuf : new Uint8Array(queryBuf);
    const len = queryBytes.length;
    const queryPtr = exports.albedo_malloc(len);

    new Uint8Array(this.memory.buffer, queryPtr, len).set(queryBytes);
    const iterPtrPtr = this.exports.albedo_malloc(4);
    let iterHandle = 0;
    try {
      const res = exports.albedo_transform(this.pointer, queryPtr, iterPtrPtr);
      if (res !== ResultCode.OK) {
        throw new Error("Failed to create transform iterator");
      }
      iterHandle = this.memory.getUint32(iterPtrPtr, true);
      if (iterHandle === 0) {
        throw new Error("Received null iterator handle from WASM module");
      }
    } finally {
      exports.albedo_free(queryPtr, len);
      exports.albedo_free(iterPtrPtr, 4);
    }

    const dataPtrPtr = this.exports.albedo_malloc(4);
    try {
      while (true) {
        // Get the current document
        const res = exports.albedo_transform_data(iterHandle, dataPtrPtr);
        if (res === ResultCode.EOS) {
          break;
        }
        if (res !== ResultCode.OK) {
          throw new Error("Failed to get data from transform iterator");
        }
        const docPtr = this.memory.getUint32(dataPtrPtr, true);
        if (docPtr === 0) {
          throw new Error("Received null document pointer from WASM module");
        }
        const docLen = this.memory.getInt32(docPtr, true);
        const currentDoc = deserialize(
          new Uint8Array(this.memory.buffer, docPtr, docLen)
        );

        // Yield the document and get the transformation from the user
        const transformDoc = yield currentDoc;

        // Apply the transformation based on what the user provided
        let transformPtr = 0;
        let transformLen = 0;

        if (transformDoc === undefined) {
          // Pass the existing document pointer
          transformPtr = docPtr;
        } else if (transformDoc === null) {
          // Pass null pointer (0)
          transformPtr = 0;
        } else {
          // Serialize the new document and allocate memory for it
          const transformBuf = serialize(transformDoc);
          const transformBytes =
            transformBuf instanceof Uint8Array
              ? transformBuf
              : new Uint8Array(transformBuf);
          transformLen = transformBytes.length;
          transformPtr = exports.albedo_malloc(transformLen);
          new Uint8Array(this.memory.buffer, transformPtr, transformLen).set(
            transformBytes
          );
        }

        // Apply the transformation
        try {
          const applyRes = exports.albedo_transform_apply(
            iterHandle,
            transformPtr
          );
          if (applyRes !== ResultCode.OK) {
            throw new Error("Failed to apply transformation");
          }
        } finally {
          // Free allocated memory for new document (but not for null or undefined cases)
          if (transformDoc !== undefined && transformDoc !== null) {
            exports.albedo_free(transformPtr, transformLen);
          }
        }
      }
    } finally {
      if (iterHandle !== 0) {
        exports.albedo_transform_close(iterHandle);
      }
      exports.albedo_free(dataPtrPtr, 4);
    }
  }

  transform(
    query: Query["query"] = {},
    mutator: (doc: any) => any | null | undefined
  ) {
    const cursor = this.transformCursor(query);
    let doc = cursor.next().value;
    while (doc) {
      const transformed = mutator(doc);
      doc = cursor.next(transformed).value;
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
