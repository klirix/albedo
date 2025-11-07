/// <reference lib="dom" />

/**
 * Browser environment imports for Albedo WASM using localStorage
 *
 * This implementation uses localStorage instead of OPFS, making it:
 * - ✅ Synchronous (no async/await needed)
 * - ✅ Works on main thread (no Web Worker required)
 * - ✅ No SharedArrayBuffer or Atomics needed
 * - ✅ Works in all modern browsers
 *
 * Limitations:
 * - localStorage typically has ~5-10MB limit per origin
 * - Slower than OPFS for large files
 * - Data stored as base64 (slightly larger)
 * - Best for small databases or development/testing
 *
 * Storage format:
 * - Files stored as base64 strings in localStorage
 * - Key format: "albedo_file:{path}"
 * - Each file stored as a single item
 */

type WasmHandleInfo = {
  path: string;
  data: Uint8Array;
  wasmPtr: number;
  wasmLen: number;
  dirty: boolean;
};

const ERROR_OK = 0;
const ERROR_NOT_FOUND = 1;
const ERROR_PERMISSION = 2;
const ERROR_ALREADY_EXISTS = 3;
const ERROR_UNEXPECTED = 4;

const STORAGE_PREFIX = "albedo_file:";

export type BrowserEnvHelpers = {
  getMemoryBuffer: () => ArrayBuffer | null;
  writeHandleStruct: (
    outPtr: number,
    handlePtr: number,
    handleLen: number
  ) => boolean;
  writeUsize: (outPtr: number, value: number) => boolean;
  writeI64: (outPtr: number, value: bigint) => boolean;
  allocHandleBytes: (bytes: Uint8Array) => { ptr: number; len: number };
  freeAlloc: (ptr: number, len: number) => void;
  toNumber: (value: number | bigint) => number;
};

function throwUnexpected(err: unknown): number {
  if (err instanceof DOMException) {
    switch (err.name) {
      case "NotFoundError":
        return ERROR_NOT_FOUND;
      case "QuotaExceededError":
        console.error("localStorage quota exceeded");
        return ERROR_UNEXPECTED;
      case "SecurityError":
        return ERROR_PERMISSION;
      default:
        console.error("localStorage error:", err);
        return ERROR_UNEXPECTED;
    }
  }
  console.error("Unexpected error:", err);
  return ERROR_UNEXPECTED;
}

function storageKey(path: string): string {
  return STORAGE_PREFIX + path;
}

function fileExists(path: string): boolean {
  return localStorage.getItem(storageKey(path)) !== null;
}

function readFile(path: string): Uint8Array | null {
  const base64 = localStorage.getItem(storageKey(path));
  if (base64 === null) return null;

  // Decode base64 to Uint8Array
  const binaryString = atob(base64);
  const bytes = new Uint8Array(binaryString.length);
  for (let i = 0; i < binaryString.length; i++) {
    bytes[i] = binaryString.charCodeAt(i);
  }
  return bytes;
}

function writeFile(path: string, data: Uint8Array): void {
  // Encode Uint8Array to base64
  let binaryString = "";
  for (let i = 0; i < data.length; i++) {
    const byte = data[i];
    if (byte !== undefined) {
      binaryString += String.fromCharCode(byte);
    }
  }
  const base64 = btoa(binaryString);
  localStorage.setItem(storageKey(path), base64);
}

function deleteFile(path: string): void {
  localStorage.removeItem(storageKey(path));
}

function decodeString(
  helpers: BrowserEnvHelpers,
  ptr: number,
  len: number,
  decoder: TextDecoder
): string {
  const buffer = helpers.getMemoryBuffer();
  if (!buffer) return "";
  return decoder.decode(new Uint8Array(buffer, ptr, len));
}

function memorySlice(
  helpers: BrowserEnvHelpers,
  ptr: number,
  len: number
): Uint8Array | null {
  const buffer = helpers.getMemoryBuffer();
  if (!buffer) return null;
  return new Uint8Array(buffer, ptr, len);
}

export function createBrowserEnvImports(helpers: BrowserEnvHelpers) {
  const handles = new Map<string, WasmHandleInfo>();
  let nextHandleId = 1;
  const encoder = new TextEncoder();
  const decoder = new TextDecoder();

  return {
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
        const path = decodeString(helpers, pathPtr, pathLen, decoder);
        const create = Boolean(createFlag);
        const truncate = Boolean(truncateFlag);
        const exists = fileExists(path);

        if (!exists && !create) {
          return ERROR_NOT_FOUND;
        }

        if (exists && !create && truncate) {
          // Truncate existing file
          writeFile(path, new Uint8Array(0));
        }

        let data: Uint8Array;
        if (!exists || truncate) {
          data = new Uint8Array(0);
          if (create) {
            writeFile(path, data);
          }
        } else {
          data = readFile(path) || new Uint8Array(0);
        }

        const handleId = `h${nextHandleId++}`;
        const bytes = encoder.encode(handleId);
        const alloc = helpers.allocHandleBytes(bytes);

        if (!helpers.writeHandleStruct(outHandlePtr, alloc.ptr, alloc.len)) {
          helpers.freeAlloc(alloc.ptr, alloc.len);
          return ERROR_UNEXPECTED;
        }

        handles.set(handleId, {
          path,
          data,
          wasmPtr: alloc.ptr,
          wasmLen: alloc.len,
          dirty: false,
        });

        return ERROR_OK;
      } catch (err) {
        return throwUnexpected(err);
      }
    },

    wasm_close_file(handlePtr: number, handleLen: number): number {
      try {
        const handleId = decodeString(helpers, handlePtr, handleLen, decoder);
        const info = handles.get(handleId);
        if (!info) return ERROR_NOT_FOUND;

        // Write back to localStorage if dirty
        if (info.dirty) {
          writeFile(info.path, info.data);
        }

        helpers.freeAlloc(info.wasmPtr, info.wasmLen);
        handles.delete(handleId);
        return ERROR_OK;
      } catch (err) {
        return throwUnexpected(err);
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
        const handleId = decodeString(helpers, handlePtr, handleLen, decoder);
        const info = handles.get(handleId);
        if (!info) return ERROR_NOT_FOUND;

        const dest = memorySlice(helpers, destPtr, destLen);
        if (!dest) return ERROR_UNEXPECTED;

        const offsetNum = helpers.toNumber(offset);
        const availableBytes = Math.max(0, info.data.length - offsetNum);
        const bytesToRead = Math.min(destLen, availableBytes);

        if (bytesToRead > 0) {
          dest.set(info.data.subarray(offsetNum, offsetNum + bytesToRead));
        }

        if (!helpers.writeUsize(outReadPtr, bytesToRead))
          return ERROR_UNEXPECTED;
        return ERROR_OK;
      } catch (err) {
        return throwUnexpected(err);
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
        const handleId = decodeString(helpers, handlePtr, handleLen, decoder);
        const info = handles.get(handleId);
        if (!info) return ERROR_NOT_FOUND;

        const src = memorySlice(helpers, srcPtr, srcLen);
        if (!src) return ERROR_UNEXPECTED;

        const offsetNum = helpers.toNumber(offset);
        const requiredSize = offsetNum + srcLen;

        // Resize data array if necessary
        if (info.data.length < requiredSize) {
          const newData = new Uint8Array(requiredSize);
          newData.set(info.data);
          info.data = newData;
        }

        // Copy data
        info.data.set(src, offsetNum);
        info.dirty = true;

        if (!helpers.writeUsize(outWrittenPtr, srcLen)) return ERROR_UNEXPECTED;
        return ERROR_OK;
      } catch (err) {
        return throwUnexpected(err);
      }
    },

    wasm_sync(handlePtr: number, handleLen: number): number {
      try {
        const handleId = decodeString(helpers, handlePtr, handleLen, decoder);
        const info = handles.get(handleId);
        if (!info) return ERROR_NOT_FOUND;

        if (info.dirty) {
          writeFile(info.path, info.data);
          info.dirty = false;
        }

        return ERROR_OK;
      } catch (err) {
        return throwUnexpected(err);
      }
    },

    wasm_delete_file(pathPtr: number, pathLen: number): number {
      try {
        const path = decodeString(helpers, pathPtr, pathLen, decoder);
        if (!fileExists(path)) {
          return ERROR_NOT_FOUND;
        }
        deleteFile(path);
        return ERROR_OK;
      } catch (err) {
        return throwUnexpected(err);
      }
    },

    wasm_rename_file(
      oldPtr: number,
      oldLen: number,
      newPtr: number,
      newLen: number
    ): number {
      try {
        const oldPath = decodeString(helpers, oldPtr, oldLen, decoder);
        const newPath = decodeString(helpers, newPtr, newLen, decoder);

        if (!fileExists(oldPath)) {
          return ERROR_NOT_FOUND;
        }

        const data = readFile(oldPath);
        if (data === null) {
          return ERROR_NOT_FOUND;
        }

        writeFile(newPath, data);
        deleteFile(oldPath);

        return ERROR_OK;
      } catch (err) {
        return throwUnexpected(err);
      }
    },

    wasm_random_bytes(destPtr: number, destLen: number): number {
      const dest = memorySlice(helpers, destPtr, destLen);
      if (!dest) return ERROR_UNEXPECTED;
      crypto.getRandomValues(dest);
      return ERROR_OK;
    },

    wasm_now_seconds(outPtr: number): number {
      const seconds = BigInt(Math.floor(Date.now() / 1000));
      return helpers.writeI64(outPtr, seconds) ? ERROR_OK : ERROR_UNEXPECTED;
    },

    wasm_log(msgPtr: number, msgLen: number): void {
      const msg = decodeString(helpers, msgPtr, msgLen, decoder);
      console.log(`[WASM]: ${msg}`);
    },
  };
}

export type BrowserEnvImports = ReturnType<typeof createBrowserEnvImports>;

/**
 * Utility function to clear all Albedo files from localStorage
 */
export function clearAlbedoFiles(): void {
  const keysToRemove: string[] = [];
  for (let i = 0; i < localStorage.length; i++) {
    const key = localStorage.key(i);
    if (key && key.startsWith(STORAGE_PREFIX)) {
      keysToRemove.push(key);
    }
  }
  keysToRemove.forEach((key) => localStorage.removeItem(key));
}

/**
 * Utility function to list all Albedo files in localStorage
 */
export function listAlbedoFiles(): string[] {
  const files: string[] = [];
  for (let i = 0; i < localStorage.length; i++) {
    const key = localStorage.key(i);
    if (key && key.startsWith(STORAGE_PREFIX)) {
      files.push(key.substring(STORAGE_PREFIX.length));
    }
  }
  return files;
}

/**
 * Utility function to get storage usage
 */
export function getStorageUsage(): { used: number; files: number } {
  let used = 0;
  let files = 0;
  for (let i = 0; i < localStorage.length; i++) {
    const key = localStorage.key(i);
    if (key && key.startsWith(STORAGE_PREFIX)) {
      const value = localStorage.getItem(key);
      if (value) {
        used += value.length;
        files++;
      }
    }
  }
  return { used, files };
}
