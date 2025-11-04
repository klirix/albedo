import * as fs from "fs";
import path from "path";
import { randomFillSync } from "crypto";

type WasmHandleInfo = {
  fd: number;
  path: string;
  wasmPtr: number;
  wasmLen: number;
};

const ERROR_OK = 0;
const ERROR_NOT_FOUND = 1;
const ERROR_PERMISSION = 2;
const ERROR_ALREADY_EXISTS = 3;
const ERROR_UNEXPECTED = 4;

export type NodeEnvHelpers = {
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

function mapFsError(err: unknown): number {
  if (!(err instanceof Error)) {
    return ERROR_UNEXPECTED;
  }
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

export function createNodeEnvImports(helpers: NodeEnvHelpers) {
  const handles = new Map<string, WasmHandleInfo>();
  let nextHandleId = 1;
  const encoder = new TextEncoder();
  const decoder = new TextDecoder();

  function decodeString(ptr: number, len: number): string {
    const buffer = helpers.getMemoryBuffer();
    if (!buffer) return "";
    return decoder.decode(new Uint8Array(buffer, ptr, len));
  }

  function memoryBufferSlice(ptr: number, len: number): Buffer | null {
    const buffer = helpers.getMemoryBuffer();
    if (!buffer) return null;
    return Buffer.from(buffer, ptr, len);
  }

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
        let alloc: { ptr: number; len: number };
        try {
          alloc = helpers.allocHandleBytes(bytes);
        } catch (_err) {
          fs.closeSync(fd);
          return ERROR_UNEXPECTED;
        }

        if (!helpers.writeHandleStruct(outHandlePtr, alloc.ptr, alloc.len)) {
          fs.closeSync(fd);
          helpers.freeAlloc(alloc.ptr, alloc.len);
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
        helpers.freeAlloc(info.wasmPtr, info.wasmLen);
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
        const dest = memoryBufferSlice(destPtr, destLen);
        if (!dest) return ERROR_UNEXPECTED;
        const bytesRead = fs.readSync(
          info.fd,
          dest,
          0,
          destLen,
          helpers.toNumber(offset)
        );
        if (!helpers.writeUsize(outReadPtr, bytesRead)) return ERROR_UNEXPECTED;
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
        const src = memoryBufferSlice(srcPtr, srcLen);
        if (!src) return ERROR_UNEXPECTED;
        const bytesWritten = fs.writeSync(
          info.fd,
          src,
          0,
          srcLen,
          helpers.toNumber(offset)
        );
        if (!helpers.writeUsize(outWrittenPtr, bytesWritten))
          return ERROR_UNEXPECTED;
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
        const dest = memoryBufferSlice(destPtr, destLen);
        if (!dest) return ERROR_UNEXPECTED;
        randomFillSync(dest);
        return ERROR_OK;
      } catch (err) {
        return mapFsError(err);
      }
    },

    wasm_now_seconds(outPtr: number): number {
      const seconds = BigInt(Math.floor(Date.now() / 1000));
      return helpers.writeI64(outPtr, seconds) ? ERROR_OK : ERROR_UNEXPECTED;
    },
  };
}

export type NodeEnvImports = ReturnType<typeof createNodeEnvImports>;
