/// <reference lib="dom" />

type WasmHandleInfo = {
  handle: any;
  path: string;
  wasmPtr: number;
  wasmLen: number;
};

const ERROR_OK = 0;
const ERROR_NOT_FOUND = 1;
const ERROR_PERMISSION = 2;
const ERROR_ALREADY_EXISTS = 3;
const ERROR_UNEXPECTED = 4;

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
      case "NotAllowedError":
      case "SecurityError":
        return ERROR_PERMISSION;
      default:
        return ERROR_UNEXPECTED;
    }
  }
  return ERROR_UNEXPECTED;
}

function splitPath(path: string): string[] {
  return path
    .split("/")
    .map((segment) => segment.trim())
    .filter((segment) => segment.length > 0 && segment !== ".");
}

async function resolveDirectory(
  root: FileSystemDirectoryHandle,
  segments: string[],
  options: { create: boolean }
): Promise<FileSystemDirectoryHandle> {
  let current = root;
  for (const segment of segments) {
    current = await current.getDirectoryHandle(segment, {
      create: options.create,
    });
  }
  return current;
}

async function resolveFileHandle(
  root: FileSystemDirectoryHandle,
  path: string,
  options: { create: boolean }
): Promise<FileSystemFileHandle> {
  const segments = splitPath(path);
  if (segments.length === 0) {
    throw new DOMException("Invalid path", "SyntaxError");
  }
  const filename = segments.pop()!;
  const directory = await resolveDirectory(root, segments, {
    create: options.create,
  });
  return directory.getFileHandle(filename, { create: options.create });
}

function runBlocking<T>(operation: () => Promise<T>): T {
  const shared = new SharedArrayBuffer(4);
  const view = new Int32Array(shared);
  let result: T | undefined;
  let error: unknown;

  operation()
    .then((value) => {
      result = value;
      Atomics.store(view, 0, 1);
      Atomics.notify(view, 0);
    })
    .catch((err) => {
      error = err;
      Atomics.store(view, 0, 2);
      Atomics.notify(view, 0);
    });

  Atomics.wait(view, 0, 0);

  const status = Atomics.load(view, 0);
  if (status === 2) {
    throw error ?? new Error("Unknown OPFS error");
  }
  return result as T;
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

export async function createBrowserEnvImports(helpers: BrowserEnvHelpers) {
  const root = await navigator.storage.getDirectory();
  const handles = new Map<string, WasmHandleInfo>();
  let nextHandleId = 1;
  const encoder = new TextEncoder();
  const decoder = new TextDecoder();

  function withFileHandle<T>(
    pathPtr: number,
    pathLen: number,
    fn: (path: string) => T
  ): T {
    const path = decodeString(helpers, pathPtr, pathLen, decoder);
    return fn(path);
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
        return withFileHandle(pathPtr, pathLen, (path) => {
          const create = Boolean(createFlag);
          const handle = runBlocking(async () => {
            const fileHandle = await resolveFileHandle(root, path, {
              create,
            });
            const access = await fileHandle.createSyncAccessHandle();
            if (!create && Boolean(readFlag) && !Boolean(writeFlag)) {
              // ensure file exists when read only
            }
            if (Boolean(truncateFlag)) {
              access.truncate(0);
            }
            return access;
          });

          const handleId = `h${nextHandleId++}`;
          const bytes = encoder.encode(handleId);
          const alloc = helpers.allocHandleBytes(bytes);
          if (!helpers.writeHandleStruct(outHandlePtr, alloc.ptr, alloc.len)) {
            handle.close();
            helpers.freeAlloc(alloc.ptr, alloc.len);
            return ERROR_UNEXPECTED;
          }
          handles.set(handleId, {
            handle,
            path,
            wasmPtr: alloc.ptr,
            wasmLen: alloc.len,
          });
          return ERROR_OK;
        });
      } catch (err) {
        return throwUnexpected(err);
      }
    },

    wasm_close_file(handlePtr: number, handleLen: number): number {
      try {
        const handleId = decodeString(helpers, handlePtr, handleLen, decoder);
        const info = handles.get(handleId);
        if (!info) return ERROR_NOT_FOUND;
        info.handle.close();
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
        const bytesRead = info.handle.read(dest, {
          at: helpers.toNumber(offset),
        });
        if (!helpers.writeUsize(outReadPtr, bytesRead)) return ERROR_UNEXPECTED;
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
        const bytesWritten = info.handle.write(src, {
          at: helpers.toNumber(offset),
        });
        if (!helpers.writeUsize(outWrittenPtr, bytesWritten))
          return ERROR_UNEXPECTED;
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
        info.handle.flush();
        return ERROR_OK;
      } catch (err) {
        return throwUnexpected(err);
      }
    },

    wasm_delete_file(pathPtr: number, pathLen: number): number {
      try {
        return withFileHandle(pathPtr, pathLen, (path) => {
          runBlocking(async () => {
            const segments = splitPath(path);
            if (segments.length === 0) {
              throw new DOMException("Invalid path", "SyntaxError");
            }
            const filename = segments.pop()!;
            const directory = await resolveDirectory(root, segments, {
              create: false,
            });
            await directory.removeEntry(filename);
          });
          return ERROR_OK;
        });
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
        runBlocking(async () => {
          const oldHandle = await resolveFileHandle(root, oldPath, {
            create: false,
          });
          const file = await oldHandle.getFile();
          const data = new Uint8Array(await file.arrayBuffer());

          const newHandle = await resolveFileHandle(root, newPath, {
            create: true,
          });
          const syncHandle = await newHandle.createSyncAccessHandle();
          try {
            syncHandle.truncate(0);
            syncHandle.write(data, { at: 0 });
            syncHandle.flush();
          } finally {
            syncHandle.close();
          }

          const oldSegments = splitPath(oldPath);
          const oldFile = oldSegments.pop()!;
          const oldDir = await resolveDirectory(root, oldSegments, {
            create: false,
          });
          await oldDir.removeEntry(oldFile);
        });
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
  };
}

export type BrowserEnvImports = Awaited<
  ReturnType<typeof createBrowserEnvImports>
>;
