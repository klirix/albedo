## Quick orientation

Albedo is a small experimental document store written in Zig with two main
consumption paths: a C-like FFI (built as a shared/static library) and a
Node/Bun binding. Primary storage is page-based BSON documents with B+ tree
indexes. The codebase is intentionally small and favors explicit, manual
memory and file layout operations.

Keep these places in mind when exploring or making changes:
- `src/albedo.zig` — core bucket, pages, document layout, insert/list/delete,
  meta page (page 0) stores index registry.
- `src/lib.zig` — C-style exported API used by the Bun FFI wrapper.
- `src/napi.zig` — Node/N-API exports via `napigen` (JavaScript bindings).
- `src/*.zig` (other files) — `bplusindex.zig`, `bson.zig`, `query.zig`, etc.
- `bun/` — example Bun consumer: `bun/albedo.ts` (ffi) and `albedo-napi.ts`
  (N-API/napigen example).

## Big-picture architecture
- Storage model: fixed page size (DEFAULT_PAGE_SIZE = 8192) with a 64-byte
  bucket header at file start and 32-byte page headers. Page types: Data,
  Index, Free, Meta. The meta page (page 0) serializes index descriptors.
- Indexes are loaded from the meta page and stored in `Bucket.indexes`
  (StringHashMap). Index creation/recording flows go through `ensureIndex`
  and `recordIndexes()`.
- Memory model: the code uses explicit allocators (often `ArenaAllocator` or
  the provided allocator). Many functions accept an allocator or create
  per-request arenas — prefer preserving or forwarding the allocator.
- Node/Bun integration: two options
  - Direct FFI (Bun `dlopen`) that loads `libalbedo.(dylib|so)` and calls
    `albedo_*` exported functions (see `bun/albedo.ts`).
  - N-API binding built with `napigen` (`src/napi.zig`) and exposed as
    `libalbedo.node` (use `zig build -Dnode=true`).

## Build & test commands (use these exact invocations)
- Build default shared library (native):

  ```sh
  zig build
  ```

- Build Node (N-API) extension:

  ```sh
  zig build -Dnode=true
  ```

- Build static library:

  ```sh
  zig build -Dstatic=true
  ```

- Run unit tests (Zig tests are embedded in sources):

  ```sh
  zig test src/albedo.zig
  # or run the build system test step:
  zig build test
  ```

- Bun example (after building shared lib):

  ```sh
  cd bun
  bun install
  bun run index.ts
  ```

Artifacts appear in `zig-out/` (`zig-out/lib/libalbedo.dylib`, and
`zig-out/bin/*` for tests). The Bun example expects `libalbedo.${suffix}` in
`bun/` (see `bun/index.ts` and `dlopen` usage in `bun/albedo.ts`).

## Project-specific conventions & gotchas
- File layout offsets matter: first 64 bytes = `BucketHeader`, pages start
  immediately after. Be careful with read/write offsets and page boundary
  arithmetic (see `Bucket.loadPage` / `writePage`).
- Meta page (page id 0) is authoritative for index registration. Updates to
  indexes must call `recordIndexes()` to persist new index metadata.
- Do not assume automatic GC: many buffers are allocated with explicit
  allocators and must be freed (look for `defer allocator.free(...)` patterns
  and `deinit()` implementations). When adding APIs that return pointers to
  internal buffers, follow existing patterns (Arena for short-lived, owned
  buffers for long-lived results).
- Concurrency: `Bucket` uses an `RwLock` around operations that mutate or
  read pages — prefer using `lock()`/`unlock()` or `lockShared()` where code
  expects it.
- Error handling: Zig error sets are used heavily. When wrapping errors for
  FFI/N-API consumers, map to the `Result` enum in `src/lib.zig` or follow
  `napigen` patterns in `src/napi.zig`.

## Typical tasks & where to start
- Add a storage change (page layout, header): inspect `PageHeader`,
  `BucketHeader` in `src/albedo.zig` and update read/write helpers and tests.
- Add a query or index feature: update `query.zig`, ensure index metadata
  serialization in `recordIndexes()` and loading in `loadIndices()`.
- Add JS API surface: prefer updating `src/napi.zig` (N-API) for richer
  ergonomics, and mirror the FFI signatures in `src/lib.zig` for Bun/FFI
  usage.

## Useful files to open when working on a change
- `src/albedo.zig` (storage, pages, insert/list/delete, meta page)
- `src/lib.zig` (C-compatible exports used by Bun FFI)
- `src/napi.zig` (napigen-based Node bindings)
- `src/bplusindex.zig` (index implementation)
- `bun/albedo.ts`, `bun/index.ts` (how FFI/N-API is consumed)
- `build.zig` and `build.md` (build flags and targets)

If anything in this guide is unclear or you want more examples (for example
concrete unit-test patterns, FFI shapes, or a checklist for adding a new
index type), tell me which part you want expanded and I will iterate.
