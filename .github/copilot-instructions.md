# Albedo — Quick orientation for AI coding agents

Albedo is a small experimental document store written in Zig with two primary consumption paths: a C-style FFI (shared/static library) and Node/Bun bindings. The codebase is intentionally compact and favors explicit, manual memory and file layout operations. This file gives the most important, discoverable facts to get productive quickly.

## Big-picture architecture (why & what)

- Storage: fixed page size (DEFAULT_PAGE_SIZE = 8192) with a 64-byte bucket header at file start and 32-byte page headers. Page types: Data, Index, Free, Meta. The meta page (page 0) is authoritative for index registration.
  - See: `src/albedo.zig` (core bucket, pages, document layout) and `src/bplusindex.zig` (index implementation).
- API surfaces:
  - C-style exports for direct FFI: `src/lib.zig` (used by Bun FFI in `bun/albedo.ts`).
  - N-API/napigen Node binding: `src/napi.zig` (build with `-Dnode=true`).
- Memory model: explicit allocators (often `ArenaAllocator` or caller-provided allocator). Many APIs return slices backed by buffers — pay attention to ownership and use `defer allocator.free(...)` or `deinit()` where present.
- Concurrency: `Bucket` operations use a `RwLock` for read/write safety. Follow existing lock patterns (lockShared/lock/ unlock) in `src/albedo.zig`.

## Critical developer workflows

- Build native shared library:
  zig build
- Build Node (N-API) extension:
  zig build -Dnode=true
- Build static library:
  zig build -Dstatic=true
- Run Zig unit tests (tests are embedded in sources):
  zig test src/albedo.zig
  or run full suite via build system:
  zig build test
- Bun example (after building shared lib):
  cd bun
  bun install
  bun run index.ts

Artifacts appear in `zig-out/` (`zig-out/lib/libalbedo.dylib`, and `zig-out/bin/*` for tests). The Bun example expects `libalbedo.${suffix}` in `bun/` (see `bun/albedo.ts`).

## Project-specific conventions & gotchas

- File layout offsets matter: the first 64 bytes = `BucketHeader`, pages start immediately after. Be careful with offsets and page boundary arithmetic (see `Bucket.loadPage` / `writePage` in `src/albedo.zig`).
- Meta page (page 0) serializes index descriptors and is authoritative. When adding/updating indexes call `recordIndexes()` (search for it in `src/albedo.zig`) to persist metadata.
- Explicit memory management: prefer forwarding allocators; many functions accept an allocator or create per-request arenas. When adding APIs that return pointers to internal buffers, follow the existing pattern — use Arena for short-lived buffers and return owned buffers only when callers are expected to free them.
- Tests: Zig tests are embedded alongside implementations (e.g., `src/bson.zig` contains many tests). Run `zig test` on the file you edit to get fast feedback.
- Avoid implicit copying of slices that point into temporary arenas. When in doubt, search for `.deinit(` or `.free(` usage nearby.

## Integration points & cross-component communication

- Bun FFI (dlopen) usage: `bun/albedo.ts` calls C exports from `libalbedo.dylib` (see `bun/index.ts` and `bun/albedo.ts`). The N-API binding lives in `src/napi.zig` and is produced by building with `-Dnode=true`.
- WASM: `zig-out/bin/albedo.wasm` and `bun/albedo-wasm/` contain examples for the WASM target.

## Key files to inspect (fast lookup)

- `src/albedo.zig` — core bucket, pages, document layout, insert/list/delete, meta page handling.
- `src/lib.zig` — C-compatible exported API used by Bun FFI.
- `src/napi.zig` — Node/N-API exports (napigen) for richer JS ergonomics.
- `src/bplusindex.zig` — index implementation and B+ tree details.
- `src/bson.zig` — BSON parsing/serialization examples and many embedded tests (useful patterns for binary layout and tests).
- `bun/albedo.ts`, `bun/index.ts` — how consumers load the library (dlopen) and use the FFI.
- `build.zig` / `build.md` — useful flags and build targets.

## How to add small features safely

- Maintain low-level invariants: preserve page alignment, header sizes, and meta page serialization format. Add tests that construct raw page bytes if you change layout.
- Use existing allocator patterns: prefer accepting an allocator parameter; use `ArenaAllocator` for internal short-lived allocations. Mirror `deinit()` semantics where appropriate.
- When adding or changing indexes: update in-memory structures and also persist changes via `recordIndexes()` so the meta page reflects the change.

## Quick troubleshooting tips

- If a test fails with memory/offset errors, add debugging prints near `writePage`/`loadPage` and verify sizes against constants at the top of `src/albedo.zig`.
- If Node binding fails to load, ensure you built with `-Dnode=true` and that `libalbedo.node` or `libalbedo.dylib` is placed/copied into `bun/`.

## Examples to cite in PRs

- To show allocator patterns: point reviewers at `src/bson.zig` tests and `BSONDocument.fromTuple`.
- To justify low-level changes: include a small test that builds the specific page bytes and uses `zig test` on the relevant file.

If anything above is unclear or you want extra examples (e.g., a small unit-test template to add, or a checklist for adding a new index type), tell me which part to expand and I will update this file.
