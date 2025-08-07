# Albedo

Albedo is an experimental document store written in [Zig](https://ziglang.org).
It targets embedded use cases and exposes a C FFI for static and dynamic
libraries used by [Bun](https://bun.sh) and Node bindings via N-API. The database
stores documents in BSON format and uses a page-based layout with B+-tree
indexing.

## Project status

This project is in early development. Interfaces and file formats may change
without notice.

## Building

The Zig build system produces shared libraries for different platforms. Refer
to [build.md](build.md) for the exact commands.

```sh
zig build
```

## Bun example

A small Bun project demonstrating how to use the library lives in the
[`bun`](bun) directory.

```sh
cd bun
bun install
bun run index.ts
```

## License

Released under the MIT License. See [license](license) for details.

