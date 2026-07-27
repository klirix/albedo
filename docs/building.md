# Building Albedo

[Documentation index](README.md) ·
[Getting started](getting-started.md)

Albedo requires the Zig version pinned in [`.zigversion`](../.zigversion).

## Standard builds

```sh
# Shared library
zig build

# Static library
zig build -Dstatic=true

# Test suite
zig build test
```

Build artifacts are written to `zig-out/`.

## Platform targets

The core storage engine is designed to cross-compile for Linux, macOS,
Windows, iOS, Android, and WASM. Target-specific flags are defined in
[`build.zig`](../build.zig); run `zig build --help` to see the options exposed
by the current checkout.

## Copying artifacts into a local binding

When developing a binding in a sibling project, build first and copy the
artifact appropriate for the host:

```sh
zig build
cp ./zig-out/lib/libalbedo.so ./path/to/binding/
cp ./zig-out/lib/libalbedo.dylib ./path/to/binding/
cp ./zig-out/lib/libalbedo.node ./path/to/binding/
```

Only copy the artifact produced for the target platform.
