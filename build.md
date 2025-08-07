# Build instructions

To build the Albedo library and copy the resulting artifacts into the Bun project, run:

```sh
zig build && cp ./zig-out/lib/libalbedo.so ./bun/libalbedo.so
zig build && cp ./zig-out/lib/libalbedo.dylib ./bun/libalbedo.dylib
zig build && cp ./zig-out/lib/libalbedo.node ./bun/libalbedo.node
```

