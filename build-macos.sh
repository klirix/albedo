zig build -Dtarget=x86_64-macos --prefix-lib-dir lib/x86 
zig build -Dtarget=aarch64-macos --prefix-lib-dir lib/arm
lipo -create -output zig-out/lib/libalbedo.dylib zig-out/lib/arm/libalbedo.dylib zig-out/lib/x86/libalbedo.dylib
rm -rf zig-out/lib/x86
rm -rf zig-out/lib/arm
