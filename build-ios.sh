zig build -Dstatic -Dtarget=aarch64-ios-simulator --prefix-lib-dir lib/arm-sim 
zig build -Dstatic -Dtarget=aarch64-ios --prefix-lib-dir lib/arm
zig build -Dstatic -Dtarget=x86_64-ios-simulator --prefix-lib-dir lib/x86
lipo zig-out/lib/arm-sim/libalbedo.a zig-out/lib/x86/libalbedo.a -output zig-out/lib/arm-sim/libalbedo.a -create
xcodebuild -create-xcframework -library zig-out/lib/arm/libalbedo.a -library zig-out/lib/arm-sim/libalbedo.a -output albedo.xcframework
rm -rf zig-out/lib/x86
rm -rf zig-out/lib/arm
rm -rf zig-out/lib/arm-sim
