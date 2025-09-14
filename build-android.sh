zig build -Dtarget=aarch64-linux-android -Dandroid --prefix-lib-dir jniLibs/arm64-v8a
zig build -Dtarget=arm-linux-android -Dandroid --prefix-lib-dir jniLibs/armeabi-v7a
zig build -Dtarget=x86_64-linux-android -Dandroid --prefix-lib-dir jniLibs/x86_64
