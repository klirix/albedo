const builtin = @import("builtin");
const wasm_impl = @import("platform/wasm.zig");
const std_impl = @import("platform/standard.zig");

pub const isWasm = builtin.target.cpu.arch == .wasm32 or builtin.target.cpu.arch == .wasm64;

const Impl = if (isWasm)
    wasm_impl
else
    std_impl;

pub const PlatformError = Impl.PlatformError;
pub const FileOptions = Impl.FileOptions;
pub const PlatformFileReadError = Impl.PlatformFileReadError;
pub const PlatformFileWriteError = Impl.PlatformFileWriteError;
pub const PlatformFileSyncError = Impl.PlatformFileSyncError;
pub const PlatformOpenError = Impl.PlatformOpenError;
pub const FileHandle = Impl.FileHandle;

pub const openFile = Impl.openFile;
pub const deleteFile = Impl.deleteFile;
pub const renameFile = Impl.renameFile;
pub const randomBytes = Impl.randomBytes;
pub const nowSeconds = Impl.nowSeconds;
