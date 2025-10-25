const std = @import("std");
const common = @import("common.zig");

pub const PlatformError = common.PlatformError;
pub const FileOptions = common.FileOptions;
pub const PlatformFileReadError = common.PlatformFileReadError;
pub const PlatformFileWriteError = common.PlatformFileWriteError;
pub const PlatformFileSyncError = common.PlatformFileSyncError;
pub const PlatformOpenError = common.PlatformOpenError;

pub const FileHandle = struct {
    pub fn close(self: *FileHandle) void {
        _ = self;
        @compileError("WASM platform file handles are not implemented yet");
    }

    pub fn preadAll(self: *const FileHandle, dest: []u8, offset: u64) PlatformFileReadError!void {
        _ = self;
        _ = dest;
        _ = offset;
        @compileError("WASM platform preadAll is not implemented");
    }

    pub fn pwriteAll(self: *const FileHandle, src: []const u8, offset: u64) PlatformFileWriteError!void {
        _ = self;
        _ = src;
        _ = offset;
        @compileError("WASM platform pwriteAll is not implemented");
    }

    pub fn sync(self: *const FileHandle) PlatformFileSyncError!void {
        _ = self;
        @compileError("WASM platform sync is not implemented");
    }
};

pub fn openFile(_: std.mem.Allocator, _: []const u8, _: FileOptions) PlatformOpenError!FileHandle {
    @compileError("WASM platform openFile is not implemented");
}

pub fn deleteFile(_: []const u8) PlatformError!void {
    @compileError("WASM platform deleteFile is not implemented");
}

pub fn renameFile(_: []const u8, _: []const u8) PlatformError!void {
    @compileError("WASM platform renameFile is not implemented");
}

pub fn randomBytes(_: []u8) PlatformError!void {
    @compileError("WASM platform randomBytes is not implemented");
}

pub fn nowSeconds() i64 {
    @compileError("WASM platform nowSeconds is not implemented");
}
