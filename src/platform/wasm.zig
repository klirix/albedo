const std = @import("std");
const mem = std.mem;
const common = @import("common.zig");

pub const PlatformError = common.PlatformError;
pub const FileOptions = common.FileOptions;
pub const PlatformFileReadError = common.PlatformFileReadError;
pub const PlatformFileWriteError = common.PlatformFileWriteError;
pub const PlatformFileSyncError = common.PlatformFileSyncError;
pub const PlatformOpenError = common.PlatformOpenError;

const WasmHandle = extern struct {
    ptr: [*]const u8,
    len: usize,
};

extern fn wasm_open_file(
    path_ptr: [*]const u8,
    path_len: usize,
    read: u8,
    write: u8,
    create: u8,
    truncate: u8,
    out_handle: *WasmHandle,
) callconv(.c) i32;

extern fn wasm_close_file(handle_ptr: [*]const u8, handle_len: usize) callconv(.c) i32;
extern fn wasm_pread(
    handle_ptr: [*]const u8,
    handle_len: usize,
    offset: u64,
    dest_ptr: [*]u8,
    dest_len: usize,
    out_read: *usize,
) callconv(.c) i32;

extern fn wasm_pwrite(
    handle_ptr: [*]const u8,
    handle_len: usize,
    offset: u64,
    src_ptr: [*]const u8,
    src_len: usize,
    out_written: *usize,
) callconv(.c) i32;

extern fn wasm_sync(handle_ptr: [*]const u8, handle_len: usize) callconv(.c) i32;
extern fn wasm_delete_file(path_ptr: [*]const u8, path_len: usize) callconv(.c) i32;
extern fn wasm_rename_file(old_ptr: [*]const u8, old_len: usize, new_ptr: [*]const u8, new_len: usize) callconv(.c) i32;
extern fn wasm_random_bytes(dest_ptr: [*]u8, dest_len: usize) callconv(.c) i32;
extern fn wasm_now_seconds(out: *i64) callconv(.c) i32;
extern fn wasm_log(msg_ptr: [*]const u8, msg_len: usize) callconv(.c) void;

fn mapStatus(code: i32) PlatformError {
    return switch (code) {
        1 => PlatformError.FileNotFound,
        2 => PlatformError.PermissionDenied,
        3 => PlatformError.AlreadyExists,
        else => PlatformError.Unexpected,
    };
}

fn sliceFromHandle(handle: WasmHandle) []const u8 {
    if (handle.len == 0) return &[_]u8{};
    return handle.ptr[0..handle.len];
}

pub const FileHandle = struct {
    allocator: std.mem.Allocator,
    handle: ?[]u8 = null,

    fn getHandleSlice(self: *const FileHandle) error{ClosedHandle}![]const u8 {
        const owned = self.handle orelse return error.ClosedHandle;
        return owned;
    }

    pub fn close(self: *FileHandle) void {
        if (self.handle) |owned| {
            const ptr_const: [*]const u8 = @ptrCast(owned.ptr);
            _ = wasm_close_file(ptr_const, owned.len);
            self.allocator.free(owned);
            self.handle = null;
        }
    }

    pub fn preadAll(self: *const FileHandle, dest: []u8, offset: u64) PlatformFileReadError!void {
        const owned = try self.getHandleSlice();
        var bytes_read: usize = 0;
        const rc = wasm_pread(owned.ptr, owned.len, offset, dest.ptr, dest.len, &bytes_read);
        if (rc != 0) return mapStatus(rc);
        if (bytes_read != dest.len) return PlatformError.Unexpected;
    }

    pub fn pwriteAll(self: *const FileHandle, src: []const u8, offset: u64) PlatformFileWriteError!void {
        const owned = try self.getHandleSlice();
        var bytes_written: usize = 0;
        const rc = wasm_pwrite(owned.ptr, owned.len, offset, src.ptr, src.len, &bytes_written);
        if (rc != 0) return mapStatus(rc);
        if (bytes_written != src.len) return PlatformError.Unexpected;
    }

    pub fn sync(self: *const FileHandle) PlatformFileSyncError!void {
        const owned = try self.getHandleSlice();
        const rc = wasm_sync(owned.ptr, owned.len);
        if (rc != 0) return mapStatus(rc);
    }
};

pub fn openFile(allocator: std.mem.Allocator, path: []const u8, options: FileOptions) PlatformOpenError!FileHandle {
    var wasm_handle: WasmHandle = .{ .ptr = undefined, .len = 0 };
    const rc = wasm_open_file(
        path.ptr,
        path.len,
        @intFromBool(options.read),
        @intFromBool(options.write),
        @intFromBool(options.create),
        @intFromBool(options.truncate),
        &wasm_handle,
    );
    if (rc != 0) return mapStatus(rc);

    const handle_slice = sliceFromHandle(wasm_handle);
    const owned = allocator.alloc(u8, handle_slice.len) catch |err| {
        _ = wasm_close_file(wasm_handle.ptr, wasm_handle.len);
        return err;
    };
    mem.copyForwards(u8, owned, handle_slice);

    return FileHandle{
        .allocator = allocator,
        .handle = owned,
    };
}

pub fn deleteFile(path: []const u8) PlatformError!void {
    const rc = wasm_delete_file(path.ptr, path.len);
    if (rc != 0) return mapStatus(rc);
}

pub fn renameFile(old_path: []const u8, new_path: []const u8) PlatformError!void {
    const rc = wasm_rename_file(old_path.ptr, old_path.len, new_path.ptr, new_path.len);
    if (rc != 0) return mapStatus(rc);
}

pub fn randomBytes(dest: []u8) PlatformError!void {
    const rc = wasm_random_bytes(dest.ptr, dest.len);
    if (rc != 0) return mapStatus(rc);
}

pub fn nowSeconds() i64 {
    var out: i64 = 0;
    const rc = wasm_now_seconds(&out);
    if (rc != 0) {
        // On failure fall back to zero; callers treat zero as epoch.
        return 0;
    }
    return out;
}

pub fn log(msg: []const u8) void {
    wasm_log(msg.ptr, msg.len);
}
