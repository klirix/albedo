const std = @import("std");

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

pub const OpenOptions = struct {
    read: bool = false,
    write: bool = false,
    create: bool = false,
    truncate: bool = false,
};

pub const FileHandle = struct {
    owned: []u8,

    pub fn close(self: FileHandle) void {
        _ = wasm_close_file(self.owned.ptr, self.owned.len);
        std.heap.wasm_allocator.free(self.owned);
    }

    pub fn readAllAt(self: FileHandle, dest: []u8, offset: u64) (std.Io.File.ReadPositionalError || error{EndOfStream})!void {
        var remaining = dest;
        var current_offset = offset;
        while (remaining.len > 0) {
            var read: usize = 0;
            const rc = wasm_pread(self.owned.ptr, self.owned.len, current_offset, remaining.ptr, remaining.len, &read);
            if (rc != 0) return mapReadStatus(rc);
            if (read == 0) return error.EndOfStream;
            remaining = remaining[read..];
            current_offset += read;
        }
    }

    pub fn writeAllAt(self: FileHandle, src: []const u8, offset: u64) std.Io.File.WritePositionalError!void {
        var remaining = src;
        var current_offset = offset;
        while (remaining.len > 0) {
            var written: usize = 0;
            const rc = wasm_pwrite(self.owned.ptr, self.owned.len, current_offset, remaining.ptr, remaining.len, &written);
            if (rc != 0) return mapWriteStatus(rc);
            if (written == 0) return error.InputOutput;
            remaining = remaining[written..];
            current_offset += written;
        }
    }

    pub fn sync(self: FileHandle) std.Io.File.SyncError!void {
        const rc = wasm_sync(self.owned.ptr, self.owned.len);
        if (rc != 0) return mapSyncStatus(rc);
    }
};

pub fn openFile(path: []const u8, options: OpenOptions) std.Io.File.OpenError!FileHandle {
    var raw_handle: WasmHandle = .{ .ptr = undefined, .len = 0 };
    const rc = wasm_open_file(
        path.ptr,
        path.len,
        @intFromBool(options.read),
        @intFromBool(options.write),
        @intFromBool(options.create),
        @intFromBool(options.truncate),
        &raw_handle,
    );
    if (rc != 0) return mapOpenStatus(rc);

    const handle_bytes = if (raw_handle.len == 0) &[_]u8{} else raw_handle.ptr[0..raw_handle.len];
    return .{
        .owned = std.heap.wasm_allocator.dupe(u8, handle_bytes) catch return error.SystemResources,
    };
}

pub fn deleteFile(path: []const u8) std.Io.Dir.DeleteFileError!void {
    const rc = wasm_delete_file(path.ptr, path.len);
    if (rc != 0) return mapDeleteStatus(rc);
}

pub fn renameFile(old_path: []const u8, new_path: []const u8) std.Io.Dir.RenameError!void {
    const rc = wasm_rename_file(old_path.ptr, old_path.len, new_path.ptr, new_path.len);
    if (rc != 0) return mapRenameStatus(rc);
}

fn crashHandler(_: ?*anyopaque) void {
    @panic("wasm io crash");
}

fn recancel(_: ?*anyopaque) void {}

fn swapCancelProtection(_: ?*anyopaque, new: std.Io.CancelProtection) std.Io.CancelProtection {
    return new;
}

fn checkCancel(_: ?*anyopaque) std.Io.Cancelable!void {}

fn futexWait(_: ?*anyopaque, _: *const u32, _: u32, _: std.Io.Timeout) std.Io.Cancelable!void {
    return error.Canceled;
}

fn futexWaitUncancelable(_: ?*anyopaque, _: *const u32, _: u32) void {
    unreachable;
}

fn futexWake(_: ?*anyopaque, _: *const u32, _: u32) void {}

fn now(_: ?*anyopaque, _: std.Io.Clock) std.Io.Timestamp {
    var seconds: i64 = 0;
    if (wasm_now_seconds(&seconds) != 0) return .zero;
    return .fromNanoseconds(@as(i96, seconds) * std.time.ns_per_s);
}

fn random(_: ?*anyopaque, buffer: []u8) void {
    if (wasm_random_bytes(buffer.ptr, buffer.len) != 0) {
        @panic("wasm_random_bytes failed");
    }
}

fn randomSecure(_: ?*anyopaque, buffer: []u8) std.Io.RandomSecureError!void {
    if (wasm_random_bytes(buffer.ptr, buffer.len) != 0) return error.EntropyUnavailable;
}

fn mapOpenStatus(code: i32) std.Io.File.OpenError {
    return switch (code) {
        1 => error.FileNotFound,
        2 => error.PermissionDenied,
        3 => error.PathAlreadyExists,
        else => error.Unexpected,
    };
}

fn mapDeleteStatus(code: i32) std.Io.Dir.DeleteFileError {
    return switch (code) {
        1 => error.FileNotFound,
        2 => error.PermissionDenied,
        else => error.Unexpected,
    };
}

fn mapRenameStatus(code: i32) std.Io.Dir.RenameError {
    return switch (code) {
        1 => error.FileNotFound,
        2 => error.PermissionDenied,
        else => error.Unexpected,
    };
}

fn mapReadStatus(code: i32) std.Io.File.ReadPositionalError {
    return switch (code) {
        1, 2 => error.AccessDenied,
        else => error.InputOutput,
    };
}

fn mapWriteStatus(code: i32) std.Io.File.WritePositionalError {
    return switch (code) {
        1 => error.AccessDenied,
        2 => error.PermissionDenied,
        else => error.InputOutput,
    };
}

fn mapSyncStatus(code: i32) std.Io.File.SyncError {
    return switch (code) {
        1, 2 => error.AccessDenied,
        else => error.InputOutput,
    };
}

const io_vtable: std.Io.VTable = blk: {
    var table: std.Io.VTable = undefined;
    table.crashHandler = crashHandler;
    table.recancel = recancel;
    table.swapCancelProtection = swapCancelProtection;
    table.checkCancel = checkCancel;
    table.futexWait = futexWait;
    table.futexWaitUncancelable = futexWaitUncancelable;
    table.futexWake = futexWake;
    table.now = now;
    table.random = random;
    table.randomSecure = randomSecure;
    break :blk table;
};

pub fn io() std.Io {
    return .{
        .userdata = null,
        .vtable = &io_vtable,
    };
}
