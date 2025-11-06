const std = @import("std");
const common = @import("common.zig");

pub const PlatformError = common.PlatformError;
pub const FileOptions = common.FileOptions;
pub const PlatformFileReadError = common.PlatformFileReadError;
pub const PlatformFileWriteError = common.PlatformFileWriteError;
pub const PlatformFileSyncError = common.PlatformFileSyncError;
pub const PlatformOpenError = common.PlatformOpenError;

const StdFileCtx = struct {
    file: std.fs.File,
    allocator: std.mem.Allocator,
};

fn mapFsError(err: anyerror) PlatformError {
    return switch (err) {
        error.FileNotFound => error.FileNotFound,
        error.FileBusy => error.Unexpected,
        error.PathAlreadyExists => error.AlreadyExists,
        error.AccessDenied => error.PermissionDenied,
        error.PermissionDenied => error.PermissionDenied,
        error.IsDir => error.Unexpected,
        error.InputOutput => error.Unexpected,
        error.NoSpaceLeft => error.Unexpected,
        else => error.Unexpected,
    };
}

pub const FileHandle = struct {
    ctx: ?*StdFileCtx,

    pub fn close(self: *FileHandle) void {
        if (self.ctx) |ctx| {
            ctx.file.close();
            ctx.allocator.destroy(ctx);
            self.ctx = null;
        }
    }

    pub fn preadAll(self: *const FileHandle, dest: []u8, offset: u64) PlatformFileReadError!void {
        const ctx = self.ctx orelse return error.ClosedHandle;
        _ = ctx.file.preadAll(dest, offset) catch |err| return mapFsError(err);
    }

    pub fn pwriteAll(self: *const FileHandle, src: []const u8, offset: u64) PlatformFileWriteError!void {
        const ctx = self.ctx orelse return error.ClosedHandle;
        ctx.file.pwriteAll(src, offset) catch |err| return mapFsError(err);
    }

    pub fn sync(self: *const FileHandle) PlatformFileSyncError!void {
        const ctx = self.ctx orelse return error.ClosedHandle;
        ctx.file.sync() catch |err| return mapFsError(err);
    }
};

pub fn openFile(allocator: std.mem.Allocator, path: []const u8, options: FileOptions) PlatformOpenError!FileHandle {
    const cwd = std.fs.cwd();
    const is_abs = std.fs.path.isAbsolute(path);
    const mode: std.fs.File.OpenMode = if (options.write) .read_write else .read_only;

    const file = if (options.create) blk: {
        const create_opts = std.fs.File.CreateFlags{
            .read = options.read,
            .truncate = options.truncate,
        };
        break :blk (if (is_abs)
            std.fs.createFileAbsolute(path, create_opts)
        else
            cwd.createFile(path, create_opts)) catch |err| return mapFsError(err);
    } else blk: {
        break :blk (if (is_abs)
            std.fs.openFileAbsolute(path, .{ .mode = mode })
        else
            cwd.openFile(path, .{ .mode = mode })) catch |err| switch (err) {
            error.FileNotFound => return error.FileNotFound,
            else => return mapFsError(err),
        };
    };

    const ctx = try allocator.create(StdFileCtx);
    ctx.* = .{
        .file = file,
        .allocator = allocator,
    };

    return FileHandle{ .ctx = ctx };
}

pub fn deleteFile(path: []const u8) PlatformError!void {
    const cwd = std.fs.cwd();
    const is_abs = std.fs.path.isAbsolute(path);
    (if (is_abs)
        std.fs.deleteFileAbsolute(path)
    else
        cwd.deleteFile(path)) catch |err| return mapFsError(err);
}

pub fn renameFile(old_path: []const u8, new_path: []const u8) PlatformError!void {
    const cwd = std.fs.cwd();
    const old_abs = std.fs.path.isAbsolute(old_path);
    const new_abs = std.fs.path.isAbsolute(new_path);
    if (old_abs != new_abs) return error.Unexpected;

    (if (old_abs)
        std.fs.renameAbsolute(old_path, new_path)
    else
        cwd.rename(old_path, new_path)) catch |err| return mapFsError(err);
}

pub fn randomBytes(dest: []u8) PlatformError!void {
    std.crypto.random.bytes(dest);
}

pub fn nowSeconds() i64 {
    return std.time.timestamp();
}

pub fn log(msg: []const u8) void {
    std.debug.print("{s}\n", .{msg});
}
