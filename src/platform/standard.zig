const std = @import("std");
const common = @import("common.zig");

pub const PlatformError = common.PlatformError;
pub const FileOptions = common.FileOptions;
pub const PlatformFileReadError = common.PlatformFileReadError;
pub const PlatformFileWriteError = common.PlatformFileWriteError;
pub const PlatformFileSyncError = common.PlatformFileSyncError;
pub const PlatformOpenError = common.PlatformOpenError;

const StdFileCtx = struct {
    file: std.Io.File,
    allocator: std.mem.Allocator,
    io: std.Io,
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
            ctx.file.close(ctx.io);
            ctx.allocator.destroy(ctx);
            self.ctx = null;
        }
    }

    pub fn preadAll(self: *const FileHandle, dest: []u8, offset: u64) PlatformFileReadError!void {
        const ctx = self.ctx orelse return error.ClosedHandle;
        var reader = std.Io.File.Reader.init(ctx.file, ctx.io, &.{});
        reader.seekTo(offset) catch |err| return mapFsError(err);
        reader.interface.readSliceAll(dest) catch |err| return mapFsError(err);
    }

    pub fn pwriteAll(self: *const FileHandle, src: []const u8, offset: u64) PlatformFileWriteError!void {
        const ctx = self.ctx orelse return error.ClosedHandle;
        var writer = std.Io.File.Writer.init(ctx.file, ctx.io, &.{});
        writer.seekTo(offset) catch |err| return mapFsError(err);
        writer.interface.writeAll(src) catch |err| return mapFsError(err);
    }

    pub fn sync(self: *const FileHandle) PlatformFileSyncError!void {
        const ctx = self.ctx orelse return error.ClosedHandle;
        ctx.file.sync(ctx.io) catch |err| return mapFsError(err);
    }
};

pub const Platform = struct {
    io: std.Io,

    pub fn init(io: std.Io) Platform {
        return .{ .io = io };
    }

    pub fn openFile(self: Platform, allocator: std.mem.Allocator, path: []const u8, options: FileOptions) PlatformOpenError!FileHandle {
        const cwd = std.Io.Dir.cwd();
        const is_abs = std.fs.path.isAbsolute(path);
        const mode: std.Io.File.OpenMode = if (options.write) .read_write else .read_only;

        const file = if (options.create) blk: {
            const create_opts = std.Io.File.CreateFlags{
                .read = options.read,
                .truncate = options.truncate,
            };
            break :blk (if (is_abs)
                std.Io.Dir.createFileAbsolute(self.io, path, create_opts)
            else
                cwd.createFile(self.io, path, create_opts)) catch |err| return mapFsError(err);
        } else blk: {
            break :blk (if (is_abs)
                std.Io.Dir.openFileAbsolute(self.io, path, .{ .mode = mode })
            else
                cwd.openFile(self.io, path, .{ .mode = mode })) catch |err| switch (err) {
                error.FileNotFound => return error.FileNotFound,
                else => return mapFsError(err),
            };
        };

        const ctx = try allocator.create(StdFileCtx);
        ctx.* = .{
            .file = file,
            .allocator = allocator,
            .io = self.io,
        };

        return FileHandle{ .ctx = ctx };
    }

    pub fn deleteFile(self: Platform, path: []const u8) PlatformError!void {
        const cwd = std.Io.Dir.cwd();
        const is_abs = std.fs.path.isAbsolute(path);
        (if (is_abs)
            std.Io.Dir.deleteFileAbsolute(self.io, path)
        else
            cwd.deleteFile(self.io, path)) catch |err| return mapFsError(err);
    }

    pub fn renameFile(self: Platform, old_path: []const u8, new_path: []const u8) PlatformError!void {
        const cwd = std.Io.Dir.cwd();
        const old_abs = std.fs.path.isAbsolute(old_path);
        const new_abs = std.fs.path.isAbsolute(new_path);
        if (old_abs != new_abs) return error.Unexpected;

        (if (old_abs)
            std.Io.Dir.renameAbsolute(old_path, new_path, self.io)
        else
            std.Io.Dir.rename(cwd, old_path, cwd, new_path, self.io)) catch |err| return mapFsError(err);
    }

    pub fn randomBytes(self: Platform, dest: []u8) PlatformError!void {
        self.io.random(dest);
    }

    pub fn nowSeconds(self: Platform) i64 {
        return std.Io.Timestamp.now(self.io, .real).toSeconds();
    }

    pub fn log(self: Platform, msg: []const u8) void {
        _ = self;
        std.debug.print("{s}\n", .{msg});
    }
};
