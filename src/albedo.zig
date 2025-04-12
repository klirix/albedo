const std = @import("std");
const bson = @import("bson.zig");
const File = std.fs.File;

const ALBEDO_MAGIC = "ALBEDO";
const ALBEDO_VERSION: u8 = 1;
const ALBEDO_FLAGS = 0;
const DEFAULT_PAGE_SIZE = 8192; // 8kB

const BucketHeader = struct {
    magic: [6]u8,
    version: u8,
    flags: u8,
    page_size: u32 = DEFAULT_PAGE_SIZE,
    page_count: u64,
    reserved: [44]u8, // Padding to make the struct 64 bytes

    pub fn init() BucketHeader {
        var header = BucketHeader{
            .magic = undefined,
            .version = ALBEDO_VERSION,
            .flags = ALBEDO_FLAGS,
            .page_size = DEFAULT_PAGE_SIZE,
            .page_count = 0,
            .reserved = [_]u8{0} ** 44,
        };

        std.mem.copyForwards(u8, &header.magic, ALBEDO_MAGIC);
        // std.mem.copyForwards(u8, &header.reserved, [_]u8{0} ** 44);

        return header;
    }

    pub fn read(memory: []const u8) BucketHeader {
        var header = BucketHeader{
            .magic = undefined,
            .version = memory[6],
            .flags = memory[7],
            .page_size = std.mem.readInt(u32, memory[8..12], .little),
            .page_count = std.mem.readInt(u64, memory[12..20], .little),
            .reserved = undefined,
        };

        std.mem.copyForwards(u8, header.magic[0..], ALBEDO_MAGIC);
        std.mem.copyForwards(u8, header.reserved[0..], &[_]u8{0} ** 44);

        return header;
    }

    pub fn write(self: *const BucketHeader) [64]u8 {
        var buffer: [64]u8 = [_]u8{0} ** 64;
        std.mem.copyForwards(u8, buffer[0..6], ALBEDO_MAGIC);
        buffer[6] = self.version;
        buffer[7] = self.flags;
        std.mem.writeInt(u32, buffer[8..12], self.page_size, .little);
        std.mem.writeInt(u64, buffer[12..20], self.page_count, .little);
        std.mem.copyForwards(u8, buffer[20..64], &self.reserved);
        return buffer;
    }
};

const PageType = enum(u8) { Meta = 0, Data = 1, Index = 2, Free = 3 };

const PageHeader = struct {
    page_type: PageType, // 1
    used_size: u32 = 0, // 4
    page_id: u64, // 8
    first_readable_byte: u32, // 4
    reserved: [15]u8, // 15

    pub fn init(page_type: PageType, id: u64) PageHeader {
        return PageHeader{
            .page_type = page_type,
            .used_size = 0,
            .page_id = id,
            .first_readable_byte = 0,
            .reserved = [_]u8{0} ** 15,
        };
    }

    pub fn write(self: *const PageHeader) [32]u8 {
        var buffer: [32]u8 = [_]u8{0} ** 32;
        buffer[0] = @intFromEnum(self.page_type);
        std.mem.writeInt(u32, buffer[1..5], self.used_size, .little);
        std.mem.writeInt(u64, buffer[5..13], self.page_id, .little);
        std.mem.writeInt(u32, buffer[13..17], self.first_readable_byte, .little);
        std.mem.copyForwards(u8, buffer[17..32], &self.reserved);
        return buffer;
    }

    pub fn read(memory: []const u8) PageHeader {
        const header = PageHeader{
            .page_type = @enumFromInt(std.mem.readInt(u8, memory[0..1], .little)),
            .used_size = std.mem.readInt(u32, memory[1..5], .little),
            .page_id = std.mem.readInt(u64, memory[5..13], .little),
            .first_readable_byte = std.mem.readInt(u32, memory[13..17], .little),
            .reserved = [_]u8{0} ** 15,
        };
        return header;
    }
};

const PageError = error{
    InvalidPageType,
    InvalidPageSize,
    InvalidPageId,
    PageNotFound,
};

const Page = struct {
    header: PageHeader,
    data: []u8,
    bucket: *Bucket,

    pub fn init(allocator: std.mem.Allocator, bucket: *Bucket, page_type: PageType, page_id: u64) !Page {
        const newPage = Page{
            .header = PageHeader.init(page_type, page_id),
            .data = try allocator.alloc(u8, DEFAULT_PAGE_SIZE - @sizeOf(PageHeader)),
            .bucket = bucket,
        };

        @memset(newPage.data, 0);
        return newPage;
    }

    pub fn deinit(self: *Page, allocator: std.mem.Allocator) void {
        allocator.free(self.data);
    }
};

const Bucket = struct {
    file: std.fs.File,
    allocator: std.mem.Allocator,
    header: BucketHeader,

    // Update init to include allocator
    pub fn init(allocator: std.mem.Allocator, path: []const u8) !Bucket {
        return try Bucket.openFile(allocator, path);
    }

    pub fn openFile(ally: std.mem.Allocator, path: []const u8) !Bucket {
        const stat: ?File.Stat = std.fs.cwd().statFile(path) catch |err| switch (err) {
            error.FileNotFound => null, // If the file doesn't exist, return null
            // error.InvalidPath => return error.InvalidPath, // Return an error if the path is invalid
            else => return err, // Return any other errors
        };
        var file: File = undefined; // Change to a pointer for better management
        if (stat) |s| {
            // If the file exists, check if it's a directory
            // std.debug.print("\nFile exists, opening...\n ", .{});
            if (s.kind == .directory) {
                return error.InvalidPath; // Return an error if it's a directory
            }

            // Open the file for reading and writing
            file = try std.fs.cwd().openFile(path, .{ .mode = .read_write });
            const header_bytes = try file.reader().readBytesNoEof(@sizeOf(BucketHeader));
            return .{
                .file = file,
                .header = BucketHeader.read(header_bytes[0..@sizeOf(BucketHeader)]),
                .allocator = ally,
            };
        } else {
            // std.debug.print("\nFile doesnt exists, creating...\n ", .{});
            // If the file doesn't exist, create it
            _ = try std.fs.cwd().createFile(path, .{});

            // Reopen the file for reading and writing
            file = try std.fs.cwd().openFile(path, .{ .mode = .read_write });

            const bytes_written = try file.write(BucketHeader.init().write()[0..64]);

            std.debug.print("{d}", .{bytes_written});
            return .{
                .file = file,
                .header = BucketHeader.init(),
                .allocator = ally,
            };
        }
    }

    // Previous openFile implementation remains the same...

    pub fn readPage(self: *Bucket, allocator: std.mem.Allocator, page_id: u64) !Page {
        var page = try Page.init(allocator, self, .Data, page_id);

        // Seek to the correct position: header size + (page_id * page size)
        const offset = @sizeOf(BucketHeader) + (page_id * DEFAULT_PAGE_SIZE);
        try self.file.seekTo(offset);
        std.debug.print("Read page, offset {d}\n", .{offset});

        // Read the page header
        const header_bytes = try self.file.reader().readBytesNoEof(@sizeOf(PageHeader));
        page.header = PageHeader.read(&header_bytes);
        std.debug.print("PageNotFound: Read  at offset: {d}\n", .{try self.file.getPos()});

        // Validate page
        if (page.header.used_size != DEFAULT_PAGE_SIZE) {
            return PageError.InvalidPageSize;
        }
        if (page.header.page_id != page_id) {
            return PageError.InvalidPageId;
        }

        // Read the page data
        const bytes_read = try self.file.reader().readAll(page.data);
        if (bytes_read != page.data.len) {
            std.debug.print("PageNotFound: Read {d} bytes instead of {d}, at offset: {d}\n", .{ bytes_read, page.data.len, try self.file.getPos() });
            return PageError.PageNotFound;
        }

        return page;
    }

    pub fn writePage(self: *Bucket, page: *const Page) !void {
        const header_size = @sizeOf(PageHeader);
        const offset = 64 + (page.header.page_id * DEFAULT_PAGE_SIZE);

        // Seek to the correct position
        try self.file.seekTo(offset);

        // Write the header
        const header_bytes: [header_size]u8 = PageHeader.write(&page.header);
        // Ensure the header is written correctly
        _ = try self.file.write(&header_bytes);

        // Write the data
        _ = try self.file.write(page.data);
        try self.file.sync(); // Ensure the write is flushed to disk
    }

    pub fn createNewPage(self: *Bucket, page_type: PageType) !Page {
        // For now, we'll just use the page_id as a sequential number
        // In a real implementation, you'd want to track available page IDs
        const new_page_id = self.header.page_count;
        self.header.page_count += 1;
        try self.file.seekTo(0);
        _ = try self.file.write(self.header.write()[0..@sizeOf(BucketHeader)]); // Update the header
        try self.file.sync(); // Ensure the header is flushed to disk

        return try Page.init(self.allocator, self, page_type, new_page_id);
    }

    pub fn insertDoc(self: *Bucket, doc: bson.BSONDocument) !void {
        const buf =
            // For now, we'll just print the document
            std.debug.print("Inserting document: {s}\n", .{});
    }

    pub fn deinit(self: *Bucket) void {
        self.file.close();
    }
};

// Example test
test "page operations" {
    const allocator = std.testing.allocator;
    var bucket = try Bucket.init(allocator, "test.bucket");
    defer bucket.deinit();

    // Create a new page
    var page = try bucket.createNewPage(.Data);
    defer page.deinit(allocator);

    // Write some test data
    @memcpy(page.data[0..5], "Hello");

    // Write the page
    try bucket.writePage(&page);

    // Read the page back
    var read_page = try bucket.readPage(allocator, page.header.page_id);
    defer read_page.deinit(allocator);

    std.debug.print("Current file offset {d}", .{try bucket.file.getPos()});

    // Verify the data
    try std.testing.expectEqualSlices(u8, page.data[0..5], read_page.data[0..5]);
}

// there must me 3 types of operations on a data in the bucket
// 1. insert
// 2. list
// 3. delete

// test "a" {
//     std.debug.print("running in: {s}", .{try std.fs.cwd().realpathAlloc(std.testing.allocator, ".")});
//     _ = try Bucket.init("./test2.bucket");
//     // std.mem.sort(comptime T: type, items: []T, context: anytype, comptime lessThanFn: fn(@TypeOf(context), lhs:T, rhs:T)bool)
// }
// test "b" {
//     // const arr = [_]u8{ 1, 2, 3, 4, 5 };
//     // std.mem.sort(u8, arr, null, (context: anytype, lhs: u8, rhs: u8) bool {
//     //     return lhs < rhs;
//     // });
// }
