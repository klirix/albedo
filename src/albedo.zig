const std = @import("std");
const bson = @import("bson.zig");
const File = std.fs.File;
const ObjectId = @import("object_id.zig").ObjectId;

const ALBEDO_MAGIC = "ALBEDO";
const ALBEDO_VERSION: u8 = 1;
const ALBEDO_FLAGS = 0;
const DEFAULT_PAGE_SIZE = 8192; // 8kB, or up to 64kB

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
    used_size: u16 = 0, // 2
    page_id: u64, // 8
    first_readable_byte: u16, // 2
    reserved: [19]u8, // 19

    pub fn init(page_type: PageType, id: u64) PageHeader {
        return PageHeader{
            .page_type = page_type,
            .used_size = 0,
            .page_id = id,
            .first_readable_byte = 0,
            .reserved = [_]u8{0} ** 19,
        };
    }

    pub fn write(self: *const PageHeader) [32]u8 {
        var buffer: [32]u8 = [_]u8{0} ** 32;
        buffer[0] = @intFromEnum(self.page_type);
        std.mem.writeInt(u16, buffer[1..3], self.used_size, .little);
        std.mem.writeInt(u64, buffer[3..11], self.page_id, .little);
        std.mem.writeInt(u16, buffer[11..13], self.first_readable_byte, .little);
        std.mem.copyForwards(u8, buffer[13..32], &self.reserved);
        return buffer;
    }

    pub fn read(memory: []const u8) PageHeader {
        const header = PageHeader{
            .page_type = @enumFromInt(std.mem.readInt(u8, memory[0..1], .little)),
            .used_size = std.mem.readInt(u16, memory[1..3], .little),
            .page_id = std.mem.readInt(u64, memory[3..11], .little),
            .first_readable_byte = std.mem.readInt(u16, memory[11..13], .little),
            .reserved = [_]u8{0} ** 19,
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
    stream: std.io.FixedBufferStream([]u8),

    pub fn init(allocator: std.mem.Allocator, bucket: *Bucket, header: PageHeader) !Page {
        const dataBuffer = try allocator.alloc(u8, DEFAULT_PAGE_SIZE - @sizeOf(PageHeader));
        var stream = std.io.fixedBufferStream(dataBuffer);
        try stream.seekTo(header.used_size);
        const newPage = Page{
            .header = header,
            .data = dataBuffer,
            .bucket = bucket,
            .stream = stream,
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

    pub fn loadPage(self: *Bucket, allocator: std.mem.Allocator, page_id: u64) !Page {
        const reader = self.file.reader();

        const offset = @sizeOf(BucketHeader) + (page_id * DEFAULT_PAGE_SIZE);
        try self.file.seekTo(offset);
        // Read the page header
        const header_bytes = try reader.readBytesNoEof(@sizeOf(PageHeader));
        const header = PageHeader.read(&header_bytes);

        const page = try Page.init(allocator, self, header);

        // Seek to the correct position: header size + (page_id * page size)
        std.debug.print("Read page, offset {d}\n", .{offset});

        // Validate page
        // if (page.header.used_size != DEFAULT_PAGE_SIZE) {
        //     return PageError.InvalidPageSize;
        // }
        if (page.header.page_id != page_id) {
            return PageError.InvalidPageId;
        }

        // Read the page data
        const bytes_read = try reader.readAll(page.data);
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

        return try Page.init(self.allocator, self, PageHeader.init(page_type, new_page_id));
    }

    // pub fn insertDoc(self: *Bucket, doc: bson.BSONDocument) !void {
    //     // const buf =
    //     // For now, we'll just print the document
    //     std.debug.print("Inserting document: {s}\n", .{});
    // }

    pub const DocHeader = packed struct { // 16bytes
        is_deleted: u8,
        doc_id: u96,
        reserved: u24,

        pub fn init() DocHeader {
            return DocHeader{
                .is_deleted = 0,
                .doc_id = ObjectId.init().toInt(),
                .reserved = 0,
            };
        }

        pub fn write(self: DocHeader, writer: anytype) !void {
            // Write the header to the writer
            try writer.writeStruct(self);
        }

        pub fn read(reader: anytype) !DocHeader {
            // Read the header from the reader
            return try reader.readStruct(DocHeader);
        }
    };
    pub fn insert(self: *Bucket, doc: *const bson.BSONDocument) !void {
        const doc_size = doc.len;
        var encoded_doc = try self.allocator.alloc(u8, doc_size);
        defer self.allocator.free(encoded_doc);
        doc.serializeToMemory(encoded_doc);
        var page: Page = undefined;

        // Get the size of the document to be inserted

        // If no pages exist yet, create one
        if (self.header.page_count == 0) {
            page = try self.createNewPage(.Data);
        } else {
            // Start from the last page
            const page_id: u64 = self.header.page_count - 1;
            page = try self.loadPage(self.allocator, page_id);
        }

        // Check if the page has enough space for header and doc size
        if (page.data.len - page.header.used_size < doc_size + @sizeOf(DocHeader)) {
            // Not enough space, create a new page
            page = try self.createNewPage(.Data);
        }

        // Write the document header

        const doc_header = DocHeader.init();
        _ = try doc_header.write(page.stream.writer());

        var bytes_written: usize = 0;
        const bytes_to_write: usize = doc_size;

        while (bytes_written < bytes_to_write) {
            // Check if current page has enough space
            const available_space = page.data.len - page.header.used_size;

            // Determine how much to write
            const to_write = @min(bytes_to_write - bytes_written, available_space);

            _ = try page.stream.write(encoded_doc[bytes_written .. bytes_written + to_write]);
            // Insert document at the current position
            page.header.used_size += @intCast(to_write);
            bytes_written += to_write;

            // Write updated page back to disk
            try self.writePage(&page);

            // If there are still bytes left to write, create a new page
            if (bytes_written < bytes_to_write) {
                page = try self.createNewPage(.Data);
            }
        }

        // After writing is done, free the page resources
        page.deinit(self.allocator);
    }

    pub fn list(self: *Bucket) ![]bson.BSONDocument {
        if (self.header.page_count == 0) {
            return &[_]bson.BSONDocument{};
        }

        var documents = std.ArrayList(bson.BSONDocument).init(self.allocator);
        defer documents.deinit(); // This will be transferred to the caller

        // Buffer to hold document data that may span multiple pages
        var doc_buffer = std.ArrayList(u8).init(self.allocator);
        defer doc_buffer.deinit();

        // Read each page
        var current_page_id: u64 = 0;
        while (current_page_id < self.header.page_count) : (current_page_id += 1) {
            var page = try self.loadPage(self.allocator, current_page_id);
            defer page.deinit(self.allocator);
            var reader = page.stream.reader();
            reader.skipBytes(page.header.first_readable_byte, comptime options: SkipBytesOptions)

            if (page.header.used_size == 0) {
                continue;
            }

            // Process the used portion of the page data
            var offset: u32 = 0;
            while (offset < page.header.used_size) {
                // If we have no data in the buffer, read the document length
                if (doc_buffer.items.len == 0) {
                    if (offset + 4 > page.header.used_size) {
                        // Document length spans two pages
                        try doc_buffer.appendSlice(page.data[offset..page.header.used_size]);
                        continue;
                    }
                    const doc_length = std.mem.readInt(u32, page.data[offset..][0..4], .little);

                    // Reset buffer and start collecting document data
                    try doc_buffer.resize(0);
                    try doc_buffer.ensureTotalCapacity(doc_length);

                    // Add initial chunk of document
                    const bytes_available = page.header.used_size - offset;
                    const bytes_to_copy = @min(doc_length, bytes_available);
                    try doc_buffer.appendSlice(page.data[offset .. offset + bytes_to_copy]);

                    if (bytes_to_copy == doc_length) {
                        // Complete document in current page
                        const doc = try bson.BSONDocument.deserializeFromMemory(self.allocator, doc_buffer.items);
                        try documents.append(doc);
                        try doc_buffer.resize(0);
                    }

                    offset += bytes_to_copy;
                } else {
                    // Continue collecting document from previous page
                    const remaining_bytes = doc_buffer.capacity - doc_buffer.items.len;
                    const bytes_available = page.header.used_size - offset;
                    const bytes_to_copy = @min(remaining_bytes, bytes_available);

                    try doc_buffer.appendSlice(page.data[offset .. offset + bytes_to_copy]);

                    if (doc_buffer.items.len == doc_buffer.capacity) {
                        // Document is complete
                        const doc = try bson.BSONDocument.deserializeFromMemory(self.allocator, doc_buffer.items);
                        try documents.append(doc);
                        try doc_buffer.resize(0);
                    }

                    offset += bytes_to_copy;
                }
            }
        }

        // Check if we have an incomplete document at the end
        if (doc_buffer.items.len > 0) {
            // This would indicate corrupted data
            return error.IncompleteDocument;
        }

        // Transfer ownership of the array to the caller
        return documents.toOwnedSlice();
    }

    pub fn delete(_: *Bucket, _: bson.BSONDocument) !void {
        // For now, we'll just print the document
        std.debug.print("Deleting document: {s}\n", .{});
    }

    pub fn deinit(self: *Bucket) void {
        self.file.close();
    }
};

test "Bucket.insert" {
    const allocator = std.testing.allocator;
    var bucket = try Bucket.init(allocator, "test.bucket");
    defer bucket.deinit();

    var docValues = [_]bson.BSONKeyValuePair{
        bson.BSONKeyValuePair{ .key = "name", .value = .{ .string = .{ .value = "Alice\x00" } } },
        bson.BSONKeyValuePair{ .key = "age", .value = .{ .int32 = .{ .value = 30 } } },
    };

    // Create a new BSON document
    var doc = bson.BSONDocument.fromPairs(allocator, docValues[0..]);

    // Insert the document into the bucket
    try bucket.insert(&doc);

    // List documents in the bucket
    for (try bucket.list()) |value| {
        std.debug.print("Document: {any}\n", .{value});
    }
}

test "DocHeader.write" {
    var fixedBuffer: [64]u8 = [_]u8{0} ** 64;
    var stream = std.io.fixedBufferStream(&fixedBuffer);

    var doc_header = Bucket.DocHeader{
        .is_deleted = 0,
        .doc_id = 0,
        .reserved = 0,
    };
    try doc_header.write(stream.writer());
    try stream.seekTo(0);
    doc_header = try Bucket.DocHeader.read(stream.reader());
}

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
    std.debug.print("Current bucket header {any}\n", .{bucket.header});
    std.debug.print("Current page header {any}\n", .{page.header});
    // Read the page back
    var read_page = try bucket.loadPage(allocator, page.header.page_id);
    defer read_page.deinit(allocator);

    std.debug.print("Current file offset {x}", .{try bucket.file.getPos()});

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
