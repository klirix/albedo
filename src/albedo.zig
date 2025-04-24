const std = @import("std");
const bson = @import("bson.zig");
const BSONValue = bson.BSONValue;
const BSONDocument = bson.BSONDocument;
const File = std.fs.File;
const mem = std.mem;
const ObjectId = @import("object_id.zig").ObjectId;
const query = @import("query.zig");

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

    pub fn init(allocator: std.mem.Allocator, bucket: *Bucket, header: PageHeader) !Page {
        const dataBuffer = try allocator.alloc(u8, DEFAULT_PAGE_SIZE - @sizeOf(PageHeader));
        var stream = std.io.fixedBufferStream(dataBuffer);
        try stream.seekTo(header.used_size);
        const newPage = Page{
            .header = header,
            .data = dataBuffer,
            .bucket = bucket,
        };

        @memset(newPage.data, 0);
        return newPage;
    }

    pub fn deinit(self: *Page, allocator: std.mem.Allocator) void {
        allocator.free(self.data);
    }
};

const BucketInitErrors = error{
    FileStatError,
    InvalidPath,
    InvalidDocId,
    FileOpenError,
    FileReadError,
};

pub const Bucket = struct {
    file: std.fs.File,
    allocator: std.mem.Allocator,
    header: BucketHeader,
    page_cache: std.AutoArrayHashMap(u64, Page),
    rwlock: std.Thread.RwLock = .{},

    const PageIterator = struct {
        bucket: *Bucket,
        index: u64 = 0,
        type: PageType,

        pub fn next(self: *PageIterator) !?Page {
            var found = false;
            var page: Page = undefined;
            while (!found) {
                if (self.index >= self.bucket.header.page_count) {
                    return null;
                }
                page = try self.bucket.loadPage(self.index);
                found = page.header.page_type == self.type;
                self.index += 1;
            }
            return page;
        }
    };

    // Update init to include allocator
    pub fn init(allocator: std.mem.Allocator, path: []const u8) BucketInitErrors!Bucket {
        var bucket = try Bucket.openFile(allocator, path);
        bucket.page_cache = std.AutoArrayHashMap(u64, Page).init(allocator);
        return bucket;
    }

    pub fn openFile(ally: std.mem.Allocator, path: []const u8) BucketInitErrors!Bucket {
        const stat: ?File.Stat = std.fs.cwd().statFile(path) catch |err| switch (err) {
            error.FileNotFound => null, // If the file doesn't exist, return null
            // error.InvalidPath => return error.InvalidPath, // Return an error if the path is invalid
            else => {
                std.debug.print("Error opening file: {any}\n", .{err});
                return BucketInitErrors.FileStatError; // Return null for any other errors
            }, // Return any other errors
        };
        var file: File = undefined; // Change to a pointer for better management
        if (stat) |s| {
            // If the file exists, check if it's a directory
            // std.debug.print("\nFile exists, opening...\n ", .{});
            if (s.kind == .directory) {
                return BucketInitErrors.InvalidPath; // Return an error if it's a directory
            }

            // Open the file for reading and writing
            file = std.fs.cwd().openFile(
                path,
                .{ .mode = .read_write },
            ) catch return BucketInitErrors.FileOpenError;
            const header_bytes = file.reader().readBytesNoEof(@sizeOf(BucketHeader)) catch return BucketInitErrors.FileReadError;
            return .{
                .file = file,
                .header = BucketHeader.read(header_bytes[0..@sizeOf(BucketHeader)]),
                .allocator = ally,
                .page_cache = std.AutoArrayHashMap(u64, Page).init(ally),
            };
        } else {
            // std.debug.print("\nFile doesnt exists, creating...\n ", .{});
            // If the file doesn't exist, create it
            _ = std.fs.cwd().createFile(path, .{}) catch return BucketInitErrors.FileOpenError;

            // Reopen the file for reading and writing
            file = std.fs.cwd().openFile(path, .{ .mode = .read_write }) catch {
                return BucketInitErrors.FileOpenError;
            };

            _ = file.write(BucketHeader.init().write()[0..64]) catch return BucketInitErrors.FileReadError;

            // std.debug.print("{d}", .{bytes_written});
            return .{
                .file = file,
                .header = BucketHeader.init(),
                .allocator = ally,
                .page_cache = std.AutoArrayHashMap(u64, Page).init(ally),
            };
        }
    }

    // Previous openFile implementation remains the same...

    pub fn loadPage(self: *Bucket, page_id: u64) !Page {
        // Check if the page is already in the cache

        if (self.page_cache.get(page_id)) |page| {
            return page;
        }

        const reader = self.file.reader();

        const offset = @sizeOf(BucketHeader) + (page_id * DEFAULT_PAGE_SIZE);
        try self.file.seekTo(offset);
        // Read the page header
        const header_bytes = try reader.readBytesNoEof(@sizeOf(PageHeader));
        const header = PageHeader.read(&header_bytes);

        const page = try Page.init(self.allocator, self, header);

        // Seek to the correct position: header size + (page_id * page size)
        // std.debug.print("Read page, offset {d}\n", .{offset});

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

        try self.page_cache.put(page_id, page);

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
        const page = try Page.init(self.allocator, self, PageHeader.init(page_type, new_page_id));
        try self.page_cache.put(new_page_id, page);
        return page;
    }

    // pub fn insertDoc(self: *Bucket, doc: bson.BSONDocument) !void {
    //     // const buf =
    //     // For now, we'll just print the document
    //     std.debug.print("Inserting document: {s}\n", .{});
    // }

    pub const DocHeader = struct { // 16bytes
        doc_id: u96,
        is_deleted: u8,
        reserved: u24,
        pub const byteSize = 16;

        pub fn init() DocHeader {
            return DocHeader{
                .is_deleted = 0,
                .doc_id = ObjectId.init().toInt(),
                .reserved = 0,
            };
        }

        pub fn fromMemory(memory: []const u8) error{ InvalidDocId, InvalidHeader }!DocHeader {
            const header = DocHeader{
                .is_deleted = memory[12],
                .doc_id = std.mem.readInt(u96, memory[0..12], .little),
                .reserved = std.mem.readInt(u24, memory[13..16], .little),
            };
            if (header.doc_id == 0) {
                return error.InvalidDocId;
            }
            if (header.reserved != 0) {
                std.debug.print("Header is correpted: {any} \n", .{header});
                @breakpoint();
                return error.InvalidHeader;
            }
            return header;
        }

        pub fn write(self: DocHeader, writer: anytype) !void {
            // Write the header to the writer
            try writer.writeInt(u96, self.doc_id, .little);
            try writer.writeInt(u8, self.is_deleted, .little);
            try writer.writeInt(u24, self.reserved, .little);
        }

        pub fn read(reader: anytype) !DocHeader {
            // Read the header from the reader
            const header = DocHeader{
                .doc_id = reader.readInt(u96, .little),
                .is_deleted = reader.readInt(u8, .little),
                .reserved = reader.readInt(u24, .little),
            };
            if (header.doc_id == 0) {
                return error.InvalidDocId;
            }
            return header;
        }
    };

    const DocInsertResult = struct {
        doc_id: u96,
        page_id: u64,
        offset: u16,
    };

    pub fn insert(self: *Bucket, insertable: *bson.BSONDocument) !DocInsertResult {
        var doc = insertable.*;
        const docId = ObjectId.init();
        const needCleanup = doc.get("_id") == null;
        if (needCleanup) {
            // If the document doesn't have an _id, generate one
            doc = try doc.set(self.allocator, "_id", BSONValue.init(docId));
        }
        defer {
            if (needCleanup) {
                doc.deinit(self.allocator);
            }
        }

        const doc_size = doc.buffer.len;
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
            page = try self.loadPage(page_id);
        }

        // Check if the page has enough space for header and doc size
        if ((page.data.len - page.header.used_size) < (4 + DocHeader.byteSize)) { // 20 bytes
            // Not enough space, create a new page
            page = try self.createNewPage(.Data);
        }

        // std.debug.print("\nDecided to write on page {d}, offset: {d}\n", .{ page.header.page_id, page.header.used_size });

        // Write the document header
        const doc_header = DocHeader{
            .is_deleted = 0,
            .doc_id = docId.toInt(),
            .reserved = 0,
        };
        const result = DocInsertResult{
            .doc_id = doc_header.doc_id,
            .page_id = page.header.page_id,
            .offset = page.header.used_size,
        };
        var stream = std.io.fixedBufferStream(page.data);
        try stream.seekTo(page.header.used_size);
        _ = try doc_header.write(stream.writer());
        page.header.used_size += @intCast(DocHeader.byteSize);

        var bytes_written: usize = 0;
        const bytes_to_write: usize = doc_size;

        while (bytes_written < bytes_to_write) {
            // Check if current page has enough space
            const available_space = page.data.len - page.header.used_size;

            // Determine how much to write
            const to_write = @min(bytes_to_write - bytes_written, available_space);
            // std.debug.print("Writing ({d}){d} bytes to the page \n", .{ page.header.used_size, to_write });
            _ = try stream.write(encoded_doc[bytes_written .. bytes_written + to_write]);
            // Insert document at the current position
            page.header.used_size += @intCast(to_write);
            bytes_written += to_write;

            // Write updated page back to disk
            self.page_cache.put(page.header.page_id, page) catch |err| {
                std.debug.print("Failed to cache page: {any}", .{err});
            };
            try self.writePage(&page);

            // If there are still bytes left to write, create a new page
            if (bytes_written < bytes_to_write) {
                page = try self.createNewPage(.Data);
                stream = std.io.fixedBufferStream(page.data);
            }
        }

        // After writing is done, free the page resources
        return result;
    }

    const DocumentLocation = struct {
        page_id: u64,
        offset: u16,
        header: DocHeader,
    };

    const IteratorResult = struct {
        page_id: u64,
        offset: u16,
        header: DocHeader,
        data: []u8,
    };

    pub const ScanIterator = struct {
        bucket: *Bucket,
        page: Page,
        allocator: std.mem.Allocator,
        initialized: bool = false,
        offset: u16 = 0,
        pageIterator: PageIterator,
        readDeleted: bool = false,
        resultBuffer: []u8 = undefined,
        resBufferIdx: usize = 0,

        fn init(bucket: *Bucket, allocator: std.mem.Allocator) !ScanIterator {
            return .{
                .bucket = bucket,
                .page = undefined,
                .allocator = allocator,
                .pageIterator = .{
                    .bucket = bucket,
                    .type = .Data,
                },
                .resultBuffer = try allocator.alloc(u8, 512 * 1024),
            };
        }

        pub fn deinit(_: *ScanIterator) void {
            // self.page.deinit(self.allocator);
        }

        pub fn step(self: *ScanIterator) !?DocumentLocation {
            if (!self.initialized) {
                self.initialized = true;
                self.page = try self.pageIterator.next() orelse return null;
                // std.debug.print("List iterator initialized, page {d} loaded\n", .{self.page.header.page_id});
            }

            if (self.offset >= (@as(u16, @truncate(self.page.data.len)) - 20)) {
                // Check if we need to load a new page
                self.page = try self.pageIterator.next() orelse return null;
                self.offset = 0;
            }

            const header = DocHeader.fromMemory(self.page.data[self.offset..]) catch |err| {
                if (err == error.InvalidDocId) {
                    return null; // No more documents
                }
                return err;
            };
            if (header.reserved != 0 or header.is_deleted > 1) @breakpoint();

            // std.debug.print("Page approved, header {x}\n", .{header.reserved});

            if (header.doc_id == 0) {
                return null; // No more documents
            }
            self.offset += DocHeader.byteSize;

            return .{
                .page_id = self.page.header.page_id,
                .offset = self.offset - DocHeader.byteSize,
                .header = header,
            };
        }
        pub fn next(self: *ScanIterator) !?IteratorResult {
            // Check if we have reached the end of the page

            var location = try self.step() orelse return null;
            // var header = location.header;

            while (location.header.is_deleted == 1 and !self.readDeleted) {

                // Skip deleted documents
                const docOffset = self.page.data[self.offset..];
                const doc_len = mem.readInt(u32, docOffset[0..4], .little);
                const availableToSkip: u16 = @as(u16, @truncate(self.page.data.len)) - self.offset;
                if (doc_len < availableToSkip) {
                    self.offset += @as(u16, @truncate(doc_len)); // No more documents
                } else {
                    const pageDataLen = self.page.data.len;
                    const pagesToSkip = @divFloor((doc_len - availableToSkip + pageDataLen), pageDataLen);
                    const newOffset = @mod((doc_len - availableToSkip + pageDataLen), pageDataLen);
                    for (0..pagesToSkip) |_| {
                        self.page = try self.pageIterator.next() orelse return null;
                    }
                    self.offset = @as(u16, @truncate(newOffset));
                }
                location = try self.step() orelse return null;
                if (location.header.is_deleted == 7) @breakpoint();
                // header = location.header;
                // std.debug.print("location header: {any}\n", .{location.header});
            }

            const doc_len = mem.readInt(u32, self.page.data[self.offset..][0..4], .little);
            var leftToCopy: u32 = doc_len;

            if (self.resBufferIdx + doc_len > self.resultBuffer.len) {
                // Resize the buffer if needed
                const newSize = @max(self.resBufferIdx + doc_len, self.resultBuffer.len * 2);
                self.resultBuffer = try self.allocator.alloc(u8, newSize);
            }
            var docBuffer = self.resultBuffer[self.resBufferIdx .. self.resBufferIdx + doc_len];
            self.resBufferIdx += doc_len;

            var writableBuffer = docBuffer[0..doc_len];
            var availableToCopy: u16 = @truncate(@min(doc_len, self.page.data.len - self.offset));
            @memcpy(writableBuffer[0..availableToCopy], self.page.data[self.offset .. self.offset + availableToCopy]);
            self.offset += @truncate(availableToCopy);
            leftToCopy -= availableToCopy;

            while (leftToCopy > 0) {
                writableBuffer = docBuffer[(doc_len - leftToCopy)..doc_len];
                // Check if we need to load a new page
                self.page = try self.pageIterator.next() orelse return null;
                availableToCopy = @truncate(@min(leftToCopy, self.page.data.len));

                @memcpy(writableBuffer, self.page.data[0..availableToCopy]);
                self.offset = availableToCopy;
                leftToCopy -= availableToCopy;
            }

            return .{
                .page_id = location.page_id,
                .offset = location.offset,
                .header = location.header,
                .data = docBuffer,
            };
        }
    };

    pub fn list(self: *Bucket, allocator: std.mem.Allocator, q: query.Query) ![]BSONDocument {
        // For now, we'll just print the document
        var docList = std.ArrayList(BSONDocument).init(allocator);
        if (q.sector) |sector| if (sector.limit) |limit| try docList.ensureTotalCapacity(limit);
        var iterator = try ScanIterator.init(self, allocator);
        var docsSkipped: u64 = 0;
        var iterRes = try iterator.next();
        while (iterRes) |docRaw| {
            const doc = BSONDocument.init(docRaw.data);
            if (!q.match(doc)) continue;
            if (q.sector) |sector| if (sector.offset) |offset| {
                if (docsSkipped < offset) {
                    docsSkipped += 1;
                    continue;
                }
            };
            try docList.append(doc);
            if (q.sector) |sector| if (sector.limit) |limit| {
                // std.debug.print("LIMIT: {d}/{d}", .{ limit, docList.items.len });
                if (docList.items.len >= limit) {
                    break;
                }
            };
            iterRes = try iterator.next();
        }

        const resultSlice = try docList.toOwnedSlice();

        if (q.sortConfig) |sortConfig| {
            std.mem.sort(BSONDocument, resultSlice, sortConfig, query.Query.sort);
        }

        return resultSlice;
        // std.debug.print("DOCS GOOD", .{});
    }

    pub fn delete(self: *Bucket, q: query.Query) !void {
        // For now, we'll just print the document
        var arena = std.heap.ArenaAllocator.init(self.allocator);
        defer arena.deinit();
        const allocator = arena.allocator();
        var locations = std.ArrayList(DocumentLocation).init(allocator);
        var iterator = try ScanIterator.init(self, allocator);
        var docsAdded: u64 = 0;
        var docsSkipped: u64 = 0;
        // std.debug.print("iterator state: {any}\n", .{iterator.offset});
        while (try iterator.next()) |doc| {
            const matched = q.match(.{ .buffer = doc.data });
            if (!matched) continue;
            if (q.sector) |sector| if (sector.offset) |offset| {
                if (docsSkipped < offset) {
                    docsSkipped += 1;
                    continue;
                }
            };
            try locations.append(.{
                .header = doc.header,
                .page_id = doc.page_id,
                .offset = doc.offset,
            });
            // std.debug.print("mark deleted at: {any}\n", .{DocumentLocation{
            //     .header = doc.header,
            //     .page_id = doc.page_id,
            //     .offset = doc.offset,
            // }});
            docsAdded += 1;
            if (q.sector) |sector| if (sector.limit) |limit| {
                if (docsAdded >= limit) {
                    break;
                }
            };
        }
        // std.debug.print("iterator state: {any}\n", .{iterator.offset});
        const writer = self.file.writer();
        self.rwlock.lock();
        for (locations.items) |*location| {
            _ = self.page_cache.swapRemove(location.page_id);
            location.header.is_deleted = 1;
            var file = self.file;
            const offset = @sizeOf(BucketHeader) + @sizeOf(PageHeader) + (location.page_id * DEFAULT_PAGE_SIZE);
            try file.seekTo(offset + location.offset);
            try location.header.write(writer);
        }
        self.rwlock.unlock();
    }

    pub fn deinit(self: *Bucket) void {
        self.file.close();
        defer self.page_cache.deinit();
        for (self.page_cache.values()) |*page| {
            page.deinit(self.allocator);
        }
    }
};

// test "Bucket.insert" {
//     var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
//     defer arena.deinit();
//     const allocator = arena.allocator();
//     var bucket = try Bucket.init(allocator, "test.bucket");
//     defer bucket.deinit();

//     // Create a new BSON document
//     var doc = try bson.BSONDocument.fromJSON(allocator,
//         \\{
//         \\  "name": "Alice",
//         \\  "age": 37
//         \\}
//     );

//     // Insert the document into the bucket
//     _ = try bucket.insert(&doc);

//     const q = try query.Query.parse(allocator, try bson.BSONDocument.fromJSON(allocator,
//         \\{
//         \\  "sector": {}
//         \\}
//     ));

//     _ = try bucket.list(allocator, q);

//     // for (qResult) |item| {
//     //     // const oId = item.get("_id").?.objectId.value;
//     //     // std.debug.print("Document _id: {s}, timestamp: {any}\n", .{ oId.toString(), oId });
//     // }
// }

// test "DocHeader.write" {
//     var fixedBuffer: [64]u8 = [_]u8{0} ** 64;
//     var stream = std.io.fixedBufferStream(&fixedBuffer);

//     var doc_header = Bucket.DocHeader{
//         .is_deleted = 0,
//         .doc_id = ObjectId.init().toInt(),
//         .reserved = 0,
//     };
//     try doc_header.write(stream.writer());
//     try stream.seekTo(0);
//     doc_header = try Bucket.DocHeader.read(stream.reader());
// }

test "Bucket.delete" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();
    var bucket = try Bucket.init(allocator, "DELETE.bucket");
    defer bucket.deinit();

    // Create a new BSON document
    var doc = try bson.BSONDocument.fromJSON(allocator,
        \\ {
        \\  "name": "Alice",
        \\  "age": "delete me"
        \\ }
    );

    // Insert the document into the bucket
    _ = try bucket.insert(&doc);

    const listQ = try query.Query.parse(allocator, try bson.BSONDocument.fromJSON(allocator,
        \\{
        \\  "query": {}
        \\}
    ));

    var docs = try bucket.list(allocator, listQ);
    const docCount = docs.len;

    std.debug.print("Doc len before delete {d}\n", .{docCount});

    const deleteQ = try query.Query.parse(allocator, try bson.BSONDocument.fromJSON(allocator,
        \\{
        \\  "query": {"name": "Alice"}
        \\}
    ));
    // Delete the document from the bucket
    _ = try bucket.delete(deleteQ);

    docs = try bucket.list(allocator, listQ);
    const newDocCount = docs.len;
    std.debug.print("Doc len after delete {d}\n", .{newDocCount});

    try std.testing.expect(newDocCount < docCount);

    _ = try bucket.insert(&doc);

    docs = try bucket.list(allocator, listQ);
    const docCountAfterInsert = docs.len;
    std.debug.print("Doc len after insert {d}\n", .{docCountAfterInsert});
    try std.testing.expect(docCountAfterInsert == docCount);
    _ = try bucket.delete(listQ);
}
