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
    doc_count: u64,
    deleted_count: u64,
    reserved: [28]u8, // Padding to make the struct 64 bytes

    const byteSize = 64;

    pub fn init() BucketHeader {
        var header = BucketHeader{
            .magic = undefined,
            .version = ALBEDO_VERSION,
            .flags = ALBEDO_FLAGS,
            .page_size = DEFAULT_PAGE_SIZE,
            .page_count = 0,
            .doc_count = 0,
            .deleted_count = 0,
            .reserved = [_]u8{0} ** 28,
        };

        std.mem.copyForwards(u8, &header.magic, ALBEDO_MAGIC);
        // std.mem.copyForwards(u8, &header.reserved, [_]u8{0} ** 28);

        return header;
    }

    pub fn read(memory: []const u8) BucketHeader {
        var header = BucketHeader{
            .magic = undefined,
            .version = memory[6],
            .flags = memory[7],
            .page_size = std.mem.readInt(u32, memory[8..12], .little),
            .page_count = std.mem.readInt(u64, memory[12..20], .little),
            .doc_count = std.mem.readInt(u64, memory[20..28], .little),
            .deleted_count = std.mem.readInt(u64, memory[28..36], .little),
            .reserved = undefined,
        };

        std.mem.copyForwards(u8, header.magic[0..], ALBEDO_MAGIC);
        std.mem.copyForwards(u8, header.reserved[0..], &[_]u8{0} ** 28);

        return header;
    }

    pub fn write(self: *const BucketHeader, writer: anytype) !void {

        // var buffer: [64]u8 = [_]u8{0} ** 64;
        _ = try writer.write(ALBEDO_MAGIC);
        _ = try writer.writeByte(self.version);
        _ = try writer.writeByte(self.flags);
        _ = try writer.writeInt(u32, self.page_size, .little);
        _ = try writer.writeInt(u64, self.page_count, .little);
        _ = try writer.writeInt(u64, self.doc_count, .little);
        _ = try writer.writeInt(u64, self.deleted_count, .little);
        _ = try writer.write(&self.reserved);
        // return buffer;
    }
};

const PageType = enum(u8) {
    Data = 1,
    Index = 2,
    Free = 3,
    Meta = 4,
};

const PageHeader = struct {
    page_type: PageType, // 1
    used_size: u16 = 0, // 2
    page_id: u64, // 8
    first_readable_byte: u16, // 2
    reserved: [19]u8, // 19

    pub const byteSize = 32;

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

pub const Page = struct {
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

    fn loadIndices(self: *const Page, hash: *std.StringHashMap(u64)) !void {
        var idx: u16 = 0;
        var data = self.data;
        while (data[idx] != 0) {
            // Read the index path
            const path = mem.span(@as([*:0]u8, @ptrCast(data[idx..].ptr)));
            idx += @truncate(path.len + 1);

            // Read the index page ID
            const page_id = mem.readInt(u64, @ptrCast(data[idx .. idx + 8]), .little);
            idx += @sizeOf(u64);

            // Insert the index into the hash map
            try hash.put(path, page_id);
        }
    }
    // Load indices from the page
};

const BucketInitErrors = error{
    FileStatError,
    InvalidPath,
    InvalidDocId,
    FileOpenError,
    FileReadError,
    LoadIndexError,
    InitializationError,
    OutOfMemory,
};

const tree = @import("btree.zig");

pub const Bucket = struct {
    file: std.fs.File,
    path: []const u8,
    allocator: std.mem.Allocator,
    header: BucketHeader,
    pageCache: std.AutoHashMap(u64, Page),
    rwlock: std.Thread.RwLock = .{},
    indexes: std.StringHashMap(u64),
    autoVaccuum: bool = true,

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
    pub fn init(allocator: std.mem.Allocator, path: []const u8) !Bucket {
        const bucket = try Bucket.openFile(allocator, path);
        // bucket.pageCache = std.AutoHashMap(u64, Page).init(allocator);
        return bucket;
    }

    const BucketFileMode = enum {
        ReadOnly,
        ReadWrite,
    };

    const OpenBucketOptions = struct {
        buildIdIndex: bool = false,
        mode: BucketFileMode = BucketFileMode.ReadWrite,
        autoVaccuum: bool = true,
    };

    pub fn openFile(ally: std.mem.Allocator, path: []const u8) BucketInitErrors!Bucket {
        return Bucket.openFileWithOptions(ally, path, .{});
    }

    pub fn openFileWithOptions(ally: std.mem.Allocator, path: []const u8, options: OpenBucketOptions) BucketInitErrors!Bucket {
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
                .{ .mode = if (options.mode == .ReadWrite) .read_write else .read_only },
            ) catch return BucketInitErrors.FileOpenError;
            const header_bytes = file.reader().readBytesNoEof(BucketHeader.byteSize) catch return BucketInitErrors.FileReadError;
            var bucket = Bucket{
                .file = file,
                .path = path,
                .header = BucketHeader.read(header_bytes[0..BucketHeader.byteSize]),
                .allocator = ally,
                .pageCache = std.AutoHashMap(u64, Page).init(ally),
                .indexes = std.StringHashMap(u64).init(ally),
            };

            const meta = bucket.loadPage(0) catch return BucketInitErrors.FileReadError;

            meta.loadIndices(&bucket.indexes) catch return BucketInitErrors.LoadIndexError;

            if (options.buildIdIndex) {
                // bucket.buildIndex("_id");
            }

            return bucket;
        } else {
            // std.debug.print("\nFile doesnt exists, creating...\n ", .{});
            // If the file doesn't exist, create it
            _ = std.fs.cwd().createFile(path, .{}) catch return BucketInitErrors.FileOpenError;

            // Reopen the file for reading and writing
            file = std.fs.cwd().openFile(path, .{ .mode = if (options.mode == .ReadWrite) .read_write else .read_only }) catch {
                return BucketInitErrors.FileOpenError;
            };

            // std.debug.print("{d}", .{bytes_written});
            var bucket = Bucket{
                .file = file,
                .path = path,
                .header = BucketHeader.init(),
                .allocator = ally,
                .pageCache = std.AutoHashMap(u64, Page).init(ally),
                .indexes = std.StringHashMap(u64).init(ally),
            };

            bucket.flushHeader() catch return BucketInitErrors.FileReadError;

            // Page 0 is the meta page
            // It stores information about the indices and other metadata
            const meta = bucket.createNewPage(.Meta) catch return BucketInitErrors.FileReadError;
            bucket.writePage(&meta) catch return BucketInitErrors.InitializationError;
            // std.debug.print("Created meta page", .{});
            // try bucket.indexes.put("_id", tree.BPlusTree.init(bucket.allocator));

            return bucket;
        }
    }

    fn flushHeader(self: *const Bucket) !void {
        // Flush the header to disk
        try self.file.seekTo(0);
        const writer = self.file.writer();
        _ = try self.header.write(writer);
        try self.file.sync(); // Ensure the write is flushed to disk
    }

    pub fn buildIndex(self: *Bucket, _: []const u8) !void {
        // Check if the index already exists
        _ = try self.createNewPage(.Index);

        // try self.indexes.put(path, newTree);
    }

    // Previous openFile implementation remains the same...

    pub fn loadPage(self: *Bucket, page_id: u64) !Page {
        // Check if the page is already in the cache

        if (self.pageCache.get(page_id)) |page| {
            return page;
        }

        const reader = self.file.reader();

        const offset = BucketHeader.byteSize + (page_id * DEFAULT_PAGE_SIZE);
        try self.file.seekTo(offset);
        // Read the page header
        const header_bytes = try reader.readBytesNoEof(PageHeader.byteSize);
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

        try self.pageCache.put(page_id, page);

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
        try self.flushHeader(); // Ensure the header is flushed to disk
        const page = try Page.init(self.allocator, self, PageHeader.init(page_type, new_page_id));
        try self.pageCache.put(new_page_id, page);
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
                // @breakpoint();
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

    fn findLastDataPage(self: *Bucket) !?Page {
        // Find the last data page
        var pageIter = PageIterator{
            .bucket = self,
            .index = 0,
            .type = PageType.Data,
        };
        while (true) {
            const page = try pageIter.next() orelse return null;
            if (page.header.page_type == PageType.Data) {
                return page;
            }
        }
        return null;
    }

    pub fn insert(self: *Bucket, insertable: bson.BSONDocument) !DocInsertResult {
        var doc = insertable;
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
        self.rwlock.lock();
        defer self.rwlock.unlock();
        // If no pages exist yet, create one
        if (self.header.page_count == 1) {
            page = try self.createNewPage(.Data);
            try self.writePage(&page);
        } else {
            // Start from the last page
            page = try findLastDataPage(self) orelse unreachable;
        }

        // Check if the page has enough space for header and doc size
        if ((page.data.len - page.header.used_size) < (4 + DocHeader.byteSize)) { // 20 bytes
            // Not enough space, create a new page
            page = try self.createNewPage(.Data);
            try self.writePage(&page);
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
            self.pageCache.put(page.header.page_id, page) catch |err| {
                std.debug.print("Failed to cache page: {any}", .{err});
            };
            try self.writePage(&page);

            // If there are still bytes left to write, create a new page
            if (bytes_written < bytes_to_write) {
                page = try self.createNewPage(.Data);
                try self.writePage(&page);
                stream = std.io.fixedBufferStream(page.data);
            }
        }

        self.header.doc_count += 1;
        try self.flushHeader();

        // for (self.indexes.keys()) |indexPath| {
        //     const index = self.indexes.get(indexPath) catch unreachable;
        //     const value = doc.get(indexPath);
        //     if (value == null or value.? == .binary or value.? == .document) {
        //         index.insert(
        //             .{ .null = .{} },
        //             .{ .offset = result.offset, .pageId = result.page_id },
        //         );
        //         continue;
        //         // This is obly for sparse indexes
        //         //continue;
        //     }

        //     if (value.?.string and value.?.string.len > 1024) {
        //         return error.StringIndexTooLong;
        //     } else {
        //         // copy the string to a new buffer
        //         const newBuffer = try self.allocator.alloc(u8, value.?.string.len);
        //         @memcpy(newBuffer, value.?.string);
        //         value = .{ .string = .{ .value = newBuffer } };
        //     }
        //     // std.debug.print("Inserting into index {s}\n", .{indexPath});
        //     index.insert(value.?, .{ .offset = result.offset, .pageId = result.page_id });
        // }

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
                // header = location.header;
                // std.debug.print("location header: {any}\n", .{location.header});
            }

            const doc_len = mem.readInt(u32, self.page.data[self.offset..][0..4], .little);
            var leftToCopy: u32 = doc_len;

            if (self.resBufferIdx + doc_len > self.resultBuffer.len) {
                @branchHint(.unlikely);
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
        var docList = std.ArrayList(BSONDocument).init(allocator);
        if (q.sector) |sector| if (sector.limit) |limit| try docList.ensureTotalCapacity(limit);
        var iterator = try ScanIterator.init(self, allocator);

        while (try iterator.next()) |docRaw| {
            const doc = BSONDocument.init(docRaw.data);
            if (q.filters.len == 0 or q.match(doc)) {
                try docList.append(doc);
            }
        }

        const resultSlice = docList.items;

        if (q.sortConfig) |sortConfig| {
            std.mem.sort(BSONDocument, resultSlice, sortConfig, query.Query.sort);
        }
        // const sorted = try std.time.Instant.now();
        // std.debug.print("List iterator sorted at {d}ms\n", .{@divFloor(sorted.since(finished), 1_000_000)});

        const offset = if (q.sector) |sector| sector.offset orelse 0 else 0;
        const limit = if (q.sector) |sector| sector.limit orelse resultSlice.len else resultSlice.len;

        return resultSlice[@min(resultSlice.len, offset)..@min(offset + limit, resultSlice.len)];
        // std.debug.print("DOCS GOOD", .{});
    }

    pub fn delete(self: *Bucket, q: query.Query) !void {
        // For now, we'll just print the document
        var arena = std.heap.ArenaAllocator.init(self.allocator);
        defer arena.deinit();
        const allocator = arena.allocator();
        var locations = std.ArrayList(DocumentLocation).init(allocator);
        var iterator = try ScanIterator.init(self, allocator);
        self.rwlock.lockShared();

        // std.debug.print("iterator state: {any}\n", .{iterator.offset});
        while (try iterator.next()) |doc| {
            const matched = q.match(.{ .buffer = doc.data });
            if (!matched) continue;
            try locations.append(.{
                .header = doc.header,
                .page_id = doc.page_id,
                .offset = doc.offset,
            });
        }
        self.rwlock.unlockShared();
        // std.debug.print("iterator state: {any}\n", .{iterator.offset});
        const writer = self.file.writer();
        self.rwlock.lock();
        defer self.rwlock.unlock();
        for (locations.items) |*location| {
            _ = self.pageCache.remove(location.page_id);
            location.header.is_deleted = 1;
            var file = self.file;
            const offset = BucketHeader.byteSize + @sizeOf(PageHeader) + (location.page_id * DEFAULT_PAGE_SIZE);
            try file.seekTo(offset + location.offset);
            try location.header.write(writer);
        }

        self.header.doc_count -= locations.items.len;
        self.header.deleted_count += locations.items.len;
        try self.flushHeader();

        if (self.header.deleted_count > self.header.doc_count) {
            // If all documents are deleted, reset the deleted count
            try self.vacuum();
        }
    }

    pub fn vacuum(self: *Bucket) !void {
        const tempFileName = try std.fmt.allocPrint(self.allocator, "{s}-temp", .{self.path});
        defer self.allocator.free(tempFileName);

        var newBucket = try Bucket.openFile(self.allocator, tempFileName);
        const cwd = std.fs.cwd();
        defer newBucket.deinit();
        // defer cwd.deleteFile(tempFileName) catch |err| {
        //     std.debug.print("Failed to delete temp file: {any}\n", .{err});
        // };
        var iterator = try ScanIterator.init(self, self.allocator);
        while (try iterator.next()) |doc| {
            const newDoc = bson.BSONDocument.init(doc.data);
            _ = try newBucket.insert(newDoc);
        }
        iterator.deinit();
        self.deinit();
        cwd.deleteFile(self.path) catch |err| {
            std.debug.print("Failed to delete old file: {any}\n", .{err});
            return err;
        };
        cwd.rename(tempFileName, self.path) catch |err| {
            std.debug.print("Failed to rename temp file: {any}\n", .{err});
            return err;
        };

        self.file = try cwd.openFile(self.path, .{ .mode = .read_write });

        try self.file.seekTo(0);
        var header_bytes = try self.file.reader().readBytesNoEof(64);
        self.header = BucketHeader.read(header_bytes[0..]);
        self.pageCache = std.AutoHashMap(u64, Page).init(self.allocator);
    }

    pub fn deinit(self: *Bucket) void {
        self.file.close();
        defer self.pageCache.deinit();
        var cacheIter = self.pageCache.iterator();
        while (cacheIter.next()) |*pair| {
            pair.value_ptr.*.deinit(self.allocator);
        }
    }
};

const testing = std.testing;

test "Bucket.insert" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();
    var bucket = try Bucket.init(allocator, "test.bucket");
    defer bucket.deinit();
    defer std.fs.cwd().deleteFile("test.bucket") catch |err| {
        std.debug.print("Failed to delete test file: {any}\n", .{err});
    };

    // Create a new BSON document
    const doc = try bson.BSONDocument.fromJSON(allocator,
        \\{
        \\  "name": "Alice",
        \\  "age": 37
        \\}
    );

    // Insert the document into the bucket
    _ = try bucket.insert(doc);

    const q1 = try query.Query.parse(allocator, try bson.BSONDocument.fromJSON(allocator,
        \\{
        \\  "sector": {}
        \\}
    ));

    const res1 = try bucket.list(allocator, q1);

    try testing.expect(res1.len == 1);
    try testing.expectEqualStrings(res1[0].get("name").?.string.value, "Alice");

    const q2 = try query.Query.parse(allocator, try bson.BSONDocument.fromJSON(allocator,
        \\{
        \\  "query": {"name": "Alice"}
        \\}
    ));

    const res2 = try bucket.list(allocator, q2);

    try testing.expect(res1.len == res2.len);
    try testing.expectEqualStrings(res1[0].get("name").?.string.value, "Alice");

    // for (qResult) |item| {
    //     // const oId = item.get("_id").?.objectId.value;
    //     // std.debug.print("Document _id: {s}, timestamp: {any}\n", .{ oId.toString(), oId });
    // }

}

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
    const doc = try bson.BSONDocument.fromJSON(allocator,
        \\ {
        \\  "name": "Alice",
        \\  "age": "delete me"
        \\ }
    );

    // Insert the document into the bucket
    _ = try bucket.insert(doc);

    const listQ = try query.Query.parse(allocator, try bson.BSONDocument.fromJSON(allocator,
        \\{
        \\  "query": {}
        \\}
    ));

    var docs = try bucket.list(allocator, listQ);
    const docCount = docs.len;

    std.debug.print("Doc len before vacuum {d}\n", .{docCount});
    try std.testing.expect(docCount == 1);

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

    _ = try bucket.insert(doc);

    docs = try bucket.list(allocator, listQ);
    const docCountAfterInsert = docs.len;
    std.debug.print("Doc len after insert {d}\n", .{docCountAfterInsert});
    try std.testing.expect(docCountAfterInsert == docCount);
    _ = try bucket.delete(listQ);
}

test "Deleted docs disappear after vacuum" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();
    var bucket = try Bucket.init(allocator, "VACUUM.bucket");
    defer bucket.deinit();
    defer std.fs.cwd().deleteFile("VACUUM.bucket") catch |err| {
        std.debug.print("Failed to delete test file: {any}\n", .{err});
    };

    // Create a new BSON document
    const doc = try bson.BSONDocument.fromJSON(allocator,
        \\ {
        \\  "name": "Alice",
        \\  "age": "delete me"
        \\ }
    );

    const docGood = try bson.BSONDocument.fromJSON(allocator,
        \\ {
        \\  "name": "not Alice",
        \\  "age": "delete me"
        \\ }
    );

    // Insert the document into the bucket
    _ = try bucket.insert(doc);
    _ = try bucket.insert(docGood);

    const listQ = try query.Query.parse(allocator, try bson.BSONDocument.fromJSON(allocator,
        \\{
        \\  "query": {}
        \\}
    ));

    const docs = try bucket.list(allocator, listQ);
    const docCount = docs.len;

    std.debug.print("Doc len before delete {d}\n", .{docCount});

    const deleteQ = try query.Query.parse(allocator, try bson.BSONDocument.fromJSON(allocator,
        \\{
        \\  "query": {"name": "Alice"}
        \\}
    ));

    bucket.delete(deleteQ) catch |err| {
        std.debug.print("Failed to delete document: {any}\n", .{err});
    };

    bucket.vacuum() catch |err| {
        std.debug.print("Failed to vacuum bucket: {any}\n", .{err});
        return err;
    };

    const afterVacuumDocs = try bucket.list(allocator, listQ);
    const afterVacuumDocCount = afterVacuumDocs.len;

    try std.testing.expect(afterVacuumDocCount == 1);
}
