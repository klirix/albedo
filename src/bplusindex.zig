const std = @import("std");
const mem = std.mem;
const albedo = @import("albedo.zig");
const bson = @import("bson.zig");
const query = @import("query.zig");
const BSONValue = bson.BSONValue;
const Bucket = albedo.Bucket;
const Page = albedo.Page;

pub const IndexOptions = packed struct {
    unique: u1 = 0,
    sparse: u1 = 0,
    reverse: u1 = 0,
    reserved: u5 = 0,
};

pub const Index = struct {
    bucket: *Bucket,
    allocator: mem.Allocator,
    options: IndexOptions = .{},
    root_page_id: u64,

    pub const RangeBoundError = error{
        InvalidLowerBoundOperator,
        InvalidUpperBoundOperator,
    };

    pub const RangeBound = struct {
        filter: query.FilterType,
        value: BSONValue,

        pub fn gt(value: BSONValue) RangeBound {
            return .{ .filter = .gt, .value = value };
        }

        pub fn gte(value: BSONValue) RangeBound {
            return .{ .filter = .gte, .value = value };
        }

        pub fn lt(value: BSONValue) RangeBound {
            return .{ .filter = .lt, .value = value };
        }

        pub fn lte(value: BSONValue) RangeBound {
            return .{ .filter = .lte, .value = value };
        }
    };

    pub const DocumentLocation = struct {
        pageId: u64,
        offset: u16,
        inline fn equal(self: *const DocumentLocation, other: *const DocumentLocation) bool {
            return self.pageId == other.pageId and self.offset == other.offset;
        }
    };

    const SplitPayload = struct {
        key_type: bson.BSONValueType,
        key_bytes: []const u8,
        owned_bytes: ?[]u8,
        right_page: *Page,

        inline fn value(self: SplitPayload) BSONValue {
            return BSONValue.read(self.key_bytes, self.key_type);
        }

        inline fn keyLen(self: SplitPayload) usize {
            return self.key_bytes.len;
        }

        fn deinit(self: SplitPayload, allocator: mem.Allocator) void {
            if (self.owned_bytes) |bytes| {
                allocator.free(bytes);
            }
        }
    };

    const Node = struct {

        // Data layout:
        //   Leaf node:
        //     u8 = 1
        //     u64 = previous leaf nodeId
        //     u64 = next leaf nodeId
        //     Repeatedly:
        //       u8 = BSONValueType
        //       [n]u8 = BSONValue
        //       u64 = documentId
        //       u16 = document offset
        //   Internal node: min theoretical cap = 30, max theoretical cap = 906
        //     u8 = 0
        //       u64 = child nodeId
        //     Repeatedly:
        //       u8 = BSONValueType
        //       [n]u8 = BSONValue
        //       u64 = child nodeId

        page: *Page,
        index: *Index,
        const leaf_header_size: u16 = 1 + 8 + 8;
        const internal_header_size: u16 = 1 + 8;

        const LeafEntry = struct {
            offset: u16,
            total_size: u16,
            value_type: bson.BSONValueType,
            value_bytes: []const u8,
            location: DocumentLocation,

            inline fn value(self: LeafEntry) BSONValue {
                return BSONValue.read(self.value_bytes, self.value_type);
            }
        };

        const InternalEntry = struct {
            offset: u16,
            total_size: u16,
            value_type: bson.BSONValueType,
            value_bytes: []const u8,
            right_child: u64,

            inline fn value(self: InternalEntry) BSONValue {
                return BSONValue.read(self.value_bytes, self.value_type);
            }
        };

        inline fn init(index: *Index, page: *Page) Node {
            return .{
                .page = page,
                .index = index,
            };
        }

        inline fn id(self: *const Node) u64 {
            return self.page.header.page_id;
        }

        inline fn isLeaf(self: *const Node) bool {
            return self.page.data[0] == 1;
        }

        inline fn leafDataStart() u16 {
            return leaf_header_size;
        }

        inline fn internalDataStart() u16 {
            return internal_header_size;
        }

        inline fn leafEntrySize(value: *const BSONValue) usize {
            return 1 + @as(usize, value.size()) + 8 + 2;
        }

        fn leafPrevId(self: *const Node) u64 {
            const bytes = self.page.data[1..9];
            return mem.readInt(u64, @as(*const [8]u8, @ptrCast(bytes.ptr)), .little);
        }

        fn leafNextId(self: *const Node) u64 {
            const bytes = self.page.data[9..17];
            return mem.readInt(u64, @as(*const [8]u8, @ptrCast(bytes.ptr)), .little);
        }

        fn setLeafPrevId(self: *Node, value: u64) void {
            const bytes = self.page.data[1..9];
            mem.writeInt(u64, @as(*[8]u8, @ptrCast(bytes.ptr)), value, .little);
        }

        fn setLeafNextId(self: *Node, value: u64) void {
            const bytes = self.page.data[9..17];
            mem.writeInt(u64, @as(*[8]u8, @ptrCast(bytes.ptr)), value, .little);
        }

        fn internalFirstChildId(self: *const Node) u64 {
            const bytes = self.page.data[1..9];
            return mem.readInt(u64, @as(*const [8]u8, @ptrCast(bytes.ptr)), .little);
        }

        fn leafEndOffset(self: *const Node) u16 {
            const explicit = self.page.header.used_size;
            if (explicit != 0) return explicit;
            return computeLeafEnd(self.page.data);
        }

        fn internalEndOffset(self: *const Node) u16 {
            const explicit = self.page.header.used_size;
            if (explicit != 0) return explicit;
            return computeInternalEnd(self.page.data);
        }

        fn computeLeafEnd(data: []const u8) u16 {
            var offset: usize = leaf_header_size;
            while (offset < data.len) {
                const type_byte = data[offset];
                if (type_byte == 0) break;
                if (offset + 1 >= data.len) break;
                const type_id: bson.BSONValueType = @enumFromInt(type_byte);
                const value_size = BSONValue.asessSize(data[offset + 1 ..], type_id);
                const total = @as(usize, 1) + @as(usize, value_size) + 10;
                if (offset + total > data.len) break;
                offset += total;
            }
            return @intCast(offset);
        }

        fn computeInternalEnd(data: []const u8) u16 {
            var offset: usize = internal_header_size;
            while (offset < data.len) {
                const type_byte = data[offset];
                if (type_byte == 0) break;
                if (offset + 1 >= data.len) break;
                const type_id: bson.BSONValueType = @enumFromInt(type_byte);
                const value_size = BSONValue.asessSize(data[offset + 1 ..], type_id);
                const total = @as(usize, 1) + @as(usize, value_size) + 8;
                if (offset + total > data.len) break;
                offset += total;
            }
            return @intCast(offset);
        }

        fn decodeLeafEntry(self: *const Node, offset: u16) ?LeafEntry {
            const data = self.page.data;
            if (offset >= data.len) return null;
            const type_byte = data[offset];
            if (type_byte == 0) return null;
            if (offset + 1 > data.len) return null;
            const type_id: bson.BSONValueType = @enumFromInt(type_byte);
            const value_size_u32 = BSONValue.asessSize(data[offset + 1 ..], type_id);
            const value_size = @as(usize, value_size_u32);
            if (value_size > std.math.maxInt(u16)) return null;
            const value_start = @as(usize, offset) + 1;
            const value_end = value_start + value_size;
            if (value_end + 10 > data.len) return null;
            const page_bytes = data[value_end .. value_end + 8];
            const page_id = mem.readInt(u64, @as(*const [8]u8, @ptrCast(page_bytes.ptr)), .little);
            const offset_bytes = data[value_end + 8 .. value_end + 10];
            const page_offset = mem.readInt(u16, @as(*const [2]u8, @ptrCast(offset_bytes.ptr)), .little);
            return LeafEntry{
                .offset = offset,
                .total_size = @intCast(@as(usize, 1) + value_size + 10),
                .value_type = type_id,
                .value_bytes = data[value_start..value_end],
                .location = .{ .pageId = page_id, .offset = page_offset },
            };
        }

        fn decodeInternalEntry(self: *const Node, offset: u16) ?InternalEntry {
            const data = self.page.data;
            if (offset >= data.len) return null;
            const type_byte = data[offset];
            if (type_byte == 0) return null;
            if (offset + 1 > data.len) return null;
            const type_id: bson.BSONValueType = @enumFromInt(type_byte);
            const value_size_u32 = BSONValue.asessSize(data[offset + 1 ..], type_id);
            const value_size = @as(usize, value_size_u32);
            if (value_size > std.math.maxInt(u16)) return null;
            const value_start = @as(usize, offset) + 1;
            const value_end = value_start + value_size;
            if (value_end + 8 > data.len) return null;
            const child_bytes = data[value_end .. value_end + 8];
            const child_id = mem.readInt(u64, @as(*const [8]u8, @ptrCast(child_bytes.ptr)), .little);
            return InternalEntry{
                .offset = offset,
                .total_size = @intCast(@as(usize, 1) + value_size + 8),
                .value_type = type_id,
                .value_bytes = data[value_start..value_end],
                .right_child = child_id,
            };
        }

        fn leafEntryCount(self: *const Node) usize {
            var count: usize = 0;
            var offset: u16 = leaf_header_size;
            while (true) {
                const entry = self.decodeLeafEntry(offset) orelse break;
                count += 1;
                offset = entry.offset + entry.total_size;
            }
            return count;
        }

        fn internalKeyCount(self: *const Node) usize {
            var count: usize = 0;
            var offset: u16 = internal_header_size;
            while (true) {
                const entry = self.decodeInternalEntry(offset) orelse break;
                count += 1;
                offset = entry.offset + entry.total_size;
            }
            return count;
        }

        fn leafEntryAt(self: *const Node, index: usize) ?LeafEntry {
            var cursor: usize = 0;
            var offset: u16 = leaf_header_size;
            while (true) {
                const entry = self.decodeLeafEntry(offset) orelse return null;
                if (cursor == index) return entry;
                offset = entry.offset + entry.total_size;
                cursor += 1;
            }
        }

        fn internalEntryAt(self: *const Node, index: usize) ?InternalEntry {
            var cursor: usize = 0;
            var offset: u16 = internal_header_size;
            while (true) {
                const entry = self.decodeInternalEntry(offset) orelse return null;
                if (cursor == index) return entry;
                offset = entry.offset + entry.total_size;
                cursor += 1;
            }
        }

        fn leafFirstEntry(self: *const Node) ?LeafEntry {
            return self.decodeLeafEntry(leaf_header_size);
        }

        fn leafLastEntry(self: *const Node) ?LeafEntry {
            var offset: u16 = leaf_header_size;
            var result: ?LeafEntry = null;
            while (true) {
                const entry = self.decodeLeafEntry(offset) orelse break;
                result = entry;
                offset = entry.offset + entry.total_size;
            }
            return result;
        }

        fn leafFindInsertOffset(self: *const Node, value: BSONValue) u16 {
            var offset: u16 = leaf_header_size;
            while (true) {
                const entry = self.decodeLeafEntry(offset) orelse return offset;
                const cmp = entry.value().order(value);
                if (cmp == .gt) return entry.offset;
                offset = entry.offset + entry.total_size;
            }
        }

        fn leafFindFirstEqualOffset(self: *const Node, value: BSONValue) ?u16 {
            var offset: u16 = leaf_header_size;
            while (true) {
                const entry = self.decodeLeafEntry(offset) orelse return null;
                const cmp = entry.value().order(value);
                switch (cmp) {
                    .lt => offset = entry.offset + entry.total_size,
                    .eq => return entry.offset,
                    .gt => return null,
                }
            }
        }

        fn leafLowerBoundOffset(self: *const Node, value: BSONValue, inclusive: bool) u16 {
            var offset: u16 = leaf_header_size;
            while (true) {
                const entry = self.decodeLeafEntry(offset) orelse return offset;
                const cmp = entry.value().order(value);
                switch (cmp) {
                    .lt => offset = entry.offset + entry.total_size,
                    .eq => {
                        if (inclusive) return entry.offset;
                        offset = entry.offset + entry.total_size;
                    },
                    .gt => return entry.offset,
                }
            }
        }

        fn leafInsert(self: *Node, value: BSONValue, location: DocumentLocation) !void {
            const insert_offset = self.leafFindInsertOffset(value);
            const insert_offset_u = @as(usize, insert_offset);
            const value_size = @as(usize, value.size());
            const entry_size = 1 + value_size + 8 + 2;
            const end = @as(usize, self.leafEndOffset());
            if (end + entry_size > self.page.data.len) return error.NodeFull;

            const tail_len = end - insert_offset_u;
            if (tail_len > 0) {
                std.mem.copyBackwards(
                    u8,
                    self.page.data[insert_offset_u + entry_size .. insert_offset_u + entry_size + tail_len],
                    self.page.data[insert_offset_u .. insert_offset_u + tail_len],
                );
            }

            self.page.data[insert_offset_u] = @intFromEnum(value.valueType());
            var stream = std.io.fixedBufferStream(self.page.data[insert_offset_u + 1 .. insert_offset_u + 1 + value_size]);
            try value.write(stream.writer());

            const loc_pos = insert_offset_u + 1 + value_size;
            const page_bytes_ptr = @as(*[8]u8, @ptrCast(self.page.data[loc_pos .. loc_pos + 8].ptr));
            mem.writeInt(u64, page_bytes_ptr, location.pageId, .little);
            const offset_bytes_ptr = @as(*[2]u8, @ptrCast(self.page.data[loc_pos + 8 .. loc_pos + 10].ptr));
            mem.writeInt(u16, offset_bytes_ptr, location.offset, .little);

            const new_end = insert_offset_u + entry_size + tail_len;
            self.page.header.used_size = @intCast(new_end);
            if (new_end < self.page.data.len) {
                self.page.data[new_end] = 0;
            }
        }

        fn leafRemoveAt(self: *Node, offset: u16, total_size: u16) void {
            const offset_u = @as(usize, offset);
            const total_u = @as(usize, total_size);
            const end = @as(usize, self.leafEndOffset());
            const tail_start = offset_u + total_u;
            const tail_len = end - tail_start;
            if (tail_len > 0) {
                std.mem.copyForwards(
                    u8,
                    self.page.data[offset_u .. offset_u + tail_len],
                    self.page.data[tail_start .. tail_start + tail_len],
                );
            }
            const new_end = end - total_u;
            @memset(self.page.data[new_end..end], 0);
            self.page.header.used_size = @intCast(new_end);
        }

        fn leafNeedsSplit(self: *const Node) bool {
            return self.leafEndOffset() > self.page.data.len - 20;
        }

        fn internalFindChild(self: *const Node, value: BSONValue) !u64 {
            var left_child = self.internalFirstChildId();
            var offset: u16 = internal_header_size;
            while (true) {
                const entry = self.decodeInternalEntry(offset) orelse return left_child;
                const right_child = entry.right_child;
                const cmp = entry.value().order(value);
                switch (cmp) {
                    .eq => return right_child,
                    .gt => return left_child,
                    .lt => {
                        left_child = right_child;
                        offset = entry.offset + entry.total_size;
                    },
                }
            }
        }

        fn internalFindInsertOffset(self: *const Node, key: BSONValue) u16 {
            var offset: u16 = internal_header_size;
            while (true) {
                const entry = self.decodeInternalEntry(offset) orelse return offset;
                const cmp = entry.value().order(key);
                if (cmp == .gt or cmp == .eq) return entry.offset;
                offset = entry.offset + entry.total_size;
            }
        }

        fn internalInsertAt(self: *Node, offset: u16, payload: *const SplitPayload) !void {
            const offset_u = @as(usize, offset);
            const key_bytes = payload.key_bytes;
            const key_len = key_bytes.len;
            const entry_size = 1 + key_len + 8;
            const end = @as(usize, self.internalEndOffset());
            if (end + entry_size > self.page.data.len) return error.NodeFull;

            const tail_len = end - offset_u;
            if (tail_len > 0) {
                std.mem.copyBackwards(
                    u8,
                    self.page.data[offset_u + entry_size .. offset_u + entry_size + tail_len],
                    self.page.data[offset_u .. offset_u + tail_len],
                );
            }

            self.page.data[offset_u] = @intFromEnum(payload.key_type);
            std.mem.copyForwards(u8, self.page.data[offset_u + 1 .. offset_u + 1 + key_len], key_bytes);
            const child_ptr = @as(*[8]u8, @ptrCast(self.page.data[offset_u + 1 + key_len .. offset_u + 1 + key_len + 8].ptr));
            mem.writeInt(u64, child_ptr, payload.right_page.header.page_id, .little);

            const new_end = offset_u + entry_size + tail_len;
            self.page.header.used_size = @intCast(new_end);
            if (new_end < self.page.data.len) {
                self.page.data[new_end] = 0;
            }
        }

        fn internalNeedsSplit(self: *const Node) bool {
            return self.internalEndOffset() > self.page.data.len - 20;
        }

        fn splitLeaf(self: *Node) !SplitPayload {
            const new_page = try self.index.bucket.createNewPage(.Index);
            new_page.data[0] = 1;

            var new_node = Node.init(self.index, new_page);

            const total = self.leafEntryCount();
            if (total == 0) return error.CorruptedNode;
            const mid = @divFloor(total, 2);
            const first_right = self.leafEntryAt(mid) orelse return error.CorruptedNode;

            const move_start = first_right.offset;
            const move_start_u = @as(usize, move_start);
            const end = @as(usize, self.leafEndOffset());
            const move_len = end - move_start_u;

            if (move_len > 0) {
                std.mem.copyForwards(
                    u8,
                    new_page.data[leaf_header_size .. leaf_header_size + move_len],
                    self.page.data[move_start_u .. move_start_u + move_len],
                );
            }

            const next_id = self.leafNextId();
            new_node.setLeafPrevId(self.id());
            new_node.setLeafNextId(next_id);
            self.setLeafNextId(new_page.header.page_id);

            if (next_id != 0) {
                const next_page = try self.index.bucket.loadPage(next_id);
                var next_node = Node.init(self.index, next_page);
                next_node.setLeafPrevId(new_page.header.page_id);
                try self.index.bucket.writePage(next_page);
            }

            self.page.header.used_size = move_start;
            @memset(self.page.data[move_start_u..end], 0);

            const new_used = @as(usize, leaf_header_size) + move_len;
            new_page.header.used_size = @intCast(new_used);
            if (new_used < new_page.data.len) {
                new_page.data[new_used] = 0;
            }

            try self.index.bucket.writePage(self.page);
            try self.index.bucket.writePage(new_page);

            const separator = new_node.leafFirstEntry() orelse return error.CorruptedNode;
            return SplitPayload{
                .key_type = separator.value_type,
                .key_bytes = separator.value_bytes,
                .owned_bytes = null,
                .right_page = new_page,
            };
        }

        fn splitInternal(self: *Node) !SplitPayload {
            const ally = self.index.allocator;
            const total = self.internalKeyCount();
            if (total == 0) return error.CorruptedNode;
            const mid = @divFloor(total, 2);
            const promoted_entry = self.internalEntryAt(mid) orelse return error.CorruptedNode;

            const key_len = promoted_entry.value_bytes.len;
            const buf = try ally.alloc(u8, key_len);
            std.mem.copyForwards(u8, buf, promoted_entry.value_bytes);

            const new_page = try self.index.bucket.createNewPage(.Index);
            new_page.data[0] = 0;
            const child_bytes = new_page.data[1..9];
            mem.writeInt(u64, @as(*[8]u8, @ptrCast(child_bytes.ptr)), promoted_entry.right_child, .little);

            const move_start = promoted_entry.offset + promoted_entry.total_size;
            const move_start_u = @as(usize, move_start);
            const end = @as(usize, self.internalEndOffset());
            const move_len = end - move_start_u;
            if (move_len > 0) {
                std.mem.copyForwards(
                    u8,
                    new_page.data[internal_header_size .. internal_header_size + move_len],
                    self.page.data[move_start_u .. move_start_u + move_len],
                );
            }

            const new_used = @as(usize, internal_header_size) + move_len;
            new_page.header.used_size = @intCast(new_used);
            if (new_used < new_page.data.len) {
                new_page.data[new_used] = 0;
            }

            const promote_offset_u = @as(usize, promoted_entry.offset);
            @memset(self.page.data[promote_offset_u..end], 0);
            self.page.header.used_size = promoted_entry.offset;

            try self.index.bucket.writePage(self.page);
            try self.index.bucket.writePage(new_page);

            return SplitPayload{
                .key_type = promoted_entry.value_type,
                .key_bytes = buf,
                .owned_bytes = buf,
                .right_page = new_page,
            };
        }
    };

    fn loadNode(self: *Index, page_id: u64) !Node {
        const page = try self.bucket.loadPage(page_id);
        return Node.init(self, page);
    }

    fn descendToLeaf(self: *Index, value: BSONValue) !Node {
        var node = try self.loadNode(self.root_page_id);
        while (!node.isLeaf()) {
            const child_id = try node.internalFindChild(value);
            node = try self.loadNode(child_id);
        }
        return node;
    }

    fn leftmostLeaf(self: *Index) !Node {
        var node = try self.loadNode(self.root_page_id);
        while (!node.isLeaf()) {
            const child_id = node.internalFirstChildId();
            node = try self.loadNode(child_id);
        }
        return node;
    }

    fn insertRecursive(self: *Index, node: *Node, value: BSONValue, loc: DocumentLocation) !?SplitPayload {
        if (node.isLeaf()) {
            const needed_space = Node.leafEntrySize(&value);
            const current_end = @as(usize, node.leafEndOffset());
            if (current_end + needed_space > node.page.data.len) {
                var payload = try node.splitLeaf();
                const separator = payload.value();
                if (separator.order(value) == .gt) {
                    try node.leafInsert(value, loc);
                    try self.bucket.writePage(node.page);
                } else {
                    var right_node = Node.init(self, payload.right_page);
                    try right_node.leafInsert(value, loc);
                    try self.bucket.writePage(right_node.page);
                }
                return payload;
            }

            try node.leafInsert(value, loc);
            if (node.leafNeedsSplit()) {
                return try node.splitLeaf();
            } else {
                try self.bucket.writePage(node.page);
                return null;
            }
        }

        const child_id = try node.internalFindChild(value);
        var child_node = try self.loadNode(child_id);
        if (try self.insertRecursive(&child_node, value, loc)) |payload| {
            defer payload.deinit(self.allocator);

            const insert_offset = node.internalFindInsertOffset(payload.value());
            try node.internalInsertAt(insert_offset, &payload);

            if (node.internalNeedsSplit()) {
                return try node.splitInternal();
            } else {
                try self.bucket.writePage(node.page);
            }
        }

        return null;
    }

    pub fn loadWithOptions(bucket: *Bucket, pageId: u64, options: IndexOptions) !*Index {
        const index = try Index.load(bucket, pageId);
        index.options = options;
        return index;
    }

    pub fn load(bucket: *Bucket, pageId: u64) !*Index {
        _ = try bucket.loadPage(pageId);
        const index = try bucket.allocator.create(Index);
        index.* = .{
            .bucket = bucket,
            .allocator = bucket.allocator,
            .options = .{},
            .root_page_id = pageId,
        };
        return index;
    }

    pub fn deinit(self: *Index) void {
        self.allocator.destroy(self);
    }

    pub fn create(bucket: *Bucket) !*Index {
        const page = try bucket.createNewPage(.Index);
        page.data[0] = 1;
        const prev_bytes = page.data[1..9];
        mem.writeInt(u64, @as(*[8]u8, @ptrCast(prev_bytes.ptr)), 0, .little);
        const next_bytes = page.data[9..17];
        mem.writeInt(u64, @as(*[8]u8, @ptrCast(next_bytes.ptr)), 0, .little);
        page.header.used_size = Node.leafDataStart();
        try bucket.writePage(page);

        const index = try bucket.allocator.create(Index);
        index.* = .{
            .bucket = bucket,
            .allocator = bucket.allocator,
            .options = .{},
            .root_page_id = page.header.page_id,
        };
        return index;
    }

    pub fn findExact(self: *const Index, value: BSONValue) !?DocumentLocation {
        var mutable = @constCast(self);
        var node = try mutable.descendToLeaf(value);
        if (node.leafFindFirstEqualOffset(value)) |offset| {
            if (node.decodeLeafEntry(offset)) |entry| {
                return entry.location;
            }
        }
        return null;
    }

    pub const RangeIterator = struct {
        index: *Index,
        current_node: Node,
        current_offset: u16,
        lower: ?RangeBound,
        upper: ?RangeBound,
        lowerSatisfied: bool,

        pub fn next(self: *RangeIterator) !?DocumentLocation {
            while (true) {
                const entry = self.current_node.decodeLeafEntry(self.current_offset) orelse {
                    const next_id = self.current_node.leafNextId();
                    if (next_id == 0) return null;
                    self.current_node = try self.index.loadNode(next_id);
                    self.current_offset = Node.leafDataStart();
                    continue;
                };

                if (self.upper) |upperBound| {
                    const cmp = entry.value().order(upperBound.value);
                    const within = switch (upperBound.filter) {
                        .lt => cmp == .lt,
                        .lte => cmp != .gt,
                        else => unreachable,
                    };
                    if (!within) return null;
                }

                if (!self.lowerSatisfied) {
                    if (self.lower) |lowerBound| {
                        const cmp = entry.value().order(lowerBound.value);
                        const meets = switch (lowerBound.filter) {
                            .gt => cmp == .gt,
                            .gte => cmp != .lt,
                            else => unreachable,
                        };
                        if (!meets) {
                            self.current_offset = entry.offset + entry.total_size;
                            continue;
                        }
                    }
                    self.lowerSatisfied = true;
                }

                self.current_offset = entry.offset + entry.total_size;
                return entry.location;
            }
        }
    };

    pub fn range(self: *Index, lower: ?RangeBound, upper: ?RangeBound) !RangeIterator {
        if (lower) |bound| switch (bound.filter) {
            .gt, .gte => {},
            else => return RangeBoundError.InvalidLowerBoundOperator,
        };

        if (upper) |bound| switch (bound.filter) {
            .lt, .lte => {},
            else => return RangeBoundError.InvalidUpperBoundOperator,
        };

        var start_node = blk: {
            if (lower) |bound| {
                var node = try self.descendToLeaf(bound.value);
                while (true) {
                    const prev_id = node.leafPrevId();
                    if (prev_id == 0) break;
                    var prev_node = try self.loadNode(prev_id);
                    const last_entry = prev_node.leafLastEntry() orelse break;
                    const order = last_entry.value().order(bound.value);
                    const should_move = switch (bound.filter) {
                        .gte => order != .lt,
                        .gt => order == .gt,
                        else => false,
                    };
                    if (!should_move) break;
                    node = prev_node;
                }
                break :blk node;
            } else {
                break :blk try self.leftmostLeaf();
            }
        };

        const start_offset: u16 = if (lower) |bound|
            start_node.leafLowerBoundOffset(bound.value, bound.filter == .gte)
        else
            Node.leafDataStart();

        return RangeIterator{
            .index = self,
            .current_node = start_node,
            .current_offset = start_offset,
            .lower = lower,
            .upper = upper,
            .lowerSatisfied = lower == null,
        };
    }

    pub fn point(self: *Index, value: BSONValue) !RangeIterator {
        const lower = RangeBound.gte(value);
        const upper = RangeBound.lte(value);
        return self.range(lower, upper);
    }

    pub fn insert(self: *Index, value: BSONValue, loc: DocumentLocation) !void {
        var root_node = try self.loadNode(self.root_page_id);
        if (try self.insertRecursive(&root_node, value, loc)) |payload| {
            defer payload.deinit(self.allocator);
            const new_root_page = try self.bucket.createNewPage(.Index);
            new_root_page.data[0] = 0;
            const child_bytes = new_root_page.data[1..9];
            mem.writeInt(u64, @as(*[8]u8, @ptrCast(child_bytes.ptr)), self.root_page_id, .little);
            var new_root = Node.init(self, new_root_page);
            const insert_offset = Node.internalDataStart();
            try new_root.internalInsertAt(insert_offset, &payload);
            try self.bucket.writePage(new_root_page);
            self.root_page_id = new_root_page.header.page_id;
        }
    }

    pub fn delete(self: *Index, value: BSONValue, loc: DocumentLocation) !void {
        var node = try self.descendToLeaf(value);
        const start_offset_opt = node.leafFindFirstEqualOffset(value) orelse return;
        var offset = start_offset_opt;
        while (true) {
            const entry = node.decodeLeafEntry(offset) orelse return;
            const cmp = entry.value().order(value);
            if (cmp != .eq) return;
            if (entry.location.equal(&loc)) {
                node.leafRemoveAt(entry.offset, entry.total_size);
                try self.bucket.writePage(node.page);
                return;
            }
            offset = entry.offset + entry.total_size;
        }
    }
};

const testing = std.testing;

test "Index inserts" {
    var bucket = try Bucket.openFile(testing.allocator, "bplus_test.bucket");
    defer bucket.deinit();
    defer std.fs.cwd().deleteFile("bplus_test.bucket") catch |err| {
        std.debug.print("Error deleting file: {}\n", .{err});
    };

    var prng = std.Random.DefaultPrng.init(blk: {
        var seed: u64 = undefined;
        try std.posix.getrandom(std.mem.asBytes(&seed));
        break :blk seed;
    });
    const rand = prng.random();

    var index = try Index.create(&bucket);
    defer index.deinit();
    for (0..10000) |_| {
        const value = BSONValue{ .int32 = .{ .value = rand.int(i32) } };
        const loc = Index.DocumentLocation{ .pageId = rand.int(u64), .offset = rand.int(u16) };
        try index.insert(value, loc);
    }
}

test "leaf split updates parent links" {
    var bucket = try Bucket.openFile(testing.allocator, "bplus_split.bucket");
    defer bucket.deinit();
    defer std.fs.cwd().deleteFile("bplus_split.bucket") catch |err| {
        std.debug.print("Error deleting file: {}\n", .{err});
    };

    var index = try Index.create(&bucket);
    defer index.deinit();

    for (0..600) |i| {
        const int_value: i32 = @intCast(i);
        const page_id: u64 = @intCast(i + 1);
        const page_offset: u16 = @intCast((i % 1024) + 1);
        const value = BSONValue{ .int32 = .{ .value = int_value } };
        const loc = Index.DocumentLocation{ .pageId = page_id, .offset = page_offset };
        try index.insert(value, loc);
    }

    var root_node = try index.loadNode(index.root_page_id);
    try testing.expect(!root_node.isLeaf());

    var walker = root_node;
    while (!walker.isLeaf()) {
        const child_id = walker.internalFirstChildId();
        walker = try index.loadNode(child_id);
    }

    var counted: usize = 0;
    try testing.expectEqual(@as(u64, 0), walker.leafPrevId());

    var current = walker;
    while (true) {
        counted += current.leafEntryCount();
        const next_id = current.leafNextId();
        if (next_id == 0) break;
        const prev_id = current.id();
        current = try index.loadNode(next_id);
        try testing.expectEqual(prev_id, current.leafPrevId());
    }

    try testing.expect(counted >= 600);
}

test "Index range" {
    var bucket = try Bucket.openFile(testing.allocator, "bplus_test1.bucket");
    defer bucket.deinit();
    defer std.fs.cwd().deleteFile("bplus_test1.bucket") catch |err| {
        std.debug.print("Error deleting file: {}\n", .{err});
    };

    var index = try Index.create(&bucket);
    defer index.deinit();
    try index.insert(BSONValue{ .int32 = .{ .value = 10 } }, .{ .pageId = 1, .offset = 10 });
    try index.insert(BSONValue{ .int32 = .{ .value = 11 } }, .{ .pageId = 1, .offset = 20 });
    try index.insert(BSONValue{ .int32 = .{ .value = 12 } }, .{ .pageId = 1, .offset = 30 });

    var iter = try index.range(null, null);
    while (try iter.next()) |_| {
        // std.debug.print("Found location: pageId={}, offset={}\n", .{ loc.pageId, loc.offset });
    }
}

test "range handles duplicates" {
    var bucket = try Bucket.openFile(testing.allocator, "bplus_range_dupes.bucket");
    defer bucket.deinit();
    defer std.fs.cwd().deleteFile("bplus_range_dupes.bucket") catch |err| {
        std.debug.print("Error deleting file: {}\n", .{err});
    };

    var index = try Index.create(&bucket);
    defer index.deinit();

    const key = BSONValue{ .int32 = .{ .value = 10 } };
    const duplicates = [_]u16{ 101, 102, 103 };
    for (duplicates, 0..) |offset, i| {
        const loc = Index.DocumentLocation{ .pageId = 1 + @as(u64, i), .offset = offset };
        try index.insert(key, loc);
    }

    try index.insert(BSONValue{ .int32 = .{ .value = 9 } }, .{ .pageId = 99, .offset = 990 });
    try index.insert(BSONValue{ .int32 = .{ .value = 11 } }, .{ .pageId = 111, .offset = 1110 });

    var iter = try index.point(key);
    var results = std.ArrayList(u16){};
    defer results.deinit(testing.allocator);

    while (try iter.next()) |loc| {
        try results.append(testing.allocator, loc.offset);
    }

    try testing.expectEqualSlices(u16, duplicates[0..], results.items);
}

test "range respects bounds" {
    var bucket = try Bucket.openFile(testing.allocator, "bplus_range_bounds.bucket");
    defer bucket.deinit();
    defer std.fs.cwd().deleteFile("bplus_range_bounds.bucket") catch |err| {
        std.debug.print("Error deleting file: {}\n", .{err});
    };

    var index = try Index.create(&bucket);
    defer index.deinit();

    const entries = [_]struct { value: i32, page: u64, offset: u16 }{
        .{ .value = 5, .page = 1, .offset = 50 },
        .{ .value = 10, .page = 2, .offset = 100 },
        .{ .value = 10, .page = 3, .offset = 101 },
        .{ .value = 15, .page = 4, .offset = 150 },
        .{ .value = 20, .page = 5, .offset = 200 },
        .{ .value = 25, .page = 6, .offset = 250 },
    };

    for (entries) |entry| {
        const value = BSONValue{ .int32 = .{ .value = entry.value } };
        const loc = Index.DocumentLocation{ .pageId = entry.page, .offset = entry.offset };
        try index.insert(value, loc);
    }

    const lower = BSONValue{ .int32 = .{ .value = 10 } };
    const upper = BSONValue{ .int32 = .{ .value = 20 } };
    var iter = try index.range(Index.RangeBound.gte(lower), Index.RangeBound.lte(upper));
    var offsets = std.ArrayList(u16){};
    defer offsets.deinit(testing.allocator);

    while (try iter.next()) |loc| {
        try offsets.append(testing.allocator, loc.offset);
    }

    const expected = [_]u16{ 100, 101, 150, 200 };
    try testing.expectEqualSlices(u16, expected[0..], offsets.items);
}

test "range supports exclusive bounds" {
    var bucket = try Bucket.openFile(testing.allocator, "bplus_range_exclusive.bucket");
    defer bucket.deinit();
    defer std.fs.cwd().deleteFile("bplus_range_exclusive.bucket") catch |err| {
        std.debug.print("Error deleting file: {}\n", .{err});
    };

    var index = try Index.create(&bucket);
    defer index.deinit();

    const entries = [_]struct { value: i32, page: u64, offset: u16 }{
        .{ .value = 5, .page = 1, .offset = 50 },
        .{ .value = 10, .page = 2, .offset = 100 },
        .{ .value = 10, .page = 3, .offset = 101 },
        .{ .value = 15, .page = 4, .offset = 150 },
        .{ .value = 20, .page = 5, .offset = 200 },
        .{ .value = 25, .page = 6, .offset = 250 },
    };

    for (entries) |entry| {
        const value = BSONValue{ .int32 = .{ .value = entry.value } };
        const loc = Index.DocumentLocation{ .pageId = entry.page, .offset = entry.offset };
        try index.insert(value, loc);
    }

    const lower = BSONValue{ .int32 = .{ .value = 10 } };
    const upper = BSONValue{ .int32 = .{ .value = 20 } };
    var iter = try index.range(.gt(lower), .lt(upper));
    var offsets = std.ArrayList(u16){};
    defer offsets.deinit(testing.allocator);

    while (try iter.next()) |loc| {
        try offsets.append(testing.allocator, loc.offset);
    }

    const expected = [_]u16{150};
    try testing.expectEqualSlices(u16, expected[0..], offsets.items);
}

test "Index orders strings lexicographically" {
    var bucket = try Bucket.openFile(testing.allocator, "bplus_strings.bucket");
    defer bucket.deinit();
    defer std.fs.cwd().deleteFile("bplus_strings.bucket") catch |err| {
        std.debug.print("Error deleting file: {}\n", .{err});
    };

    var index = try Index.create(&bucket);
    defer index.deinit();

    const entries = [_]struct { value: []const u8, offset: u16 }{
        .{ .value = "delta", .offset = 3 },
        .{ .value = "alpha", .offset = 0 },
        .{ .value = "charlie", .offset = 2 },
        .{ .value = "bravo", .offset = 1 },
        .{ .value = "echo", .offset = 4 },
    };

    const insert_order = [_]usize{ 0, 2, 4, 1, 3 };
    for (insert_order, 0..) |idx, i| {
        const entry = entries[idx];
        const value = BSONValue{ .string = .{ .value = entry.value } };
        const loc = Index.DocumentLocation{ .pageId = @intCast(i + 1), .offset = entry.offset };
        try index.insert(value, loc);
    }

    var iter = try index.range(null, null);
    var offsets = std.ArrayList(u16){};
    defer offsets.deinit(testing.allocator);

    while (try iter.next()) |loc| {
        try offsets.append(testing.allocator, loc.offset);
    }

    const expected = [_]u16{ 0, 1, 2, 3, 4 };
    try testing.expectEqualSlices(u16, expected[0..], offsets.items);
}

test "Index orders ObjectIds lexicographically" {
    var bucket = try Bucket.openFile(testing.allocator, "bplus_objectids.bucket");
    defer bucket.deinit();
    defer std.fs.cwd().deleteFile("bplus_objectids.bucket") catch |err| {
        std.debug.print("Error deleting file: {}\n", .{err});
    };

    var index = try Index.create(&bucket);
    defer index.deinit();

    const hex_values = [_][]const u8{
        "507c7f79bcf86cd7994f6c0e",
        "507c7f79bcf86cd7994f6c0f",
        "507c7f79bcf86cd7994f6c0a",
        "507c7f79bcf86cd7994f6c0d",
        "507c7f79bcf86cd7994f6c0b",
    };

    const expected_offsets = [_]u16{ 3, 4, 0, 2, 1 };
    const insert_order = [_]usize{ 0, 4, 2, 1, 3 };

    for (insert_order, 0..) |idx, i| {
        const object_id = try bson.ObjectId.parseString(hex_values[idx]);
        const value = BSONValue{ .objectId = .{ .value = object_id } };
        const loc = Index.DocumentLocation{ .pageId = @intCast(i + 1), .offset = expected_offsets[idx] };
        try index.insert(value, loc);
    }

    var iter = try index.range(null, null);
    var offsets = std.ArrayList(u16){};
    defer offsets.deinit(testing.allocator);

    while (try iter.next()) |loc| {
        try offsets.append(testing.allocator, loc.offset);
    }

    const expected = [_]u16{ 0, 1, 2, 3, 4 };
    try testing.expectEqualSlices(u16, expected[0..], offsets.items);
}

test "Index delete" {
    var bucket = try Bucket.openFile(testing.allocator, "bplus_test2.bucket");
    defer bucket.deinit();
    defer std.fs.cwd().deleteFile("bplus_test2.bucket") catch |err| {
        std.debug.print("Error deleting file: {}\n", .{err});
    };

    var index = try Index.create(&bucket);
    defer index.deinit();
    try index.insert(BSONValue{ .int32 = .{ .value = 10 } }, .{ .pageId = 1, .offset = 10 });
    try index.insert(BSONValue{ .int32 = .{ .value = 11 } }, .{ .pageId = 1, .offset = 20 });
    try index.insert(BSONValue{ .int32 = .{ .value = 15 } }, .{ .pageId = 1, .offset = 30 });

    try index.delete(BSONValue{ .int32 = .{ .value = 11 } }, .{ .pageId = 1, .offset = 20 });
    var iter = try index.range(null, Index.RangeBound.lt(BSONValue{ .int32 = .{ .value = 12 } }));
    while (try iter.next()) |loc| {
        std.debug.print("Found no deleted location: pageId={}, offset={}\n", .{ loc.pageId, loc.offset });
    }
}

test "Index load from root and query" {
    var bucket = try Bucket.openFile(testing.allocator, "bplus_load.bucket");
    defer bucket.deinit();
    defer std.fs.cwd().deleteFile("bplus_load.bucket") catch |err| {
        std.debug.print("Error deleting file: {}\n", .{err});
    };

    // Build an index with enough entries to force internal nodes, then reload it by root page id
    var index = try Index.create(&bucket);
    const base_page: u64 = 10_000;
    const total_entries: usize = 800; // large enough to trigger leaf splits and internal nodes
    for (0..total_entries) |i| {
        const ival: i32 = @intCast(i);
        const value = BSONValue{ .int32 = .{ .value = ival } };
        const loc = Index.DocumentLocation{ .pageId = base_page + @as(u64, @intCast(i)), .offset = @intCast(i) };
        try index.insert(value, loc);
    }

    const root_id = index.root_page_id;
    index.deinit();

    // Load a fresh index instance from the root page id
    var loaded = try Index.load(&bucket, root_id);
    defer loaded.deinit();

    // Spot-check a few exact lookups
    const checks = [_]i32{ 0, 1, 2, 123, 400, 799 };
    inline for (checks) |c| {
        const v = BSONValue{ .int32 = .{ .value = c } };
        const loc_opt = try loaded.findExact(v);
        try testing.expect(loc_opt != null);
        const loc = loc_opt.?;
        try testing.expectEqual(base_page + @as(u64, @intCast(c)), loc.pageId);
        try testing.expectEqual(@as(u16, @intCast(c)), loc.offset);
    }

    // Range [100, 199] inclusive should yield exactly 100 entries in order
    const lower = BSONValue{ .int32 = .{ .value = 100 } };
    const upper = BSONValue{ .int32 = .{ .value = 199 } };
    var iter = try loaded.range(Index.RangeBound.gte(lower), Index.RangeBound.lte(upper));
    var count: usize = 0;
    var expected_offset: u16 = 100;
    while (try iter.next()) |loc| {
        try testing.expect(loc.offset == expected_offset);
        expected_offset += 1;
        count += 1;
    }
    try testing.expectEqual(@as(usize, 100), count);
}
