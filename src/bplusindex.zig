const std = @import("std");
const mem = std.mem;
const albedo = @import("albedo.zig");
const bson = @import("bson.zig");
const BSONValue = bson.BSONValue;
const Bucket = albedo.Bucket;
const Page = albedo.Page;

const Index = struct {
    bucket: *Bucket,
    root: *Node,
    allocator: mem.Allocator,

    const DocumentLocation = struct {
        pageId: u64,
        offset: u16,
        inline fn equal(self: *const DocumentLocation, other: *const DocumentLocation) bool {
            return self.pageId == other.pageId and self.offset == other.offset;
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
        isLeaf: bool,
        index: *Index,
        keys: std.ArrayList(BSONValue),
        children: std.ArrayList(*Node),
        locationPairs: std.ArrayList(LocationPair),
        prev: ?*Node = null,
        next: ?*Node = null,
        parent: ?*Node = null,
        id: u64,

        fn leafSize(self: *const Node) usize {
            var size: u16 = 17;
            for (self.locationPairs.items) |pair| {
                size += 1 + @as(u16, @truncate(pair.bsonValue.size())) + 8 + 2;
            }
            return size;
        }

        fn internalSize(self: *const Node) usize {
            var size: u16 = 1 + 8;
            for (self.keys.items) |key| {
                size += 1 + @as(u16, @truncate(key.size())) + 8;
            }
            return size;
        }

        const ChildPair = struct {
            bsonValue: BSONValue,
            childNodeId: *Node,
        };
        const LocationPair = struct {
            bsonValue: BSONValue,
            location: DocumentLocation,
        };

        pub fn init(index: *Index, page: *Page) !*Node {
            const isLeaf = page.data[0] == 1;
            const node = try index.allocator.create(Node);
            node.* = Node{
                .page = page,
                .isLeaf = isLeaf,
                .index = index,
                .children = .{},
                .keys = .{},
                .locationPairs = .{},
                .parent = null,
                .id = page.header.page_id,
            };
            return node;
        }

        pub fn nodeFromPage(index: *Index, page: *Page, lastLeaf: *?Node, parent: ?*Node) !*Node {
            const node = try Node.init(index, page);
            node.parent = parent;
            const ally = index.allocator;
            if (node.isLeaf) {
                var offset: u16 = 17;
                const data = page.data;
                while (offset < data.len) {
                    const typeId: bson.BSONValueType = @enumFromInt(data[offset]);
                    if (data[offset] == 0) break;
                    offset += 1;
                    const bsonValue = BSONValue.read(data[offset..], typeId);
                    offset += @truncate(bsonValue.size());
                    const numData = data[offset .. offset + 10];
                    const pageId = mem.readInt(u64, numData[0..8], .little);
                    offset += 8;
                    const pageOffset = mem.readInt(u16, data[8..10], .little);
                    offset += 2;
                    try node.locationPairs.append(ally, LocationPair{
                        .bsonValue = bsonValue,
                        .location = DocumentLocation{
                            .pageId = pageId,
                            .offset = pageOffset,
                        },
                    });
                }
                if (lastLeaf.*) |*lastLeafNode| {
                    lastLeafNode.*.next = node;
                    node.prev = lastLeafNode;
                }
                lastLeaf.* = node.*;
            } else {
                var offset: u16 = 1;
                const data = page.data;
                const firstChildId = mem.readInt(u64, data[1..9], .little);
                const firstChildPage = try index.bucket.loadPage(firstChildId);
                const firstChildNode = try Node.nodeFromPage(index, firstChildPage, lastLeaf, node);
                try node.children.append(ally, firstChildNode);
                while (offset < data.len) {
                    if (data[offset] == 0) break;
                    const typeId: bson.BSONValueType = @enumFromInt(data[offset]);
                    offset += 1;
                    const bsonValue = BSONValue.read(data[offset..], typeId);
                    try node.keys.append(ally, bsonValue);
                    offset += @truncate(bsonValue.size());
                    const nodeId = mem.readInt(u64, data[offset..][0..8], .little);
                    offset += 8;

                    const childPage = try index.bucket.loadPage(nodeId);
                    const childNode = try Node.nodeFromPage(index, childPage, lastLeaf, node);
                    try node.children.append(ally, childNode);
                }
            }
            return node;
        }

        pub fn persist(self: *const Node) !void {
            // Write the node to the page
            const page = self.page;
            var stream = std.io.fixedBufferStream(page.data);
            var writer = stream.writer();
            if (self.isLeaf) {
                try writer.writeInt(u8, 1, .little);
                const prevId = if (self.prev) |prevNode| prevNode.id else 0;
                try writer.writeInt(u64, prevId, .little);
                const nextId = if (self.next) |nextNode| nextNode.id else 0;
                try writer.writeInt(u64, nextId, .little);
                for (self.locationPairs.items) |pair| {
                    try writer.writeInt(u8, @intFromEnum(pair.bsonValue), .little);

                    try pair.bsonValue.write(writer);
                    try writer.writeInt(u64, pair.location.pageId, .little);
                    try writer.writeInt(u16, pair.location.offset, .little);
                }
            } else {
                try writer.writeInt(u8, 0, .little);
                if (self.children.items.len != 0) {
                    try writer.writeInt(u64, self.children.items[0].id, .little);
                    for (self.keys.items, 0..) |key, i| {
                        try writer.writeInt(u8, @intFromEnum(key), .little);
                        try key.write(writer);
                        try writer.writeInt(u64, self.children.items[i + 1].id, .little);
                    }
                }
            }

            try self.index.bucket.writePage(page);
        }

        pub fn deinit(self: *Node) void {
            const ally = self.index.allocator;
            self.keys.deinit(ally);
            defer self.children.deinit(ally);
            self.locationPairs.deinit(ally);

            for (self.children.items) |node| {
                node.deinit();
                ally.destroy(node);
            }
        }

        fn traverseChildren(self: *const Node, value: BSONValue) *Node {
            var lastChild: *Node = self.children.items[0];
            for (self.keys.items, 0..) |pair, i| {
                if (pair.order(value) == .eq) {
                    return self.children.items[i + 1];
                } else if (pair.order(value) == .gt) {
                    return lastChild;
                }
                lastChild = self.children.items[i + 1];
            }
            return lastChild;
        }

        fn findMatchIndex(self: *const Node, value: BSONValue) ?usize {
            // Avoid shadowing self.index fields by not using 'pair' or 'i' as variable names
            var i: usize = 0;
            while (i < self.locationPairs.items.len) : (i += 1) {
                const loc_pair = self.locationPairs.items[i];
                switch (loc_pair.bsonValue.order(value)) {
                    .lt => continue,
                    .gt => break,
                    .eq => return i,
                }
            }
            return null;
        }

        fn findMatch(self: *const Node, value: BSONValue) ?DocumentLocation {
            if (findMatchIndex(self, value)) |index| {
                return self.locationPairs.items[index].location;
            }
            return null;
        }
    };

    fn load(ally: mem.Allocator, bucket: *Bucket, pageId: u64) !Index {
        // Load the index page from the bucket
        var nodeMap = std.AutoHashMap(u64, *Node).init(ally);
        defer nodeMap.deinit();

        const page = try bucket.loadPage(pageId);
        var index = Index{
            .bucket = bucket,
            .root = undefined,
            .allocator = ally,
        };
        var lastLeafNode: ?Node = null;
        const node = try Node.nodeFromPage(&index, page, &lastLeafNode, null);
        node.parent = null;
        index.root = node;
        return index;
    }

    fn deinit(self: *Index) void {
        // Deinitialize the index
        const ally = self.allocator;
        self.root.deinit();
        ally.destroy(self.root);
        ally.destroy(self);
    }

    pub fn create(ally: mem.Allocator, bucket: *Bucket) !*Index {
        // Create a new index
        const page = try bucket.createNewPage(.Index);
        page.data[0] = 1; // Leaf node
        try bucket.writePage(page);
        const index = try ally.create(Index);
        index.* = Index{
            .bucket = bucket,
            .root = undefined,
            .allocator = ally,
        };
        const node = try Node.init(index, page);
        try node.persist();
        index.root = node;
        return index;
    }

    pub fn findExact(self: *const Index, value: BSONValue) !?DocumentLocation {
        // Find the document location in the index
        var current = self.root;

        while (current.isLeaf != true) {
            current = current.traverseChildren(value);
        }

        return current.findMatch(value);
    }

    const RangeIterator = struct {
        index: *Index,
        current: *Node,
        currentIndex: usize,
        until: ?BSONValue,

        pub fn next(self: *RangeIterator) !?DocumentLocation {
            // Get the next document location in the index
            if (self.currentIndex >= self.current.locationPairs.items.len) {
                if (self.current.next) |nextNode| {
                    self.current = nextNode;
                    self.currentIndex = 0;
                } else {
                    return null;
                }
            }
            const pair = self.current.locationPairs.items[self.currentIndex];
            if (self.until) |untilValue| {
                if (pair.bsonValue.order(untilValue) == .gt) {
                    return null;
                }
            }
            const loc = self.current.locationPairs.items[self.currentIndex].location;
            self.currentIndex += 1;
            return loc;
        }
    };

    pub fn range(self: *Index, gt: ?BSONValue, lt: ?BSONValue) !RangeIterator {
        // Create a range iterator for the index
        var iter = RangeIterator{
            .index = self,
            .current = self.root,
            .currentIndex = 0,
            .until = lt,
        };
        if (gt) |gtValue| {
            // Descend to the correct leaf for the lower bound
            while (!iter.current.isLeaf) {
                iter.current = iter.current.traverseChildren(gtValue);
            }
            iter.currentIndex = blk: {
                for (iter.current.locationPairs.items, 0..) |pair, i| {
                    if (pair.bsonValue.order(gtValue) != .lt) {
                        break :blk i;
                    }
                }
                break :blk iter.current.locationPairs.items.len;
            };
        } else {
            // Find the left-most leaf node
            while (!iter.current.isLeaf) {
                iter.current = iter.current.children.items[0];
            }
        }

        return iter;
    }

    pub fn insert(self: *Index, value: BSONValue, loc: DocumentLocation) !void {
        try insertInternal(self, self.root, value, loc);
    }

    fn insertInternal(self: *Index, node: *Node, value: BSONValue, loc: DocumentLocation) !void {
        const ally = self.allocator;
        if (node.isLeaf) {
            var insertAt: usize = 0;
            while (insertAt < node.locationPairs.items.len) : (insertAt += 1) {
                const order = node.locationPairs.items[insertAt].bsonValue.order(value);
                if (order == .gt) break;
            }

            try node.locationPairs.insert(ally, insertAt, Node.LocationPair{
                .bsonValue = value,
                .location = loc,
            });

            if (node.leafSize() > node.page.data.len - 20) {
                try self.splitLeafNode(node);
            } else {
                try node.persist();
            }
            return;
        }

        var childIndex: usize = 0;
        while (childIndex < node.keys.items.len) : (childIndex += 1) {
            const order = node.keys.items[childIndex].order(value);
            if (order == .gt or order == .eq) break;
        }

        const child = node.children.items[childIndex];
        try insertInternal(self, child, value, loc);
    }

    pub fn delete(self: *Index, value: BSONValue, loc: DocumentLocation) !void {
        // Delete a document location from the index
        var current: *Node = self.root;
        while (current.isLeaf != true) {
            current = current.traverseChildren(value);
        }
        var index = current.findMatchIndex(value) orelse return;
        while (true) {
            if (index >= current.locationPairs.items.len) {
                current = current.next orelse return;
                index = 0;
                continue;
            }
            const locPair = current.locationPairs.items[index];
            if (locPair.location.equal(&loc)) {
                _ = current.locationPairs.orderedRemove(index);
                break;
            }
            if (locPair.bsonValue.order(value) != .eq) {
                break;
            }
            index += 1;
        }
        try current.persist();
    }

    fn splitInternalNode(self: *Index, node: *Node) anyerror!void {
        const page = try self.bucket.createNewPage(.Index);
        page.data[0] = 0;
        const newNode = try Node.init(self, page);
        newNode.isLeaf = false;
        newNode.parent = node.parent;

        const ally = self.allocator;
        const totalKeys = node.keys.items.len;
        const mid = @divFloor(totalKeys, 2);
        const promoteKey = node.keys.items[mid];

        var i = mid + 1;
        while (i < totalKeys) : (i += 1) {
            try newNode.keys.append(ally, node.keys.items[i]);
        }
        node.keys.shrinkRetainingCapacity(mid);

        const totalChildren = node.children.items.len;
        var j: usize = mid + 1;
        while (j < totalChildren) : (j += 1) {
            const child = node.children.items[j];
            try newNode.children.append(ally, child);
            child.parent = newNode;
        }
        node.children.shrinkRetainingCapacity(mid + 1);

        try node.persist();
        try newNode.persist();

        try self.insertIntoParent(node, promoteKey, newNode);
    }

    fn splitLeafNode(self: *Index, node: *Node) anyerror!void {
        const page = try self.bucket.createNewPage(.Index);
        page.data[0] = 1;
        const newNode = try Node.init(self, page);
        newNode.isLeaf = true;
        newNode.parent = node.parent;

        const ally = self.allocator;
        const total = node.locationPairs.items.len;
        const mid = @divFloor(total, 2);
        var i = mid;
        while (i < total) : (i += 1) {
            try newNode.locationPairs.append(ally, node.locationPairs.items[i]);
        }
        node.locationPairs.shrinkRetainingCapacity(mid);

        newNode.next = node.next;
        if (node.next) |next| {
            next.prev = newNode;
            try next.persist();
        }
        node.next = newNode;
        newNode.prev = node;

        try node.persist();
        try newNode.persist();

        const separator = newNode.locationPairs.items[0].bsonValue;
        try self.insertIntoParent(node, separator, newNode);
    }

    fn insertIntoParent(self: *Index, left: *Node, key: BSONValue, right: *Node) anyerror!void {
        const ally = self.allocator;
        if (left.parent) |parent| {
            var insertIndex: usize = 0;
            while (insertIndex < parent.keys.items.len) : (insertIndex += 1) {
                const ordering = parent.keys.items[insertIndex].order(key);
                if (ordering == .gt or ordering == .eq) break;
            }

            try parent.keys.insert(ally, insertIndex, key);
            try parent.children.insert(ally, insertIndex + 1, right);
            right.parent = parent;

            try parent.persist();

            if (parent.internalSize() > parent.page.data.len - 20) {
                try self.splitInternalNode(parent);
            }
        } else {
            const page = try self.bucket.createNewPage(.Index);
            page.data[0] = 0;
            const newRoot = try Node.init(self, page);
            newRoot.isLeaf = false;
            newRoot.parent = null;

            try newRoot.children.append(ally, left);
            try newRoot.children.append(ally, right);
            left.parent = newRoot;
            right.parent = newRoot;
            try newRoot.keys.append(ally, key);

            try newRoot.persist();
            self.root = newRoot;
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

    var index = try Index.create(testing.allocator, &bucket);
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

    var index = try Index.create(testing.allocator, &bucket);
    defer index.deinit();

    for (0..600) |i| {
        const int_value: i32 = @intCast(i);
        const page_id: u64 = @intCast(i + 1);
        const page_offset: u16 = @intCast((i % 1024) + 1);
        const value = BSONValue{ .int32 = .{ .value = int_value } };
        const loc = Index.DocumentLocation{ .pageId = page_id, .offset = page_offset };
        try index.insert(value, loc);
    }

    try testing.expect(!index.root.isLeaf);
    try testing.expect(index.root.parent == null);
    try testing.expect(index.root.children.items.len >= 2);

    for (index.root.children.items) |child| {
        try testing.expect(child.parent == index.root);
    }

    var walker = index.root;
    while (!walker.isLeaf) {
        walker = walker.children.items[0];
    }

    var current: ?*Index.Node = walker;
    var counted: usize = 0;
    while (current) |node| {
        try testing.expect(node.parent != null);
        counted += node.locationPairs.items.len;
        current = node.next;
    }

    try testing.expect(counted >= 600);
}

test "Index range" {
    var bucket = try Bucket.openFile(testing.allocator, "bplus_test1.bucket");
    defer bucket.deinit();
    defer std.fs.cwd().deleteFile("bplus_test1.bucket") catch |err| {
        std.debug.print("Error deleting file: {}\n", .{err});
    };

    var index = try Index.create(testing.allocator, &bucket);
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

    var index = try Index.create(testing.allocator, &bucket);
    defer index.deinit();

    const key = BSONValue{ .int32 = .{ .value = 10 } };
    const duplicates = [_]u16{ 101, 102, 103 };
    for (duplicates, 0..) |offset, i| {
        const loc = Index.DocumentLocation{ .pageId = 1 + @as(u64, i), .offset = offset };
        try index.insert(key, loc);
    }

    try index.insert(BSONValue{ .int32 = .{ .value = 9 } }, .{ .pageId = 99, .offset = 990 });
    try index.insert(BSONValue{ .int32 = .{ .value = 11 } }, .{ .pageId = 111, .offset = 1110 });

    var iter = try index.range(key, key);
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

    var index = try Index.create(testing.allocator, &bucket);
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
    var iter = try index.range(lower, upper);
    var offsets = std.ArrayList(u16){};
    defer offsets.deinit(testing.allocator);

    while (try iter.next()) |loc| {
        try offsets.append(testing.allocator, loc.offset);
    }

    const expected = [_]u16{ 100, 101, 150, 200 };
    try testing.expectEqualSlices(u16, expected[0..], offsets.items);
}

test "Index delete" {
    var bucket = try Bucket.openFile(testing.allocator, "bplus_test2.bucket");
    defer bucket.deinit();
    defer std.fs.cwd().deleteFile("bplus_test2.bucket") catch |err| {
        std.debug.print("Error deleting file: {}\n", .{err});
    };

    var index = try Index.create(testing.allocator, &bucket);
    defer index.deinit();
    try index.insert(BSONValue{ .int32 = .{ .value = 10 } }, .{ .pageId = 1, .offset = 10 });
    try index.insert(BSONValue{ .int32 = .{ .value = 11 } }, .{ .pageId = 1, .offset = 20 });
    try index.insert(BSONValue{ .int32 = .{ .value = 15 } }, .{ .pageId = 1, .offset = 30 });

    try index.delete(BSONValue{ .int32 = .{ .value = 11 } }, .{ .pageId = 1, .offset = 20 });
    var iter = try index.range(null, .{ .int32 = .{ .value = 12 } });
    while (try iter.next()) |loc| {
        std.debug.print("Found no deleted location: pageId={}, offset={}\n", .{ loc.pageId, loc.offset });
    }
}
