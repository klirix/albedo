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
    nodeMap: std.AutoHashMap(u64, Node),
    
    const MAX_VALUE_SIZE = 512;

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
        //       [n]u8 = BSONValue size n
        //       u64 = documentId
        //       u16 = document offset
        //     u8 = 0  as end of the list
        //   Internal node: min theoretical cap = 30, max theoretical cap = 906
        //     u8 = 0
        //     Repeatedly:
        //       u8 = BSONValueType
        //       [n]u8 = BSONValue size n
        //       u64 = child nodeId
        //.    u8 = 0 as end of the list

        parent: ?*Node,
        page: *Page,
        isLeaf: bool,
        index: *Index,
        size: u16,
        prev: ?*Node = null,
        next: ?*Node = null,
        id: u64,

        fn firstValue(self: *Node) ?BSONValue {
            const offset: u16 = if (self.isLeaf) 1+8+8 else 1;
            const valueType = self.page.data[offset];
            if(valueType != 0) {
                return BSONValue.read(self.page.data[offset+1..], valueType);
            }
            return value;
        }

        fn addChild(self: *Node, value: BSONValue, childId: u64) void {
            const size = value.size();
            const insertionSize: u32 = size + @sizeOf(childId);

            // find the offset where item gt starts
            var data = self.page.data;
            var offset: u16 = 1;

            while (data[offset] != 0) {
                const current = BSONValue.read(data[offset + 1 ..], @enumFromInt((data[offset])));
                if (current.order(value) == .gt) break;
                offset += current.size() + @sizeOf(u64);
            }

            // move all memory ahead for the size of value+childId
            mem.copyBackwards(u8, data[offset + insertionSize .. self.size + insertionSize], data[offset..self.size]);

            // insert value+child
            value.appendWrite(data[offset .. offset + size]);

            offset += size;
            mem.writeInt(u64, data[offset..@sizeOf(u64)], childId, .little);
        }

        fn mitoseInto(self: *Node, other: *Node) void {
            var offset: u16 = if (self.isLeaf) 1+8+8 else 1;
            const minSplitOffset: comptime_int = @divFloor(albedo.DEFAULT_PAGE_SIZE,2) - @divFloor(MAX_VALUE_SIZE, 2);
            while (offset < minSplitOffset) {
                const currentSize: u16 = @truncate(BSONValue.asessSize(data[offset + 1 ..], @enumFromInt((data[offset]))));
                offset += currentSize + if (self.isLeaf) @sizeOf(u64) + @sizeOf(u16) else @sizeOf(u64);
            }

        }

        fn addLocation(self: *Node, value: BSONValue, loc: DocumentLocation) void {
            const size = value.size();
            const insertionSize: u32 = size + @sizeOf(u64) + @sizeOf(u16);

            // find the offset where item gt starts
            var data = self.page.data;
            var offset: u16 = 1 + 8 + 8;

            while (data[offset] != 0) {
                const current = BSONValue.read(data[offset + 1 ..], @enumFromInt((data[offset])));
                if (current.order(value) == .gt) break;
                offset += current.size() + @sizeOf(u64) + @sizeOf(u16);
            }

            // move all memory ahead for the size of value+childId
            mem.copyBackwards(u8, data[offset + insertionSize .. self.size + insertionSize], data[offset..self.size]);

            // insert value+child
            value.appendWrite(data[offset .. offset + size]);
            offset += size;

            mem.writeInt(u64, data[offset .. offset + @sizeOf(u64)], loc.pageId, .little);
            offset += @sizeOf(u64);

            mem.writeInt(u64, data[offset .. offset + @sizeOf(u16)], loc.pageId, .little);
        }

        fn canFit(self: *const Node, value: *BSONValue) bool {
            var newSize: usize = 0;
            if (self.isLeaf) {
                newSize = self.size + 1 + value.size() + 8 + 2;
            } else {
                newSize = self.size + 1 + value.size() + 8;
            }
            return newSize < 8162;
        }

        const ChildPair = struct {
            bsonValue: BSONValue,
            childNodeId: *Node,
        };
        const LocationPair = struct {
            bsonValue: BSONValue,
            location: DocumentLocation,
        };

        pub fn init(ally: mem.Allocator, index: *Index, page: *Page, isLeaf: bool) !*Node {
            const node = try ally.create(Node);
            node.* = Node{
                .page = page,
                .isLeaf = isLeaf,
                .index = index,
                .id = page.header.page_id,
            };
            return node;
        }

        pub fn persist(self: *const Node) !void {
            // Write the node to the page
            const page = self.page;

            try self.index.bucket.writePage(page);
        }

        fn traverseChildren(self: *const Node, value: BSONValue) !*Node {
            var offset: u16 = 1;
            const page = try self.index.bucket.loadPage(self.id);
            const data = page.data;
            const firstValue = BSONValue.read(data[offset + 1 ..], @enumFromInt(data[offset]));
            offset += 1 + firstValue.size();
            const firstChildId = mem.readInt(u64, data[offset .. offset + 8], .little);
            var current: u64 = firstChildId;
            offset += 8;
            while (data[offset] != 0) {
                const childValue = BSONValue.read(data[offset + 1 ..], @enumFromInt(data[offset]));
                if (childValue.order(value) == .gt) break;
                offset += childValue.size();
                current = mem.readInt(u64, data[offset .. offset + 8], .little);
            }
            return try self.index.loadNode(current, self);
        }

        fn findMatchOffset(self: *const Node, value: BSONValue) ?u16 {
            const page = try self.index.bucket.loadPage(self.id);
            var data = page.data;
            var offset: u16 = 1 + 8 + 8;

            while (data[offset] != 0) {
                const current = BSONValue.read(data[offset + 1 ..], @enumFromInt((data[offset])));
                if (current.order(value) == .eq) return offset;
                offset += current.size() + @sizeOf(u64) + @sizeOf(u16);
            }

            return null;
        }

        fn findMatch(self: *const Node, value: BSONValue) ?DocumentLocation {
            const page = try self.index.bucket.loadPage(self.id);
            var data = page.data;
            if (findMatchOffset(self, value)) |offset| {
                offset += BSONValue.asessSize(data[offset + 1 ..], data[offset]);

                return DocumentLocation{
                    .pageId = mem.readInt(u64, data[offset .. offset + @sizeOf(u64)], .little),
                    .offset = mem.readInt(u16, data[offset + @sizeOf(u64) .. offset + @sizeOf(u64) + @sizeOf(u16)], .little),
                };
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
        const node = try Node.nodeFromPage(ally, &index, page, &lastLeafNode);
        index.root = node;
        return index;
    }

    fn deinit(self: *Index, ally: mem.Allocator) void {
        // Deinitialize the index
        self.root.deinit(ally);
        ally.destroy(self.root);
        ally.destroy(self);
    }

    pub fn create(ally: mem.Allocator, bucket: *Bucket) !*Index {
        // Create a new index
        const index = try ally.create(Index);
        index.* = Index{
            .bucket = bucket,
            .root = undefined,
            .allocator = ally,
            .nodeMap = .init(ally),
        };
        var node = index.createNode(true, null);
        try node.persist();
        index.root = node;
        return index;
    }

    pub fn loadNode(self: *Index, id: u64, parent: ?*Node) !*Node {
        if (self.nodeMap.getPtr(id)) |node| return node;
        const page = try self.bucket.loadPage(id);
        const k = id;
        try self.nodeMap.put(k, Node{
            .parent = parent,
            .id = id,
            .page = page,
            .size = 0,
            .index = self,
            .isLeaf = page.data[0] == 1,
        });
        return self.nodeMap.getPtr(k).?;
    }

    pub fn createNode(self: *Index, isLeaf: bool, parent: ?*Node) *Node {
        const page = try self.bucket.createNewPage(.Index);
        const k = page.*.header.page_id;
        try self.nodeMap.put(k, Node{
            .isLeaf = isLeaf,
            .index = self,
            .parent = parent,
            .page = page,
            .size = 17,
            .id = k,
        });
        return self.nodeMap.getPtr(k);
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
        currentOffset: u16,
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
            .currentOffset = 0,
            .until = lt,
        };
        if (gt) |gtValue| {
            // Find the first node that is greater than gtValue
            iter.current = self.root.traverseChildren(gtValue);
            iter.currentIndex = idx: {
                for (iter.current.*.locationPairs.items, 0..) |pair, i| {
                    if (pair.bsonValue.order(gtValue) != .lt) {
                        break :idx i;
                    }
                }
                break :idx iter.current.*.locationPairs.items.len;
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
        // If it returns something but null, it means all shit went haywire
        try insertInternal(self, self.root, value, loc);
    }

    fn insertInternal(self: *Index, node: *Node, value: BSONValue, loc: DocumentLocation) !void {
        // Insert a new document location into the index
        if (node.isLeaf) {
            if (!node.canFit(value)) {
                // break it up and try again from the parent
                self.splitLeafNode(node);

                // ? is safe because the leaf after splitting cannot be missing the parent
                return self.insertInternal(node.parent.?, value, loc);
            }

            var insertAt = node.locationPairs.items.len;
            for (node.locationPairs.items, 0..) |locPair, i| {
                switch (locPair.bsonValue.order(value)) {
                    .lt => continue,
                    .gt => {
                        insertAt = if (i == 0) 0 else @max(i - 1, 0);
                    },
                    .eq => {
                        insertAt = i;
                    },
                }
            }
            try node.locationPairs.insert(insertAt, Node.LocationPair{
                .bsonValue = value,
                .location = loc,
            });

            if (node.leafSize() > node.page.data.len - 20) {
                // Split the node
                return try splitLeafNode(self, node);
            }
            try node.persist();

            return null;
        } else {
            const insertAt = blk: {
                for (node.keys.items, 0..) |key, i| {
                    if (key.order(value) == .lt) continue;
                    break :blk i;
                }
                break :blk node.keys.items.len;
            };
            const child = node.children.items[insertAt];
            const newOne = try self.insertInternal(child, value, loc);
            if (newOne) |newChild| {
                // Insert the new child into the parent node
                const key: BSONValue = if (newChild.isLeaf)
                    newChild.locationPairs.items[0].bsonValue
                else
                    newChild.keys.items[0];

                try node.keys.insert(insertAt, key);
                try node.children.insert(insertAt + 1, newChild);
                try node.persist();

                if (node.internalSize() > node.page.data.len - 20) {
                    // Split the node
                    return try splitLeafNode(self, node);
                }
            }

            try node.persist();
            return null;
        }
    }

    pub fn delete(self: *Index, value: BSONValue, loc: DocumentLocation) !void {
        // Delete a document location from the index
        var current: *Node = self.root;
        while (current.isLeaf != true) {
            current = current.traverseChildren(value);
        }
        var data = current.page.data;
        var offset = current.findMatchOffset(value) orelse return;
        var currValue = BSONValue.read(data[offset + 1 ..], data[offset]);
        var currValueSize: u16 = @truncate(currValue.size());
        var currLoc: DocumentLocation = .{
            .pageId = mem.readInt(u64, data[offset + 1 + currValueSize .. offset + 1 + currValueSize + 8], .little),
            .offset = mem.readInt(u16, data[offset + 1 + currValueSize + 8 .. offset + 1 + currValueSize + 8 + 2], .little),
        };
        while (currValue.order(value) != .gt) {}
        try current.persist();
    }

    fn splitInternalNode(self: *Index, node: *Node) !*Node {
        // Split the internal node into two nodes
        const page = try self.bucket.createNewPage(.Index);
        const newNode = try Node.init(self.*.allocator, self, page, false);
        const mid: usize = @divFloor(node.keys.items.len, 2) + 1;

        // Update the children pointers
        try newNode.keys.appendSlice(node.keys.items[mid + 1 ..]);
        node.keys.shrinkRetainingCapacity(mid);
        try newNode.children.appendSlice(node.children.items[mid..]);
        node.children.shrinkRetainingCapacity(mid);

        try newNode.persist();
        try node.persist();

        const promoKey = node.keys.items[mid];

        if (node == self.root) {
            const newRoot = try Node.init(self.allocator, false);
            try newRoot.keys.append(promoKey);
            try newRoot.children.append(node);
            try newRoot.children.append(newNode);
            try newRoot.persist();
            self.root = newRoot;
            return null;
        }

        return newNode;
    }

    fn splitLeafNode(self: *Index, node: *Node) !*Node {
        // Split the leaf node into two nodes
        // find approx mid, copy the right part into the new node, assign next prevs appropriately
        // add the new node record into the parent, if it exists, if not -- create new root
        if (node.parent == null or self.root == node) {
            var newRoot = self.createNode(false, null);
            newRoot.addChild(node.firstValue(), node.id);

            const newNode = self.createNode(true, newRoot);

        }

        if (node.parent) |parent| {
            con
        }
    }
};

const testing = std.testing;

test "Index inserts" {
    var bucket = try Bucket.openFile(testing.allocator, "bplus_test.bucket");
    defer bucket.deinit();
    // defer std.fs.cwd().deleteFile("bplus_test.bucket") catch |err| {
    //     std.debug.print("Error deleting file: {}\n", .{err});
    // };

    var prng = std.Random.DefaultPrng.init(blk: {
        var seed: u64 = undefined;
        try std.posix.getrandom(std.mem.asBytes(&seed));
        break :blk seed;
    });
    const rand = prng.random();

    var index = try Index.create(testing.allocator, &bucket);
    defer index.deinit(testing.allocator);
    for (0..10000) |_| {
        const value = BSONValue{ .int32 = .{ .value = rand.int(i32) } };
        const loc = Index.DocumentLocation{ .pageId = rand.int(u64), .offset = rand.int(u16) };
        try index.insert(value, loc);
    }
}

test "Index range" {
    var bucket = try Bucket.openFile(testing.allocator, "bplus_test1.bucket");
    defer bucket.deinit();
    defer std.fs.cwd().deleteFile("bplus_test1.bucket") catch |err| {
        std.debug.print("Error deleting file: {}\n", .{err});
    };

    var index = try Index.create(testing.allocator, &bucket);
    defer index.deinit(testing.allocator);
    try index.insert(BSONValue{ .int32 = .{ .value = 10 } }, .{ .pageId = 1, .offset = 10 });
    try index.insert(BSONValue{ .int32 = .{ .value = 11 } }, .{ .pageId = 1, .offset = 20 });
    try index.insert(BSONValue{ .int32 = .{ .value = 12 } }, .{ .pageId = 1, .offset = 30 });

    var iter = try index.range(null, null);
    while (try iter.next()) |loc| {
        std.debug.print("Found location: pageId={}, offset={}\n", .{ loc.pageId, loc.offset });
    }
}

test "Index delete" {
    var bucket = try Bucket.openFile(testing.allocator, "bplus_test2.bucket");
    defer bucket.deinit();
    defer std.fs.cwd().deleteFile("bplus_test2.bucket") catch |err| {
        std.debug.print("Error deleting file: {}\n", .{err});
    };

    var index = try Index.create(testing.allocator, &bucket);
    defer index.deinit(testing.allocator);
    try index.insert(BSONValue{ .int32 = .{ .value = 10 } }, .{ .pageId = 1, .offset = 10 });
    try index.insert(BSONValue{ .int32 = .{ .value = 11 } }, .{ .pageId = 1, .offset = 20 });
    try index.insert(BSONValue{ .int32 = .{ .value = 15 } }, .{ .pageId = 1, .offset = 30 });

    try index.delete(BSONValue{ .int32 = .{ .value = 11 } }, .{ .pageId = 1, .offset = 20 });
    var iter = try index.range(null, .{ .int32 = .{ .value = 12 } });
    var i: u8 = 0;
    while (try iter.next()) |loc| : (i += 1) {
        std.debug.print("Found no deleted location: pageId={}, offset={}\n", .{ loc.pageId, loc.offset });
    }
    try testing.expectEqual(2, i);
}
