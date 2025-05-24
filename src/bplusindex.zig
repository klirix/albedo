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

        pub fn init(ally: mem.Allocator, index: *Index, page: *Page) !*Node {
            const isLeaf = page.data[0] == 1;
            const node = try ally.create(Node);
            node.* = Node{
                .page = page,
                .isLeaf = isLeaf,
                .index = index,
                .children = .init(ally),
                .keys = .init(ally),
                .locationPairs = .init(ally),
                .id = page.header.page_id,
            };
            return node;
        }

        pub fn nodeFromPage(ally: mem.Allocator, index: *Index, page: Page, lastLeaf: *?Node) !*Node {
            const node = try Node.init(ally, index, page);

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
                    try node.locationPairs.append(LocationPair{
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
                const firstChildNode = try Node.nodeFromPage(ally, index, firstChildPage, lastLeaf);
                try node.children.append(firstChildNode);
                while (offset < data.len) {
                    if (data[offset] == 0) break;
                    const typeId: bson.BSONValueType = @enumFromInt(data[offset]);
                    offset += 1;
                    const bsonValue = BSONValue.read(data[offset..], typeId);
                    try node.keys.append(bsonValue);
                    offset += @truncate(bsonValue.size());
                    const nodeId = mem.readInt(u64, data[offset..][0..8], .little);
                    offset += 8;

                    const childPage = try index.bucket.loadPage(nodeId);
                    const childNode = try Node.nodeFromPage(ally, index, childPage, lastLeaf);
                    try node.children.append(childNode);
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

        pub fn deinit(self: *const Node, ally: mem.Allocator) void {
            self.keys.deinit();
            defer self.children.deinit();
            self.locationPairs.deinit();

            for (self.children.items) |node| {
                node.deinit(ally);
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
            for (self.keys.items, 0..) |key, i| {
                switch (key.order(value)) {
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
        const node = try Node.nodeFromPage(ally, &index, page, &lastLeafNode);
        index.root = node;
        return index;
    }

    fn deinit(self: *Index, ally: mem.Allocator) void {
        // Deinitialize the index
        self.root.deinit(ally);
        ally.destroy(self.root);
    }

    pub fn create(ally: mem.Allocator, bucket: *Bucket) !Index {
        // Create a new index
        const page = try bucket.createNewPage(.Index);
        page.data[0] = 1; // Leaf node
        try bucket.writePage(page);
        var index = Index{
            .bucket = bucket,
            .root = undefined,
            .allocator = ally,
        };
        const node = try Node.init(ally, &index, page);
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
        _ = try insertInternal(self, self.root, value, loc);
    }

    fn insertInternal(self: *Index, node: *Node, value: BSONValue, loc: DocumentLocation) !?*Node {
        // Insert a new document location into the index
        if (node.isLeaf) {
            var insertAt = node.locationPairs.items.len;
            for (node.locationPairs.items, 0..) |locPair, i| {
                switch (locPair.bsonValue.order(value)) {
                    .lt => continue,
                    .gt => {
                        insertAt = @max(i - 1, 0);
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
            const newOne = try insertInternal(self, child, value, loc);
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

    fn delete(self: *Index, value: BSONValue, loc: DocumentLocation) !void {
        // Delete a document location from the index
        var current = self.root;
        while (current.isLeaf != true) {
            current = current.traverseChildren(value);
        }
        const index = current.findMatchIndex(value);
        if (index == null) return;
        while (true) {
            const locPair = current.locationPairs.items[index.*];
            if (locPair.location.equal(&loc)) {
                current.locationPairs.orderedRemove(index);
                break;
            }
            index += 1;
            if (index >= current.locationPairs.items.len) {
                current = current.next;
                index = 0;
                if (current == null) break;
            }
            if (current.locationPairs.items[index.*].bsonValue.order(value) != .eq) {
                break;
            }
        }
        try current.persist();
    }

    fn splitInternalNode(self: *Index, node: *Node) !*Node {
        // Split the internal node into two nodes
        const page = try self.bucket.createNewPage(.Index);
        const newNode = try Node.init(self.*.allocator, self, page);
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
        const page = try self.bucket.createNewPage(.Index);
        const newNode = try Node.init(self.*.allocator, self, page);
        const mid: usize = @divFloor(node.locationPairs.items.len, 2);
        try newNode.locationPairs.appendSlice(node.locationPairs.items[mid..]);
        node.locationPairs.shrinkRetainingCapacity(mid);

        // Update the next and previous pointers
        if (node.next) |next| {
            next.prev = newNode;
            newNode.next = next;
        }
        newNode.prev = node;
        node.next = newNode;
        try node.persist();
        try newNode.persist();

        return newNode;
    }
};

const testing = std.testing;

test "Index inserts" {
    var bucket = try Bucket.openFile(testing.allocator, "bplus_test.bucket");
    defer bucket.deinit();
    defer std.fs.cwd().deleteFile("bplus_test.bucket") catch |err| {
        std.debug.print("Error deleting file: {}\n", .{err});
    };

    var index = try Index.create(testing.allocator, &bucket);
    defer index.deinit(testing.allocator);
    try index.insert(BSONValue{ .int32 = .{ .value = 10 } }, .{ .pageId = 1, .offset = 10 });
    try index.insert(BSONValue{ .int32 = .{ .value = 11 } }, .{ .pageId = 1, .offset = 20 });
    try index.insert(BSONValue{ .int32 = .{ .value = 12 } }, .{ .pageId = 1, .offset = 30 });
}

test "Index range" {
    var bucket = try Bucket.openFile(testing.allocator, "bplus_test.bucket");
    defer bucket.deinit();
    defer std.fs.cwd().deleteFile("bplus_test.bucket") catch |err| {
        std.debug.print("Error deleting file: {}\n", .{err});
    };

    var index = try Index.create(testing.allocator, &bucket);
    defer index.deinit(testing.allocator);
    try index.insert(BSONValue{ .int32 = .{ .value = 10 } }, .{ .pageId = 1, .offset = 10 });
    try index.insert(BSONValue{ .int32 = .{ .value = 11 } }, .{ .pageId = 1, .offset = 20 });
    try index.insert(BSONValue{ .int32 = .{ .value = 12 } }, .{ .pageId = 1, .offset = 30 });

    var iter = try index.range(null, null);
    while (true) {
        const maybeLoc = try iter.next();
        if (maybeLoc) |loc| {
            std.debug.print("Found location: pageId={}, offset={}\n", .{ loc.pageId, loc.offset });
        }
    }
}
