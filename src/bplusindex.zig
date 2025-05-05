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

    const DocumentLocation = struct {
        pageId: u64,
        offset: u16,
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

        page: Page,
        isLeaf: bool,
        index: *Index,
        keys: std.ArrayList(BSONValue),
        children: std.ArrayList(*Node),
        locationPairs: std.ArrayList(LocationPair),
        prev: ?*Node = null,
        next: ?*Node = null,
        allocator: mem.Allocator,
        id: u64,

        fn leafSize(self: *const Node) usize {
            var size: u16 = 17;
            for (self.locationPairs.items) |pair| {
                size += 1 + pair.bsonValue.size() + 8 + 2;
            }
            return size;
        }

        fn internalSize(self: *const Node) usize {
            var size: u16 = 1 + 8;
            for (self.keys.items) |key| {
                size += 1 + key.size() + 8;
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

        pub fn init(ally: mem.Allocator, index: *Index, page: Page) !*Node {
            const isLeaf = page.data[0] == 1;
            const node = try ally.create(Node);
            node.* = Node{
                .page = page,
                .isLeaf = isLeaf,
                .index = index,
                .children = std.ArrayList(*Node).init(ally),
                .keys = std.ArrayList(BSONValue).init(ally),
                .locationPairs = std.ArrayList(LocationPair).init(ally),
                .id = page.id,
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
                    offset += bsonValue.size();
                    const pageId = mem.readInt(u64, data[offset .. offset + 8], .little);
                    offset += 8;
                    const pageOffset = mem.readInt(u16, data[offset .. offset + 2], .little);
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
                lastLeaf = node;
            } else {
                var offset: u16 = 1;
                const data = page.data;
                const firstChildId = mem.readInt(u64, data[offset .. offset + 8], .little);
                const firstChildPage = try index.bucket.loadPage(firstChildId);
                const firstChildNode = try Node.nodeFromPage(ally, index, firstChildPage, lastLeaf);
                try node.children.append(firstChildNode);
                while (offset < data.len) {
                    if (data[offset] == 0) break;
                    const typeId: bson.BSONValueType = @enumFromInt(data[offset]);
                    offset += 1;
                    const bsonValue = BSONValue.read(data[offset..], typeId);
                    try node.keys.append(bsonValue);
                    offset += bsonValue.size();
                    const nodeId = mem.readInt(u64, data[offset .. offset + 8], .little);
                    offset += 8;

                    const childPage = try index.bucket.loadPage(nodeId);
                    const childNode = try Node.nodeFromPage(ally, index, childPage, lastLeaf);
                    try node.children.append(childNode);
                }
            }
            return node;
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

    fn load(ally: mem.Allocator, bucket: *Bucket, pageId: u64) *Index {
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
        const node = try Node.nodeFromPage(ally, index, page, &lastLeafNode);
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

    pub fn insert(self: *Index, value: BSONValue, loc: DocumentLocation) !void {
        const newRoot = try insertInternal(self, self.root, value, loc);
        if (newRoot) |root| {
            self.root = root;
        }
    }

    fn insertInternal(self: *Index, node: *Node, value: BSONValue, loc: DocumentLocation) !?*Node {
        // Insert a new document location into the index
        if (node.isLeaf) {
            const insertAt = blk: {
                for (node.locationPairs.items, 0..) |locPair, i| {
                    switch (locPair.bsonValue.order(value)) {
                        .lt => continue,
                        .gt => break :blk @max(i - 1, 0),
                        .eq => break :blk i,
                    }
                }
                break :blk node.keys.items.len;
            };
            try node.locationPairs.insert(insertAt, Node.LocationPair{
                .bsonValue = value,
                .location = loc,
            });

            if (node.leafSize() > node.page.data.len - 20) {
                // Split the node
                return try splitLeafNode(self, node);
            }

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
                const key: BSONValue = if (newChild.isLeaf) {
                    newChild.locationPairs.items[0].bsonValue;
                } else {
                    newChild.keys.items[0];
                };
                try node.keys.insert(insertAt, key);
                try node.children.insert(insertAt + 1, newChild);

                if (node.internalSize() > node.page.data.len - 20) {
                    // Split the node
                    return try splitLeafNode(self, node);
                }
            }
        }
    }

    fn splitInternalNode(self: *Index, node: *Node) !*Node {
        // Split the internal node into two nodes
        const page = try self.bucket.createNewPage(.Index);
        const newNode = try Node.init(self.*.allocator, self, page);
        const mid: usize = @divFloor(node.keys.items.len, 2) + 1;
        try newNode.keys.appendSlice(node.keys.items[mid + 1 ..]);
        node.keys.shrinkAndFree(mid);

        // Update the children pointers
        for (node.children.items[mid..]) |child| {
            try newNode.children.append(child);
        }
        node.children.shrinkAndFree(mid);

        return newNode;
    }

    fn splitLeafNode(self: *Index, node: *Node) !*Node {
        // Split the leaf node into two nodes
        const page = try self.bucket.createNewPage(.Index);
        const newNode = try Node.init(self.*.allocator, self, page);
        const mid: usize = @divFloor(node.locationPairs.items.len, 2);
        try newNode.locationPairs.appendSlice(node.locationPairs.items[mid..]);
        try node.locationPairs.shrinkAndFree(mid);

        // Update the next and previous pointers
        if (node.next) |next| {
            next.prev = newNode;
            newNode.next = next;
        }
        newNode.prev = node;
        node.next = newNode;

        return newNode;
    }
};
