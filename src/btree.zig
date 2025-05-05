const std = @import("std");
const mem = std.mem;
const Allocator = mem.Allocator;
const BSONValue = @import("bson.zig").BSONValue;

pub const DocumentLocation = struct {
    pageId: u64,
    offset: u16,
};

pub const DocLocationList = struct {
    locations: std.ArrayList(DocumentLocation),

    pub fn init(allocator: std.mem.Allocator) DocLocationList {
        return .{
            .locations = std.ArrayList(DocumentLocation).init(allocator),
        };
    }

    pub fn deinit(self: *DocLocationList) void {
        self.locations.deinit();
    }

    pub fn append(self: *DocLocationList, loc: DocumentLocation) !void {
        try self.locations.append(loc);
    }

    pub fn items(self: *const DocLocationList) []const DocumentLocation {
        return self.locations.items;
    }
};

pub const BPlusTree = struct {
    const DEFAULT_ORDER = 4;
    const MIN_KEYS = DEFAULT_ORDER / 2;

    allocator: std.mem.Allocator,
    root: ?*Node,

    const Node = struct {
        keys: std.ArrayList(BSONValue),
        isLeaf: bool,
        children: std.ArrayList(*Node),
        values: ?std.ArrayList(DocLocationList), // Changed to store lists of locations
        next: ?*Node,

        pub fn init(allocator: std.mem.Allocator, isLeaf: bool) !*Node {
            const node = try allocator.create(Node);
            node.* = Node{
                .keys = std.ArrayList(BSONValue).init(allocator),
                .isLeaf = isLeaf,
                .children = std.ArrayList(*Node).init(allocator),
                .values = if (isLeaf) std.ArrayList(DocLocationList).init(allocator) else null,
                .next = null,
            };
            return node;
        }

        pub fn deinit(self: *Node, allocator: std.mem.Allocator) void {
            self.keys.deinit();
            for (self.children.items) |child| {
                child.deinit(allocator);
            }
            self.children.deinit();
            if (self.values) |*values| {
                for (values.items) |*list| {
                    list.deinit();
                }
                values.deinit();
            }
            allocator.destroy(self);
        }

        fn findKeyIndex(self: *Node, key: BSONValue) usize {
            for (self.keys.items, 0..) |nodeKey, i| {
                if (key.order(nodeKey) != .gt) {
                    return i;
                }
            }
            return self.keys.items.len;
        }

        fn findExactKeyIndex(self: *Node, key: BSONValue) ?usize {
            for (self.keys.items, 0..) |nodeKey, i| {
                if (key.eql(nodeKey)) {
                    return i;
                }
            }
            return null;
        }
    };

    pub fn init(allocator: std.mem.Allocator) BPlusTree {
        return BPlusTree{
            .allocator = allocator,
            .root = null,
        };
    }

    pub fn deinit(self: *BPlusTree) void {
        if (self.root) |root| {
            root.deinit(self.allocator);
        }
    }

    pub fn insert(self: *BPlusTree, key: BSONValue, loc: DocumentLocation) !void {
        if (self.root == null) {
            const newNode = try Node.init(self.allocator, true);
            try newNode.keys.append(key);
            var locList = DocLocationList.init(self.allocator);
            try locList.append(loc);
            try newNode.values.?.append(locList);
            self.root = newNode;
            return;
        }

        const result = try self.insertInternal(self.root.?, key, loc);
        if (result) |newRoot| {
            self.root = newRoot;
        }
    }

    fn insertInternal(self: *BPlusTree, node: *Node, key: BSONValue, loc: DocumentLocation) !?*Node {
        if (node.isLeaf) {
            // Check if key already exists
            if (node.findExactKeyIndex(key)) |existingIndex| {
                // Add location to existing list
                try node.values.?.items[existingIndex].append(loc);
                return null;
            }

            const keyIndex = node.findKeyIndex(key);
            try node.keys.insert(keyIndex, key);
            var locList = DocLocationList.init(self.allocator);
            try locList.append(loc);
            try node.values.?.insert(keyIndex, locList);

            if (node.keys.items.len < DEFAULT_ORDER) {
                return null;
            }
            return try self.splitLeaf(node);
        }

        const keyIndex = node.findKeyIndex(key);
        const childResult = try self.insertInternal(node.children.items[keyIndex], key, loc);
        if (childResult) |newChild| {
            try node.keys.insert(keyIndex, newChild.keys.items[0]);
            try node.children.insert(keyIndex + 1, newChild);

            if (node.keys.items.len < DEFAULT_ORDER) {
                return null;
            }

            // If this is the root node that needs splitting, create a new root
            if (node == self.root) {
                const splitResult = try self.splitInternal(node);
                const newRoot = try Node.init(self.allocator, false);
                try newRoot.keys.append(splitResult.promoKey);
                try newRoot.children.append(node);
                try newRoot.children.append(splitResult.rightNode);
                return newRoot;
            }

            return try self.splitInternal(node);
        }
        return null;
    }

    fn splitLeaf(self: *BPlusTree, node: *Node) !*Node {
        const midPoint = (DEFAULT_ORDER + 1) / 2;
        const newNode = try Node.init(self.allocator, true);

        // Move half of the keys and values to the new node
        try newNode.keys.appendSlice(node.keys.items[midPoint..]);
        for (node.values.?.items[midPoint..]) |locList| {
            var newLocList = DocLocationList.init(self.allocator);
            try newLocList.locations.appendSlice(locList.locations.items);
            try newNode.values.?.append(newLocList);
        }

        // Clean up old values
        for (node.values.?.items[midPoint..]) |*locList| {
            locList.deinit();
        }

        // Truncate the original node
        node.keys.shrinkRetainingCapacity(midPoint);
        node.values.?.shrinkRetainingCapacity(midPoint);

        // Set up the leaf node chain
        newNode.next = node.next;
        node.next = newNode;

        return newNode;
    }

    const SplitResult = struct {
        rightNode: *Node,
        promoKey: BSONValue,
    };

    fn splitInternal(self: *BPlusTree, node: *Node) !SplitResult {
        const midPoint = DEFAULT_ORDER / 2;
        const newNode = try Node.init(self.allocator, false);

        // Save the middle key that will be promoted
        const promoKey = node.keys.items[midPoint];

        // Copy keys after midPoint+1 to new node
        try newNode.keys.appendSlice(node.keys.items[midPoint + 1 ..]);
        // Copy children after midPoint+1 to new node
        try newNode.children.appendSlice(node.children.items[midPoint + 1 ..]);

        // Truncate the original node
        node.keys.shrinkRetainingCapacity(midPoint);
        node.children.shrinkRetainingCapacity(midPoint + 1);

        return SplitResult{
            .rightNode = newNode,
            .promoKey = promoKey,
        };
    }

    pub fn find(self: *const BPlusTree, key: BSONValue) ?[]const DocumentLocation {
        var current = self.root orelse return null;

        while (!current.isLeaf) {
            const index = current.findKeyIndex(key);
            current = current.children.items[index];
        }

        if (current.findExactKeyIndex(key)) |index| {
            return current.values.?.items[index].items();
        }

        return null;
    }

    pub fn range(self: *const BPlusTree, start: BSONValue, end: BSONValue) !std.ArrayList(DocumentLocation) {
        var results = std.ArrayList(DocumentLocation).init(self.allocator);
        errdefer results.deinit();

        var current: ?*Node = self.root orelse return results;

        // Find the leaf node containing the start key
        while (!current.?.isLeaf) {
            const index = current.?.findKeyIndex(start);
            current = current.?.children.items[index];
        }

        // Iterate through leaf nodes until we reach the end key
        var index = current.?.findKeyIndex(start);
        while (current) |curr| : ({
            current = curr.next;
            index = 0;
        }) {
            while (index < curr.keys.items.len) : (index += 1) {
                const key = curr.keys.items[index];
                if (key.order(end) == .gt) {
                    return results;
                }
                // Append all locations for this key
                try results.appendSlice(curr.values.?.items[index].items());
            }
        }

        return results;
    }
};

test "BPlusTree multiple values per key" {
    var tree = BPlusTree.init(std.testing.allocator);
    defer tree.deinit();

    // Insert multiple locations for the same key
    const key1 = BSONValue{ .int32 = .{ .value = 1 } };
    try tree.insert(key1, DocumentLocation{ .pageId = 1, .offset = 0 });
    try tree.insert(key1, DocumentLocation{ .pageId = 1, .offset = 100 });
    try tree.insert(key1, DocumentLocation{ .pageId = 2, .offset = 0 });

    // Insert another key
    const key2 = BSONValue{ .int32 = .{ .value = 2 } };
    try tree.insert(key2, DocumentLocation{ .pageId = 3, .offset = 0 });

    // Test finding multiple locations for key1
    if (tree.find(key1)) |locations| {
        try std.testing.expectEqual(@as(usize, 3), locations.len);
        try std.testing.expectEqual(@as(u64, 1), locations[0].pageId);
        try std.testing.expectEqual(@as(u16, 0), locations[0].offset);
        try std.testing.expectEqual(@as(u64, 1), locations[1].pageId);
        try std.testing.expectEqual(@as(u16, 100), locations[1].offset);
        try std.testing.expectEqual(@as(u64, 2), locations[2].pageId);
    } else {
        try std.testing.expect(false);
    }

    // Test range query
    var range_results = try tree.range(key1, key2);
    defer range_results.deinit();

    try std.testing.expectEqual(@as(usize, 4), range_results.items.len);
    try std.testing.expectEqual(@as(u64, 1), range_results.items[0].pageId);
    try std.testing.expectEqual(@as(u64, 1), range_results.items[1].pageId);
    try std.testing.expectEqual(@as(u64, 2), range_results.items[2].pageId);
    try std.testing.expectEqual(@as(u64, 3), range_results.items[3].pageId);
}
