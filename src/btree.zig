const std = @import("std");
const ObjectId = @import("object_id.zig").ObjectId;

pub const BTreeNode = struct {
    keys: std.ArrayList(ObjectId),
    values: std.ArrayList(u64),
    children: std.ArrayList(*BTreeNode),
    is_leaf: bool,
    order: usize, // Maximum number of children

    pub fn init(allocator: std.mem.Allocator, order: usize) !*BTreeNode {
        const node = try allocator.create(BTreeNode);
        node.* = BTreeNode{
            .keys = std.ArrayList(ObjectId).init(allocator),
            .values = std.ArrayList(u64).init(allocator),
            .children = std.ArrayList(*BTreeNode).init(allocator),
            .is_leaf = true,
            .order = order,
        };
        return node;
    }

    pub fn deinit(self: *BTreeNode) void {
        for (self.children.items) |child| {
            child.deinit();
            self.children.allocator.destroy(child);
        }
        self.keys.deinit();
        self.values.deinit();
        self.children.deinit();
    }
};

pub const BTree = struct {
    root: ?*BTreeNode,
    order: usize,
    allocator: std.mem.Allocator,

    pub fn init(allocator: std.mem.Allocator, order: usize) BTree {
        return BTree{
            .root = null,
            .order = order,
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *BTree) void {
        if (self.root) |root| {
            root.deinit();
            self.allocator.destroy(root);
        }
    }

    pub fn insert(self: *BTree, key: ObjectId, value: u64) !void {
        if (self.root == null) {
            self.root = try BTreeNode.init(self.allocator, self.order);
        }

        const root = self.root.?;

        // Handle root split if needed
        if (root.keys.items.len == (2 * self.order - 1)) {
            const new_root = try BTreeNode.init(self.allocator, self.order);
            new_root.is_leaf = false;
            new_root.children.append(root) catch unreachable;
            try self.splitChild(new_root, 0);
            self.root = new_root;
            try self.insertNonFull(new_root, key, value);
        } else {
            try self.insertNonFull(root, key, value);
        }
    }

    fn insertNonFull(self: *BTree, node: *BTreeNode, key: ObjectId, value: u64) !void {
        var i: isize = @as(isize, @intCast(node.keys.items.len)) - 1;

        if (node.is_leaf) {
            // Find position to insert
            while (i >= 0 and std.mem.lessThan(u8, key.buffer[0..], node.keys.items[@as(usize, @intCast(i))].buffer[0..])) {
                i -= 1;
            }

            try node.keys.insert(@as(usize, @intCast(i + 1)), key);
            try node.values.insert(@as(usize, @intCast(i + 1)), value);
        } else {
            // Find child to recurse
            while (i >= 0 and std.mem.lessThan(u8, key.buffer[0..], node.keys.items[@as(usize, @intCast(i))].buffer[0..])) {
                i -= 1;
            }
            i += 1;

            var child_idx = @as(usize, @intCast(i));
            const child = node.children.items[child_idx];

            if (child.keys.items.len == (2 * self.order - 1)) {
                try self.splitChild(node, child_idx);
                if (std.mem.lessThan(u8, node.keys.items[child_idx].buffer[0..], key.buffer[0..])) {
                    child_idx += 1;
                }
            }
            try self.insertNonFull(node.children.items[child_idx], key, value);
        }
    }

    fn splitChild(self: *BTree, parent: *BTreeNode, child_idx: usize) !void {
        const child = parent.children.items[child_idx];
        const new_node = try BTreeNode.init(self.allocator, self.order);
        new_node.is_leaf = child.is_leaf;

        // Copy last (order-1) keys and values to new node
        const mid = self.order - 1;
        var i: usize = 0;
        while (i < mid) : (i += 1) {
            try new_node.keys.append(child.keys.items[i + self.order]);
            try new_node.values.append(child.values.items[i + self.order]);
        }

        // If not leaf, move corresponding children
        if (!child.is_leaf) {
            i = 0;
            while (i < self.order) : (i += 1) {
                try new_node.children.append(child.children.items[i + self.order]);
            }
            child.children.shrinkRetainingCapacity(self.order);
        }

        // Insert new node into parent
        try parent.children.insert(child_idx + 1, new_node);
        try parent.keys.insert(child_idx, child.keys.items[mid]);
        try parent.values.insert(child_idx, child.values.items[mid]);

        // Shrink original child
        child.keys.shrinkRetainingCapacity(mid);
        child.values.shrinkRetainingCapacity(mid);
    }

    pub fn search(self: *const BTree, key: ObjectId) ?u64 {
        if (self.root == null) return null;
        return self.searchNode(self.root.?, key);
    }

    fn searchNode(self: *const BTree, node: *BTreeNode, key: ObjectId) ?u64 {
        var i: usize = 0;
        while (i < node.keys.items.len and std.mem.lessThan(u8, node.keys.items[i].buffer[0..], key.buffer[0..])) {
            i += 1;
        }

        if (i < node.keys.items.len and std.mem.eql(u8, node.keys.items[i].buffer[0..], key.buffer[0..])) {
            return node.values.items[i];
        }

        if (node.is_leaf) {
            return null;
        }

        return self.searchNode(node.children.items[i], key);
    }

    pub fn serialize(self: *const BTree, writer: anytype) !void {
        try writer.writeInt(usize, self.order, .little);
        if (self.root) |root| {
            try writer.writeByte(1);
            try self.serializeNode(root, writer);
        } else {
            try writer.writeByte(0);
        }
    }

    fn serializeNode(self: *const BTree, node: *BTreeNode, writer: anytype) !void {
        try writer.writeByte(if (node.is_leaf) 1 else 0);
        try writer.writeInt(usize, node.keys.items.len, .little);

        // Write keys and values
        for (node.keys.items, node.values.items) |key, value| {
            try writer.writeAll(&key.buffer);
            try writer.writeInt(u64, value, .little);
        }

        // Write children recursively if not leaf
        if (!node.is_leaf) {
            for (node.children.items) |child| {
                try self.serializeNode(child, writer);
            }
        }
    }

    pub fn deserialize(allocator: std.mem.Allocator, reader: anytype) !BTree {
        const order = try reader.readInt(usize, .little);
        var tree = BTree.init(allocator, order);

        const has_root = try reader.readByte();
        if (has_root == 1) {
            tree.root = try tree.deserializeNode(reader);
        }

        return tree;
    }

    fn deserializeNode(self: *BTree, reader: anytype) !*BTreeNode {
        const node = try BTreeNode.init(self.allocator, self.order);
        node.is_leaf = (try reader.readByte()) == 1;
        const num_keys = try reader.readInt(usize, .little);

        // Read keys and values
        var i: usize = 0;
        while (i < num_keys) : (i += 1) {
            var key_buffer: [12]u8 = undefined;
            _ = try reader.readAll(&key_buffer);
            const key = ObjectId{ .buffer = key_buffer };
            const value = try reader.readInt(u64, .little);

            try node.keys.append(key);
            try node.values.append(value);
        }

        // Read children recursively if not leaf
        if (!node.is_leaf) {
            i = 0;
            while (i <= num_keys) : (i += 1) {
                const child = try self.deserializeNode(reader);
                try node.children.append(child);
            }
        }

        return node;
    }
};

test "BTree basic operations" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    var tree = BTree.init(allocator, 3);
    defer tree.deinit();

    // Create some test ObjectIds
    const id1 = ObjectId.parseString("507c7f79bcf86cd7994f6c0e");
    const id2 = ObjectId.parseString("507c7f79bcf86cd7994f6c0f");
    const id3 = ObjectId.parseString("507c7f79bcf86cd7994f6c10");

    // Test insertion
    try tree.insert(id1, 1);
    try tree.insert(id2, 2);
    try tree.insert(id3, 3);

    // Test search
    try std.testing.expectEqual(tree.search(id1).?, 1);
    try std.testing.expectEqual(tree.search(id2).?, 2);
    try std.testing.expectEqual(tree.search(id3).?, 3);

    // Test serialization
    var buffer = std.ArrayList(u8).init(allocator);
    defer buffer.deinit();

    try tree.serialize(buffer.writer());

    // Test deserialization
    var stream = std.io.fixedBufferStream(buffer.items);
    var deserializedTree = try BTree.deserialize(allocator, stream.reader());
    defer deserializedTree.deinit();

    // Verify deserialized tree
    try std.testing.expectEqual(deserializedTree.search(id1).?, 1);
    try std.testing.expectEqual(deserializedTree.search(id2).?, 2);
    try std.testing.expectEqual(deserializedTree.search(id3).?, 3);
}
