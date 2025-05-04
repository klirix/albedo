const std = @import("std");
const mem = std.mem;
const albedo = @import("albedo.zig");
const bson = @import("bson.zig");
const BSONValue = bson.BSONValue;
const Bucket = albedo.Bucket;
const Page = albedo.Page;

const Index = struct {
    bucket: *Bucket,
    root: Node,

    const DocumentLocation = struct {
        pageId: u64,
        offset: u16,
    };

    const Node = struct {

        // Data layout:
        //   Leaf node:
        //     u8 = 1
        //     u64 = previous leaf nodeId
        //     Repeatedly:
        //       u8 = BSONValueType
        //       [n]u8 = BSONValue
        //       u64 = documentId
        //       u16 = document offset
        //     u64 = next leaf nodeId
        //   Internal node:
        //     u8 = 0
        //     Repeatedly:
        //       u8 = BSONValueType
        //       [n]u8 = BSONValue
        //       u64 = child nodeId

        page: Page,
        isLeaf: bool,
        index: *Index,

        pub fn nodeFromPage(index: *Index, page: Page) Node {
            const isLeaf = page.data[0] == 1;
            return Node{
                .page = page,
                .isLeaf = isLeaf,
                .index = index,
            };
        }

        fn traverseChildren(self: *const Node, value: BSONValue) ?u64 {
            var offset: u16 = 1;
            var lastNodeId: ?u64 = null;
            const data = self.page.data;
            while (offset < data.len) {
                const typeId: bson.BSONValueType = @enumFromInt(data[offset]);
                if (data[offset] == 0) break;
                offset += 1;
                const bsonValue = BSONValue.read(data[offset..], typeId);
                offset += bsonValue.size();
                const nodeId = mem.readInt(u64, data[offset .. offset + 8], .little);
                offset += 8;
                if (bsonValue.order(value) == .gt) {
                    return lastNodeId;
                }
                lastNodeId = nodeId;
            }
            return lastNodeId;
        }

        fn findMatch(self: *const Node, value: BSONValue) ?DocumentLocation {
            var offset: u16 = 1 + @sizeOf(u64); // Skip the first byte and the previous node ID
            const data = self.page.data;
            while (offset < data.len) {
                const typeId: bson.BSONValueType = @enumFromInt(data[offset]);
                if (data[offset] == 0) break;
                offset += 1;
                const bsonValue = BSONValue.read(data[offset..], typeId);
                offset += bsonValue.size();
                if (bsonValue.order(value) == .eq) {
                    const pageId = mem.readInt(u64, data[offset .. offset + 8], .little);
                    const docOffset = mem.readInt(u16, data[offset + 8 .. offset + 10], .little);
                    return DocumentLocation{
                        .pageId = pageId,
                        .offset = docOffset,
                    };
                }
                offset += @sizeOf(u64) + @sizeOf(u16); // Skip the document ID and offset
            }
            return null;
        }
    };

    fn load(bucket: *Bucket, pageId: u64) Index {
        // Load the index page from the bucket
        const page = try bucket.loadPage(pageId);
        const node = Node{
            .page = &page,
            .isLeaf = page.data[0] == 1,
        };
        return Index{
            .bucket = bucket,
            .root = &node,
        };
    }

    pub fn findExact(self: *const Index, value: BSONValue) !?DocumentLocation {
        // Find the document location in the index
        var current = self.root;

        while (current.isLeaf != true) {
            const childNodeId = current.traverseChildren(value) orelse return null;
            const childPage = try self.bucket.loadPage(childNodeId);
            current = Node.nodeFromPage(self.index, childPage);
        }

        return current.findMatch(value);
    }

    pub fn insert(self: *Index, value: BSONValue, _: DocumentLocation) !void {
        // Insert a new document location into the index
        var current = self.root;

        while (current.isLeaf != true) {
            const childNodeId = current.traverseChildren(value) orelse return null;
            const childPage = try self.bucket.loadPage(childNodeId);
            current = Node.nodeFromPage(self.index, childPage);
        }
    }

    // fn create(bucket: *Bucket, path: []const u8) Index {
    //     // Create a new index root page
    //     const page = try bucket.createNewPage(.Index);
    // }
};
