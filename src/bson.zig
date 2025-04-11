const std = @import("std");
const mem = std.mem;
const ObjectId = @import("./object_id.zig").ObjectId;

pub const BSONString = struct {
    // code: i8, // 0x02
    value: []u8,

    pub fn write(self: BSONString, memory: []u8) void {
        // var memory = try allocator.alloc(u8, 4 + self.length);
        const length = @as(u32, @truncate(self.value.len)) + 1;

        std.mem.copyForwards(u8, memory[0..4], &mem.toBytes(length));
        std.mem.copyForwards(u8, memory[4 .. length + 4], self.value);
        memory[length + 3] = 0;
    }

    pub fn size(self: *BSONString) u32 {
        return 4 + @as(u32, @truncate(self.value.len)) + 1;
    }

    pub fn read(memory: []const u8) BSONString {
        const length = std.mem.bytesToValue(u32, memory[0..4]) - 1;

        return BSONString{ .value = @constCast(memory[4 .. 4 + length]) };
    }
};

test "encodes BSONString" {
    var allocator = std.testing.allocator;
    const test_string = "test";
    // const less_const: []const u8 = test_string;
    var string = BSONString{ .value = @constCast(test_string) };
    const actual = allocator.alloc(u8, string.size()) catch unreachable;
    defer allocator.free(actual);

    string.write(actual);

    const expected_string = @constCast("\x05\x00\x00\x00test\x00");
    const expected: []u8 = expected_string.*[0..];
    // for (actual) |value| {
    //     std.debug.print("{x} ", .{value});
    // }
    // std.debug.print("{x}", .{actual});

    try std.testing.expectEqualSlices(u8, expected, actual);
}

test "decodes BSONString" {
    // var allocator = std.testing.allocator;
    const test_memory = @constCast(&[_]u8{ 5, 0, 0, 0, 0x74, 0x65, 0x73, 0x74, 0 });
    const test_string = "test";
    // const less_const: []const u8 = test_string;
    const string = BSONString.read(test_memory[0..]);
    try std.testing.expectEqualSlices(u8, test_string, string.value);
    try std.testing.expectEqual(4, string.value.len);
    // const actual = allocator.alloc(u8, string.size()) catch unreachable;
    // string.write(actual);
    // defer allocator.free(actual);

    // const expected_string = @constCast("\x05\x00\x00\x00test\x00");
    // const expected: []u8 = expected_string.*[0..];
    // // for (actual) |value| {
    // //     std.debug.print("{x} ", .{value});
    // // }
    // std.debug.print("{x}", .{actual});

    // try std.testing.expectEqualSlices(u8, expected, actual);
}

pub const BSONDouble = struct {
    // code: i8, // 0x02
    value: f64,

    pub fn write(self: BSONDouble, memory: []u8) void {
        // var memory = try allocator.alloc(u8, 4 + self.length);

        std.mem.copyForwards(u8, memory, &mem.toBytes(self.value));
    }

    pub fn size(_: BSONDouble) u32 {
        return 8;
    }

    pub fn read(memory: []const u8) BSONDouble {
        return BSONDouble{
            .value = std.mem.bytesToValue(f64, memory),
        };
    }
};

test "encodes BSONDouble" {
    var allocator = std.testing.allocator;
    const test_double = 123.456;
    var double = BSONDouble{ .value = test_double };
    const actual = allocator.alloc(u8, double.size()) catch unreachable;
    double.write(actual);
    defer allocator.free(actual);

    const expected_double = @constCast(&[_]u8{ 0x77, 0xbe, 0x9f, 0x1a, 0x2f, 0xdd, 0x5e, 0x40 });
    const expected: []u8 = expected_double[0..];
    try std.testing.expectEqualSlices(u8, expected, actual);
}

test "decodes BSONDouble" {
    const test_memory = (&[_]u8{ 0x77, 0xbe, 0x9f, 0x1a, 0x2f, 0xdd, 0x5e, 0x40 });
    const expected_double = 123.456;
    const double = BSONDouble.read(test_memory[0..]);
    try std.testing.expectEqual(expected_double, double.value);
}

pub const BSONInt32 = struct {
    // code: i8, // 0x02
    value: i32,

    pub fn write(self: BSONInt32, memory: []u8) void {
        // var memory = try allocator.alloc(u8, 4 + self.length);

        std.mem.copyForwards(u8, memory, &mem.toBytes(self.value));
    }

    pub fn size(_: BSONInt32) u32 {
        return 4;
    }

    pub fn read(memory: []const u8) BSONInt32 {
        return BSONInt32{
            .value = std.mem.bytesToValue(i32, memory),
        };
    }
};

test "encodes BSONInt32" {
    var allocator = std.testing.allocator;
    const test_int = 32;
    var double = BSONInt32{ .value = test_int };
    const actual = allocator.alloc(u8, double.size()) catch unreachable;
    double.write(actual);
    defer allocator.free(actual);

    const expected_double = @constCast(&[_]u8{ 0x20, 0, 0, 0 });
    const expected: []u8 = expected_double[0..];
    try std.testing.expectEqualSlices(u8, expected, actual);
}

test "decodes BSONInt32" {
    const test_memory = @constCast(&[_]u8{ 32, 0, 0, 0 });
    const expected_double = 32;
    const double = BSONInt32.read(test_memory[0..]);
    try std.testing.expectEqual(expected_double, double.value);
}

pub const BSONInt64 = struct {
    // code: i8, // 0x02
    value: i64,

    pub fn write(self: BSONInt64, memory: []u8) void {
        // var memory = try allocator.alloc(u8, 4 + self.length);

        std.mem.copyForwards(u8, memory, &mem.toBytes(self.value));
    }

    pub fn size(_: BSONInt64) u32 {
        return 8;
    }

    pub fn read(memory: []const u8) BSONInt64 {
        return BSONInt64{
            .value = std.mem.bytesToValue(i64, memory),
        };
    }
};

test "encodes BSONInt64" {
    var allocator = std.testing.allocator;
    const test_int = 32;
    var double = BSONInt64{ .value = test_int };
    const actual = allocator.alloc(u8, double.size()) catch unreachable;
    double.write(actual);
    defer allocator.free(actual);

    const expected_double = @constCast(&[_]u8{ 0x20, 0, 0, 0, 0, 0, 0, 0 });
    const expected: []u8 = expected_double[0..];
    try std.testing.expectEqualSlices(u8, expected, actual);
}

test "decodes BSONInt64" {
    const test_memory = @constCast(&[_]u8{ 32, 0, 0, 0, 0, 0, 0, 0 });
    const expected_double = 32;
    const double = BSONInt64.read(test_memory[0..]);
    try std.testing.expectEqual(expected_double, double.value);
}

pub const BSONBinary = struct {
    value: []const u8,
    subtype: u8,

    pub fn write(self: BSONBinary, memory: []u8) void {
        const length = @as(u32, @truncate(self.value.len));
        std.mem.copyForwards(u8, memory[0..4], &mem.toBytes(length));
        memory[4] = self.subtype;
        std.mem.copyForwards(u8, memory[5..], self.value);
    }

    pub fn size(self: BSONBinary) u32 {
        return 5 + @as(u32, @truncate(self.value.len));
    }

    pub fn read(memory: []const u8) BSONBinary {
        const length = std.mem.bytesToValue(u32, memory[0..4]);
        const subtype = memory[4];
        return BSONBinary{
            .value = memory[5 .. 5 + length],
            .subtype = subtype,
        };
    }
};

test "encodes BSONBinary" {
    var allocator = std.testing.allocator;
    const test_binary = [_]u8{ 0x01, 0x02, 0x03 };
    const test_subtype: u8 = 0x00;
    var binary = BSONBinary{ .value = @constCast(&test_binary), .subtype = test_subtype };
    const actual = allocator.alloc(u8, binary.size()) catch unreachable;
    binary.write(actual);
    defer allocator.free(actual);

    const expected_binary = @constCast(&[_]u8{ 0x03, 0x00, 0x00, 0x00, 0x00, 0x01, 0x02, 0x03 });
    const expected: []u8 = expected_binary[0..];
    try std.testing.expectEqualSlices(u8, expected, actual);
}

test "decodes BSONBinary" {
    const test_memory = @constCast(&[_]u8{ 0x03, 0x00, 0x00, 0x00, 0x00, 0x01, 0x02, 0x03 });
    const expected_binary = [_]u8{ 0x01, 0x02, 0x03 };
    const expected_subtype: u8 = 0x00;
    const binary = BSONBinary.read(test_memory[0..]);
    try std.testing.expectEqualSlices(u8, &expected_binary, binary.value);
    try std.testing.expectEqual(expected_subtype, binary.subtype);
}

pub const BSONBoolean = struct {
    value: bool,

    pub fn write(self: BSONBoolean, memory: []u8) void {
        memory[0] = if (self.value) 0x01 else 0x00;
    }

    pub fn size(_: BSONBoolean) u32 {
        return 1;
    }

    pub fn read(memory: []const u8) BSONBoolean {
        return BSONBoolean{ .value = memory[0] != 0x00 };
    }
};

test "encodes BSONBoolean" {
    var allocator = std.testing.allocator;
    const test_bool = true;
    var boolean = BSONBoolean{ .value = test_bool };
    const actual = allocator.alloc(u8, boolean.size()) catch unreachable;
    boolean.write(actual);
    defer allocator.free(actual);

    const expected_bool = @constCast(&[_]u8{0x01});
    const expected: []u8 = expected_bool[0..];
    try std.testing.expectEqualSlices(u8, expected, actual);
}

test "decodes BSONBoolean" {
    const test_memory = @constCast(&[_]u8{0x01});
    const expected_bool = true;
    const boolean = BSONBoolean.read(test_memory[0..]);
    try std.testing.expectEqual(expected_bool, boolean.value);
}

pub const BSONNull = struct {
    pub fn write(_: BSONNull, _: []u8) void {
        // Null type does not need to write any data
    }

    pub fn size(_: BSONNull) u32 {
        return 0;
    }

    pub fn read(_: []const u8) BSONNull {
        return BSONNull{};
    }
};

test "encodes BSONNull" {
    var allocator = std.testing.allocator;
    var null_value = BSONNull{};
    const actual = allocator.alloc(u8, null_value.size()) catch unreachable;
    null_value.write(actual);
    defer allocator.free(actual);

    const expected_null = @constCast(&[_]u8{});
    const expected: []u8 = expected_null[0..];
    try std.testing.expectEqualSlices(u8, expected, actual);
}

// test "decodes BSONNull" {
//     const test_memory = @constCast(&[_]u8{});
//     // const null_value = BSONNull.read(test_memory[0..]);
// }

pub const BSONValue = union(BSONValueType) {
    double: BSONDouble,
    string: BSONString,
    document: BSONDocument,
    array: BSONDocument,
    binary: BSONBinary,
    objectId: BSONObjectId,
    boolean: BSONBoolean,
    datetime: BSONInt64,
    null: BSONNull,
    int32: BSONInt32,
    int64: BSONInt64,

    pub fn toString() [:0]u8 {}

    pub fn size(self: *BSONValue) u32 {
        return switch (self.*) {
            .string => self.string.size(),
            .double => 8,
            .int32 => 4,
            .document => self.document.len,
            .array => self.array.len,
            .datetime => 8,
            .int64 => 8,
            .binary => self.binary.size(),
            .boolean => 1,
            .null => 0,
            .objectId => 12,
        };
    }

    pub fn asessSize(memory: []const u8, pairType: BSONValueType) u32 {
        return switch (pairType) {
            .string => std.mem.bytesToValue(u32, memory[0..4]),
            .double => 8,
            .int32 => 4,
            .document => std.mem.bytesToValue(u32, memory[0..4]),
            .array => std.mem.bytesToValue(u32, memory[0..4]),
            .datetime => 8,
            .int64 => 8,
            .binary => std.mem.bytesToValue(u32, memory[0..4]),
            .boolean => 1,
            .null => 0,
            .objectId => 12,
        };
    }

    pub fn read(ally: mem.Allocator, memory: []const u8, pairType: BSONValueType) mem.Allocator.Error!BSONValue {
        return switch (pairType) {
            .string => BSONValue{ .string = BSONString.read(memory) },
            .double => BSONValue{ .double = BSONDouble.read(memory) },
            .int32 => BSONValue{ .int32 = BSONInt32.read(memory) },
            .array => BSONValue{ .array = try BSONDocument.read(ally, memory) },
            .document => BSONValue{ .document = try BSONDocument.read(ally, memory) },
            .datetime => BSONValue{ .datetime = BSONInt64.read(memory) },
            .int64 => BSONValue{ .int64 = BSONInt64.read(memory) },
            .binary => BSONValue{ .binary = BSONBinary.read(memory) },
            .boolean => BSONValue{ .boolean = BSONBoolean.read(memory) },
            .null => BSONValue{ .null = BSONNull.read(memory) },
            .objectId => BSONValue{ .objectId = BSONObjectId.read(memory) },
        };
    }
};

const BSONValueType = enum(u8) {
    double = 0x01,
    string = 0x02,
    document = 0x03,
    array = 0x04,
    binary = 0x05,
    // 0x06, // undefined
    objectId = 0x07, // ObjectId
    boolean = 0x08,
    datetime = 0x09, // UTC datetime
    null = 0x0A,
    // 0x0B, // regex
    // 0x0D, // JavaScript
    int32 = 0x10,
    // 0x11, // timestamp
    int64 = 0x12, // 64-bit integer
    // 0xFF, // min key
    // 0x7F, // max key
};

const TypeNamePair = struct {
    type: BSONValueType,
    name: []const u8,
    len: u32,
    pub fn read(bytes: []const u8) ?TypeNamePair {
        if (bytes[0] == 0x00) return null;
        var length: u32 = 0;
        while (bytes[length + 1] != 0) length += 1;
        const name: []const u8 = bytes[1 .. 2 + length];
        const name_size: u32 = @truncate(name.len);

        return TypeNamePair{
            .type = mem.bytesToValue(BSONValueType, bytes[0..1]),
            .name = name,
            .len = name_size + 1,
        };
    }
};

pub const BSONObjectId = struct {
    value: ObjectId,

    pub fn write(self: BSONObjectId, memory: []u8) void {
        std.mem.copyForwards(u8, memory, self.value.buffer[0..]);
    }

    pub fn size(_: BSONObjectId) u32 {
        return 12;
    }

    pub fn read(memory: []const u8) BSONObjectId {
        var buffer: [12:0]u8 = undefined;
        std.mem.copyForwards(u8, buffer[0..], memory[0..12]);
        return BSONObjectId{ .value = ObjectId{ .buffer = buffer } };
    }
};

test "encodes BSONObjectId" {
    var allocator = std.testing.allocator;
    const test_object_id = ObjectId.parseString(("507c7f79bcf86cd7994f6c0e"));
    var object_id = BSONObjectId{ .value = test_object_id };
    const actual = allocator.alloc(u8, object_id.size()) catch unreachable;
    object_id.write(actual);
    defer allocator.free(actual);

    const expected_object_id = (&[_]u8{ 0x50, 0x7c, 0x7f, 0x79, 0xbc, 0xf8, 0x6c, 0xd7, 0x99, 0x4f, 0x6c, 0x0e });
    const expected: []u8 = expected_object_id[0..];
    try std.testing.expectEqualSlices(u8, expected, actual);
}

test "decodes BSONObjectId" {
    const test_memory = (&[_]u8{ 0x50, 0x7c, 0x7f, 0x79, 0xbc, 0xf8, 0x6c, 0xd7, 0x99, 0x4f, 0x6c, 0x0e });
    const expected_object_id = ObjectId.parseString(("507c7f79bcf86cd7994f6c0e"));
    const object_id = BSONObjectId.read(test_memory[0..]);
    try std.testing.expectEqualSlices(u8, expected_object_id.buffer[0..], object_id.value.buffer[0..]);
}

fn readDocument(allocator: mem.Allocator, memory: []const u8) mem.Allocator.Error![]BSONKeyValuePair {
    var idx: u32 = 0;
    var docLen: u32 = 0;
    while (TypeNamePair.read(memory[idx..])) |pair| {
        idx += pair.len;

        const item_memory = memory[idx..];

        idx += BSONValue.asessSize(item_memory, pair.type);

        docLen += 1;
    }
    idx = 0;
    var keyValPairs = try allocator.alloc(BSONKeyValuePair, docLen);
    var i: u32 = 0;
    while (TypeNamePair.read(memory[idx..])) |pair| {
        idx += pair.len;

        // std.debug.print("\n Pair: {x}\n", .{pair.len});

        // std.debug.print("\nString length: {x}\n", .{memory[idx..]});

        const item_memory = memory[idx..];

        var value = try BSONValue.read(
            allocator,
            item_memory,
            pair.type,
        );

        idx += value.size();

        keyValPairs[i] = BSONKeyValuePair{
            .key = pair.name,
            .value = value,
        };
        i += 1;
    }
    return keyValPairs;
}

const BSONKeyValuePair = struct {
    key: []const u8,
    value: BSONValue,
};

const BSONDocument = struct {
    values: []BSONKeyValuePair,
    len: u32,
    ally: mem.Allocator,

    pub fn read(allocator: mem.Allocator, memory: []const u8) mem.Allocator.Error!BSONDocument {
        const length = mem.bytesToValue(u32, memory[0..4]);

        const kvPairs = try readDocument(allocator, memory[4..]);
        return BSONDocument{
            .values = kvPairs,
            .len = length,
            .ally = allocator,
        };
    }

    fn get(self: *@This(), key: []const u8) ?BSONValue {
        for (self.values) |value| {
            if (mem.eql(u8, value.key, key)) {
                return value.value;
            }
        }
        return null;
    }

    pub fn deinit(self: *BSONDocument) void {
        defer self.*.ally.free(self.*.values);
        // std.debug.print("\nvalues pointer: {x}\n", .{&self.values});

        // std.debug.print("\n{any}", .{self.values});

        for (self.*.values) |*item| {
            switch (item.value) {
                inline else => {
                    continue;
                },
                .document => {
                    item.value.document.deinit();
                },
                .array => {
                    item.value.array.deinit();
                },
            }
        }
    }
};

test "BSONDoc read" {
    // std.AutoHashMap([]u8, BSONValue).init(allocator: Allocator)
    const empty_obj = "\x05\x00\x00\x00\x00";

    var doc = try BSONDocument.read(std.testing.allocator, empty_obj);
    defer doc.deinit();

    try std.testing.expect(doc.len == 5);
    try std.testing.expect(doc.values.len == 0);

    var a_is_one_buff: [14:0]u8 = "\x0e\x00\x00\x00\x02\x31\x00\x02\x00\x00\x00\x61\x00\x00".*;
    const a_is_one = a_is_one_buff[0..];

    var doc_a = try BSONDocument.read(std.testing.allocator, a_is_one);
    defer doc_a.deinit();

    try std.testing.expectEqual(1, doc_a.values.len);
    // std.debug.print("{x}", .{doc_a.values});
    // const key_string = "\x31\x00";
    // const key_slice: []u8 = @constCast(key_string);
    // const key: [*]u8 = @ptrCast(key_slice);
    const value = doc_a.get("\x31\x00") orelse unreachable;
    // var iter = doc_a.values.keyIterator();
    // while (iter.next()) |item| {
    //     std.debug.print("{x}", .{item});
    // }
    try std.testing.expectEqualStrings("a", value.string.value);
}

test "BSONDoc embedded documents" {
    const obj = "\x16\x00\x00\x00\x03\x31\x00\x0e\x00\x00\x00\x02\x61\x00\x02\x00\x00\x00\x62\x00\x00\x00";

    var doc = try BSONDocument.read(std.testing.allocator, obj);
    defer doc.deinit();

    var embedded = doc.get("1\x00") orelse unreachable;
    const text = embedded.document.get("a\x00").?.string.value;

    try std.testing.expectEqualStrings("\x62", text);

    // std.debug.print("\n Text: {x} \n", .{text});
}

test "BSONDoc embedded array" {
    const obj = "\x16\x00\x00\x00\x04\x61\x00\x0e\x00\x00\x00\x02\x30\x00\x02\x00\x00\x00\x62\x00\x00\x00";

    var doc = try BSONDocument.read(std.testing.allocator, obj);
    defer doc.deinit();

    var embedded = doc.get("a\x00") orelse unreachable;
    const text = embedded.array.get("0\x00").?.string.value;

    try std.testing.expectEqualStrings("\x62", text);

    // std.debug.print("\n Text: {x} \n", .{text});
}
