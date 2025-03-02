const std = @import("std");
const mem = std.mem;

pub const BSONString = struct {
    // code: i8, // 0x02
    value: []u8,

    pub fn write(self: BSONString, memory: []u8) void {
        // var memory = try allocator.alloc(u8, 4 + self.length);
        const length = @as(u32, @truncate(self.value.len));

        std.mem.copyForwards(u8, memory[0..4], &mem.toBytes(length));
        std.mem.copyForwards(u8, memory[4..], self.value);
    }

    pub fn size(self: BSONString) u32 {
        return 4 + @as(u32, @truncate(self.value.len));
    }

    pub fn read(memory: []u8) BSONString {
        const length = std.mem.bytesToValue(u32, memory[0..4]);
        return BSONString{ .value = memory[4 .. 4 + length] };
    }
};

test "encodes BSONString" {
    var allocator = std.testing.allocator;
    const test_string = "test\x00";
    // const less_const: []const u8 = test_string;
    var string = BSONString{ .value = @constCast(test_string) };
    const actual = allocator.alloc(u8, string.size()) catch unreachable;
    string.write(actual);
    defer allocator.free(actual);

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
    const test_string = "test\x00";
    // const less_const: []const u8 = test_string;
    const string = BSONString.read(test_memory[0..]);
    try std.testing.expectEqualSlices(u8, test_string, string.value);
    try std.testing.expectEqual(5, string.value.len);
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

    pub fn read(memory: []u8) BSONDouble {
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
    const test_memory = @constCast(&[_]u8{ 0x77, 0xbe, 0x9f, 0x1a, 0x2f, 0xdd, 0x5e, 0x40 });
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

    pub fn read(memory: []u8) BSONInt32 {
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

    pub fn read(memory: []u8) BSONInt64 {
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
    value: []u8,
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

    pub fn read(memory: []u8) BSONBinary {
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

    pub fn read(memory: []u8) BSONBoolean {
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
    pub fn write(_: BSONNull, memory: []u8) void {
        // Null type does not need to write any data
    }

    pub fn size(_: BSONNull) u32 {
        return 0;
    }

    pub fn read(_: []u8) BSONNull {
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

test "decodes BSONNull" {
    const test_memory = @constCast(&[_]u8{});
    const null_value = BSONNull.read(test_memory[0..]);
    // No value to compare, just check that it can be read
}

pub const BSONValue = union(BSONValueType) {
    double: BSONDouble,
    string: BSONString,
    document: BSONDocument,
    array: BSONDocument,
    binary: BSONBinary,
    datetime: BSONInt64,
    int32: BSONInt32,
    int64: BSONInt64,
    boolean: BSONBoolean,
    null: BSONNull,
};

const BSONValueType = enum(u8) {
    double = 0x01,
    string = 0x02,
    document = 0x03,
    array = 0x04,
    binary = 0x05,
    // 0x06, // undefined
    // 0x07, // ObjectId
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
    name: []u8,
    len: u32,
    pub fn read(bytes: []u8) ?TypeNamePair {
        if (bytes[0] == 0x00) return null;
        var length: u32 = 0;
        while (bytes[length + 1] != 0) length += 1;
        const name: []u8 = bytes[1 .. 2 + length];
        const name_size: u32 = @truncate(name.len);

        return TypeNamePair{
            .type = mem.bytesToValue(BSONValueType, bytes[0..1]),
            .name = name,
            .len = name_size + 1,
        };
    }
};

const BSONDocument = struct {
    values: std.StringHashMap(BSONValue),
    len: u32,

    pub fn read(allocator: mem.Allocator, memory: []u8) !BSONDocument {
        var map = std.StringHashMap(BSONValue).init(allocator);
        const length = mem.bytesToValue(u32, memory[0..4]);
        var idx: u32 = 4;
        while (TypeNamePair.read(memory[idx..])) |pair| {
            idx += pair.len;

            // std.debug.print("\n Pair: {x}\n", .{pair.len});

            // std.debug.print("\nString length: {x}\n", .{memory[idx..]});

            const item_memory = memory[idx..];

            const value = switch (pair.type) {
                .string => BSONValue{ .string = BSONString.read(item_memory) },
                .double => BSONValue{ .double = BSONDouble.read(item_memory) },
                .int32 => BSONValue{ .int32 = BSONInt32.read(item_memory) },
                .array => BSONValue{ .array = try BSONDocument.read(allocator, item_memory) },
                .document => BSONValue{ .document = try BSONDocument.read(allocator, item_memory) },
                .datetime => BSONValue{ .datetime = BSONInt64.read(item_memory) },
                .int64 => BSONValue{ .int64 = BSONInt64.read(item_memory) },
                .binary => BSONValue{ .binary = BSONBinary.read(item_memory) },
                .boolean => BSONValue{ .boolean = BSONBoolean.read(item_memory) },
                .null => BSONValue{ .null = BSONNull.read(item_memory) },
                // else => unreachable,
            };

            const inc: u32 = switch (value) {
                .string => value.string.size(),
                .double => 8,
                .int32 => 4,
                .document => value.document.len,
                .array => value.array.len,
                .datetime => 8,
                .int64 => 8,
                .binary => value.binary.size(),
                .boolean => 1,
                .null => 0,
                // else => unreachable,
            };

            idx += inc;

            try map.put(pair.name, value);
        }
        return BSONDocument{
            .values = map,
            .len = length,
        };
    }

    pub fn deinit(self: *BSONDocument) void {
        defer self.values.deinit();
        const scaner = std.json.Scanner.initStreaming(std.testing.allocator);
        scaner.
        var iter = self.values.valueIterator();
        while (iter.next()) |item| {
            switch (item.*) {
                inline else => {
                    continue;
                },
                .document => {
                    item.*.document.deinit();
                },
                .array => {
                    item.*.array.deinit();
                },
            }
        }
    }
};

test "BSONDoc read" {
    // std.AutoHashMap([]u8, BSONValue).init(allocator: Allocator)
    const empty_obj = "\x05\x00\x00\x00\x00";

    var doc = try BSONDocument.read(std.testing.allocator, @constCast(empty_obj));
    defer doc.deinit();

    try std.testing.expect(doc.len == 5);
    try std.testing.expect(doc.values.count() == 0);

    const a_is_one = "\x0e\x00\x00\x00\x02\x31\x00\x02\x00\x00\x00\x61\x00\x00";

    var doc_a = try BSONDocument.read(std.testing.allocator, @constCast(a_is_one));
    defer doc_a.deinit();

    try std.testing.expectEqual(1, doc_a.values.count());
    // std.debug.print("{x}", .{doc_a.values});
    // const key_string = "\x31\x00";
    // const key_slice: []u8 = @constCast(key_string);
    // const key: [*]u8 = @ptrCast(key_slice);
    const value = doc_a.values.get("\x31\x00") orelse unreachable;
    // var iter = doc_a.values.keyIterator();
    // while (iter.next()) |item| {
    //     std.debug.print("{x}", .{item});
    // }
    try std.testing.expectEqualStrings("a\x00", value.string.value);
}

test "BSONDoc embedded documents" {
    const obj = "\x16\x00\x00\x00\x03\x31\x00\x0e\x00\x00\x00\x02\x61\x00\x02\x00\x00\x00\x62\x00\x00\x00";

    var doc = try BSONDocument.read(std.testing.allocator, @constCast(obj));
    defer doc.deinit();

    var embedded = doc.values.get("1\x00") orelse unreachable;
    const text = embedded.document.values.get("a\x00").?.string.value;

    try std.testing.expectEqualStrings("\x62\x00", text);

    // std.debug.print("\n Text: {x} \n", .{text});
}

test "BSONDoc embedded array" {
    const obj = "\x16\x00\x00\x00\x04\x61\x00\x0e\x00\x00\x00\x02\x30\x00\x02\x00\x00\x00\x62\x00\x00\x00";

    var doc = try BSONDocument.read(std.testing.allocator, @constCast(obj));
    defer doc.deinit();

    var embedded = doc.values.get("a\x00") orelse unreachable;
    const text = embedded.array.values.get("0\x00").?.string.value;

    try std.testing.expectEqualStrings("\x62\x00", text);
    // std.debug.print("\n Text: {x} \n", .{text});
}
