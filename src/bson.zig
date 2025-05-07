const std = @import("std");
const mem = std.mem;
pub const ObjectId = @import("./object_id.zig").ObjectId;

pub const BSONString = struct {
    // code: i8, // 0x02
    value: []const u8,

    pub fn write(self: BSONString, memory: []u8) void {
        // var memory = try allocator.alloc(u8, 4 + self.length);
        const length = @as(u32, @truncate(self.value.len)) + 1;

        std.mem.writeInt(u32, memory[0..4], length, .little);
        // std.mem.copyForwards(u8, memory[4 .. length + 3], self.value);
        @memcpy(memory[4 .. 4 + length - 1], self.value);
        memory[length + 3] = 0x00;
    }

    pub fn size(self: *const BSONString) u32 {
        return 4 + @as(u32, @truncate(self.value.len)) + 1;
    }

    pub fn read(memory: []const u8) BSONString {
        const length = std.mem.bytesToValue(u32, memory[0..4]);
        return BSONString{ .value = memory[4 .. 4 + length - 1] };
    }
};

test "encodes BSONString" {
    var allocator = std.testing.allocator;
    const test_string: *const [4:0]u8 = "test";
    // const less_const: []const u8 = test_string;
    var string = BSONString{ .value = test_string };
    const actual = allocator.alloc(u8, string.size()) catch unreachable;
    defer allocator.free(actual);

    string.write(actual);

    // for (actual) |value| {
    //     std.debug.print("{x} ", .{value});
    // }
    // std.debug.print("{x}", .{actual});

    try std.testing.expectEqualSlices(u8, "\x05\x00\x00\x00test\x00", actual);
}

test "decodes BSONString" {
    // var allocator = std.testing.allocator;
    const test_memory = @constCast(&[_]u8{ 5, 0, 0, 0, 0x74, 0x65, 0x73, 0x74, 0 });
    const test_string: *const [4]u8 = "test";
    // const less_const: []const u8 = test_string;
    const string = BSONString.read(test_memory[0..]);
    try std.testing.expectEqualSlices(u8, test_string, string.value);
    try std.testing.expectEqual(4, string.value.len);
    try std.testing.expectEqual(9, string.size());
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

        std.mem.writeInt(i32, memory[0..4], self.value, .little);
    }

    pub fn size(_: BSONInt32) u32 {
        return 4;
    }

    pub inline fn read(memory: []const u8) BSONInt32 {
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

pub const BSONDatetime = struct {
    // code: i8, // 0x02
    value: u64,

    pub fn write(self: BSONDatetime, memory: []u8) void {
        // var memory = try allocator.alloc(u8, 4 + self.length);

        std.mem.copyForwards(u8, memory, &mem.toBytes(self.value));
    }

    pub fn size(_: BSONDatetime) u32 {
        return 8;
    }

    pub fn read(memory: []const u8) BSONDatetime {
        return BSONDatetime{
            .value = std.mem.bytesToValue(u64, memory),
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
    datetime: BSONDatetime,
    null: BSONNull,
    int32: BSONInt32,
    int64: BSONInt64,

    pub fn init(value: anytype) BSONValue {
        return switch (@TypeOf(value)) {
            i32 => BSONValue{ .int32 = BSONInt32{ .value = value } },
            f64 => BSONValue{ .double = BSONDouble{ .value = value } },
            []u8 => BSONValue{ .string = BSONString{ .value = value } },
            BSONDocument => BSONValue{ .document = value },
            BSONBinary => BSONValue{ .binary = BSONBinary{ .value = value, .subtype = 1 } },
            ObjectId => BSONValue{ .objectId = .{ .value = value } },
            else => |unsupportedType| @compileError(std.fmt.comptimePrint("Unsupported BSONValue type: {any}", .{unsupportedType})),
        };
    }

    pub fn toString() [:0]u8 {}

    pub inline fn size(self: *const BSONValue) u32 {
        return switch (self.*) {
            .string => self.string.size(),
            .double => 8,
            .int32 => 4,
            .document, .array => |sizable| @truncate(sizable.buffer.len),
            .datetime => 8,
            .int64 => 8,
            .binary => self.binary.size(),
            .boolean => 1,
            .null => 0,
            .objectId => 12,
        };
    }

    pub inline fn valueType(self: *const BSONValue) BSONValueType {
        return switch (self.*) {
            .string => .string,
            .double => .double,
            .int32 => .int32,
            .document => .document,
            .array => .array,
            .datetime => .datetime,
            .int64 => .int64,
            .binary => .binary,
            .boolean => .boolean,
            .null => .null,
            .objectId => .objectId,
        };
    }

    pub inline fn valueOrder(self: *const BSONValue) u8 {
        return switch (self.*) {
            .null => 1,
            .double, .int32, .int64 => 2,
            .string => 3,
            .document => 4,
            .array => 5,
            .binary => 6,
            .objectId => 7,
            .boolean => 8,
            .datetime => 9,
        };
    }

    pub inline fn asessSize(memory: []const u8, pairType: BSONValueType) u32 {
        return switch (pairType) {
            .document, .array => std.mem.bytesToValue(u32, memory[0..4]),
            .string => 4 + std.mem.bytesToValue(u32, memory[0..4]),
            .binary => 5 + std.mem.bytesToValue(u32, memory[0..4]),
            .double, .datetime, .int64 => 8,
            .int32 => 4,
            .boolean => 1,
            .null => 0,
            .objectId => 12,
        };
    }

    pub fn eql(self: *const BSONValue, other: *const BSONValue) bool {
        if (self.* == BSONValueType.array) {
            @branchHint(.unlikely);
            if (other.* == BSONValueType.array) {
                if (self.array.buffer.len != other.array.buffer.len) return false;

                return std.mem.eql(u8, self.array.buffer, other.array.buffer);
            } else switch (other.*) {
                .document, .array => {
                    return false;
                },

                else => {
                    var idx: u32 = 4;
                    while (TypeNamePair.read(self.array.buffer[idx..])) |pair| {
                        idx += pair.len;
                        if (pair.type == .array or pair.type == .document) {
                            idx += BSONValue.asessSize(self.array.buffer[idx..], pair.type);
                            continue;
                        }
                        const innerValue = BSONValue.read(self.array.buffer[idx..], pair.type);
                        idx += BSONValue.asessSize(self.array.buffer[idx..], pair.type);
                        if (eql(&innerValue, other)) {
                            return true;
                        }
                    }
                    return false;
                },
            }
        }

        if (std.meta.activeTag(self.*) != std.meta.activeTag(other.*)) {
            return false;
        }

        return switch (self.*) {
            .string => std.mem.eql(u8, self.string.value, other.string.value),
            .int32 => |int| int.value == other.int32.value,
            .int64 => self.int64.value == other.int64.value,
            .double => self.double.value == other.double.value,
            .document => false,
            .datetime => self.datetime.value == other.datetime.value,
            .binary => std.mem.eql(u8, self.binary.value, other.binary.value),
            .boolean => self.boolean.value == other.boolean.value,
            .null => true,
            .objectId => |objid| std.mem.eql(u8, &objid.value.buffer, &other.objectId.value.buffer),
            else => false,
        };
    }

    pub fn order(self: BSONValue, other: BSONValue) std.math.Order {
        const a_val = self.valueOrder();
        const b_val = other.valueOrder();
        if (a_val != b_val) {
            return std.math.order(a_val, b_val);
        }

        return switch (self) {
            .string => std.mem.order(u8, self.string.value, other.string.value),
            .int32, .int64, .double => orderNums(self, other),
            .document => .eq,
            .array => .eq,
            .datetime => std.math.order(self.datetime.value, other.datetime.value),
            .binary => .eq,
            .boolean => .eq,
            .null => .eq,
            .objectId => std.mem.order(u8, &self.objectId.value.buffer, &other.objectId.value.buffer),
        };
    }

    fn orderNums(a: BSONValue, b: BSONValue) std.math.Order {
        return switch (a) {
            .double => {
                return switch (b) {
                    .double => std.math.order(a.double.value, b.double.value),
                    .int32 => std.math.order(a.double.value, @as(f64, @floatFromInt(b.int32.value))),
                    .int64 => std.math.order(a.double.value, @as(f64, @floatFromInt(b.int64.value))),
                    else => unreachable,
                };
            },
            .int32 => {
                return switch (b) {
                    .double => std.math.order(@as(f64, @floatFromInt(a.int32.value)), b.double.value),
                    .int32 => std.math.order(a.int32.value, b.int32.value),
                    .int64 => std.math.order(@as(i64, a.int32.value), b.int64.value),
                    else => unreachable,
                };
            },
            .int64 => {
                return switch (b) {
                    .double => std.math.order(@as(f64, @floatFromInt(a.int64.value)), b.double.value),
                    .int32 => std.math.order(a.int64.value, @as(i64, b.int32.value)),
                    .int64 => std.math.order(a.int64.value, b.int64.value),
                    else => unreachable,
                };
            },
            else => unreachable,
        };
    }

    pub fn write(self: *const BSONValue, writer: anytype) !void {
        switch (self.*) {
            .string => {
                try writer.writeInt(u32, @as(u32, @truncate(self.string.value.len)) + 1, .little);
                try writer.writeAll(self.string.value);
                try writer.writeByte(0);
            },
            .double => {
                try writer.writeAll(&mem.toBytes(self.double.value));
            },
            .int32 => {
                try writer.writeInt(i32, self.int32.value, .little);
            },
            .array, .document => |writable| {
                try writer.writeAll(writable.buffer);
            },
            .datetime => {
                try writer.writeInt(u64, self.datetime.value, .little);
            },
            .int64 => {
                try writer.writeInt(i64, self.int64.value, .little);
            },
            .binary => {
                try writer.writeInt(u32, @as(u32, @truncate(self.binary.value.len)) + 5, .little);
                try writer.writeByte(self.binary.subtype);
                try writer.writeAll(self.binary.value);
            },
            .boolean => {
                try writer.writeByte(if (self.boolean.value) 0x01 else 0x00);
            },
            .null => {
                // Null type does not need to write any data
            },
            .objectId => {
                try writer.writeAll(&self.objectId.value.buffer);
            },
        }
    }

    pub inline fn read(memory: []const u8, pairType: BSONValueType) BSONValue {
        return switch (pairType) {
            .string => BSONValue{ .string = BSONString.read(memory) },
            .double => BSONValue{ .double = BSONDouble.read(memory) },
            .int32 => BSONValue{ .int32 = .{ .value = std.mem.bytesToValue(i32, memory[0..4]) } },
            .array => BSONValue{ .array = blk: {
                const length = std.mem.bytesToValue(u32, memory[0..4]);
                break :blk BSONDocument.init(memory[0..length]);
            } },
            .document => BSONValue{ .document = blk: {
                const length = std.mem.bytesToValue(u32, memory[0..4]);
                break :blk BSONDocument.init(memory[0..length]);
            } },
            .datetime => BSONValue{ .datetime = BSONDatetime.read(memory) },
            .int64 => BSONValue{ .int64 = BSONInt64.read(memory) },
            .binary => BSONValue{ .binary = BSONBinary.read(memory) },
            .boolean => BSONValue{ .boolean = BSONBoolean.read(memory) },
            .null => BSONValue{ .null = BSONNull.read(memory) },
            .objectId => BSONValue{ .objectId = BSONObjectId.read(memory) },
        };
    }
};

test "BSONValue.eql array -> int32" {
    const array = try BSONDocument.fromTuple(std.testing.allocator, .{
        .@"0" = BSONValue{ .int32 = .{ .value = 123 } },
        .@"1" = BSONValue{ .int32 = .{ .value = 123 } },
    });
    const doc = BSONValue{ .array = array };
    defer array.deinit(std.testing.allocator);
    const other = BSONValue{ .int32 = BSONInt32{ .value = 123 } };
    const otherfalse = BSONValue{ .int32 = BSONInt32{ .value = 321 } };
    try std.testing.expectEqual(true, doc.eql(&other));

    try std.testing.expectEqual(false, doc.eql(&otherfalse));
}

test "BSONValue.eql int32" {
    const double = BSONValue{ .int32 = BSONInt32{ .value = 123 } };
    const other = BSONValue{ .int32 = BSONInt32{ .value = 123 } };
    try std.testing.expectEqual(true, double.eql(&other));
}

pub const BSONValueType = enum(u8) {
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
        const name: []const u8 = bytes[1..(1 + length)];
        const name_size: u32 = @truncate(name.len + 1);

        return TypeNamePair{
            .type = @enumFromInt(bytes[0]),
            .name = name,
            .len = name_size + 1,
        };
    }
};

pub const BSONObjectId = struct {
    value: ObjectId,

    pub fn write(self: BSONObjectId, memory: []u8) void {
        @memcpy(memory[0..12], &self.value.buffer);
    }

    pub fn read(memory: []const u8) BSONObjectId {
        var buffer: [12:0]u8 = undefined;
        @memcpy(buffer[0..], memory[0..12]);
        return BSONObjectId{ .value = ObjectId{ .buffer = buffer } };
    }
};

test "encodes BSONObjectId" {
    var allocator = std.testing.allocator;
    const test_object_id = try ObjectId.parseString(("507c7f79bcf86cd7994f6c0e"));
    var object_id = BSONObjectId{ .value = test_object_id };
    const actual = allocator.alloc(u8, 12) catch unreachable;
    object_id.write(actual);
    defer allocator.free(actual);

    const expected_object_id = (&[_]u8{ 0x50, 0x7c, 0x7f, 0x79, 0xbc, 0xf8, 0x6c, 0xd7, 0x99, 0x4f, 0x6c, 0x0e });
    try std.testing.expectEqualSlices(u8, expected_object_id, actual);
}

test "decodes BSONObjectId" {
    const test_memory = (&[_]u8{ 0x50, 0x7c, 0x7f, 0x79, 0xbc, 0xf8, 0x6c, 0xd7, 0x99, 0x4f, 0x6c, 0x0e });
    const expected_object_id = try ObjectId.parseString(("507c7f79bcf86cd7994f6c0e"));
    const object_id = BSONObjectId.read(test_memory[0..]);
    try std.testing.expectEqualSlices(u8, expected_object_id.buffer[0..], object_id.value.buffer[0..]);
}

pub const BSONKeyValuePair = struct {
    key: []const u8,
    value: BSONValue,
    pub fn init(key: []const u8, value: BSONValue) BSONKeyValuePair {
        return BSONKeyValuePair{
            .key = key,
            .value = value,
        };
    }
};

test "from json" {
    const json = "{ \"key\": {\"key2\": \"val\"} }";
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    const allocator = arena.allocator();
    const doc = try BSONDocument.fromJSON(allocator, json);
    defer arena.deinit();

    try std.testing.expectEqualStrings("val", doc.getPath("key.key2").?.string.value);
}

pub const BSONDocument = struct {
    // values: []BSONKeyValuePair,
    buffer: []const u8,

    pub fn init(buffer: []const u8) BSONDocument {
        return BSONDocument{
            .buffer = buffer,
        };
    }

    pub fn initEmpty() BSONDocument {
        return BSONDocument{
            .buffer = @constCast("\x05\x00\x00\x00\x00"),
        };
    }

    pub fn fromJSON(ally: mem.Allocator, json: []const u8) !BSONDocument {
        var jsonStream = std.json.Scanner.initCompleteInput(ally, json);
        defer jsonStream.deinit();

        return .{ .buffer = try jsonToDoc(&jsonStream, ally) };
    }

    pub fn fromTuple(ally: mem.Allocator, comptime tuple: anytype) !BSONDocument {
        // asses doc size
        const T = @TypeOf(tuple);
        const fields = @typeInfo(T).@"struct".fields;
        comptime var size: u32 = 4; // Placeholder for length
        inline for (fields) |field| {
            const value: BSONValue = @field(tuple, field.name);
            size += comptime value.size() + field.name.len + 2;
        }
        size += 1; // Null terminator for the document
        const buffer = try ally.alloc(u8, size);
        std.mem.writeInt(u32, buffer[0..4], size, .little);
        var idx: u32 = 4;
        inline for (fields) |field| {
            const value: BSONValue = @field(tuple, field.name);
            buffer[idx] = @intFromEnum(value.valueType());
            idx += 1;
            std.mem.copyForwards(u8, buffer[idx..], field.name);
            idx += field.name.len;
            buffer[idx] = 0x00;
            idx += 1;
            // std.mem.copyForwards(u8, buffer[idx..], &value);
            switch (value) {
                .string => |s| s.write(buffer[idx..]),
                .double => |s| s.write(buffer[idx..]),
                .int32 => |s| s.write(buffer[idx..]),
                .int64 => |s| s.write(buffer[idx..]),
                .datetime => |s| s.write(buffer[idx..]),
                .binary => |s| s.write(buffer[idx..]),
                .boolean => buffer[idx] = if (value.boolean.value) 0x01 else 0x00,
                .null => {},
                else => unreachable,
            }
            idx += value.size();
        }
        buffer[idx] = 0x00; // Null terminator for the document

        // std.debug.print("{any}", .{buffer});
        return BSONDocument{ .buffer = buffer };
    }

    fn jsonToDoc(json: *std.json.Scanner, allocator: mem.Allocator) ![]u8 {
        var pairs = std.ArrayList(u8).init(allocator);
        var writer = pairs.writer();
        try writer.writeInt(u32, 0, .little); // Placeholder for length
        if (try json.next() != .object_begin) {
            return error.InvalidJSON;
        }
        var token = try json.nextAlloc(allocator, .alloc_always);
        while (token != .object_end) {
            if (token != .allocated_string) {
                return error.InvalidJSON;
            }
            const key = token.allocated_string;
            const nextType = try json.peekNextTokenType();
            if (nextType == .object_end) {
                break;
            }

            switch (nextType) {
                .number => {
                    token = try json.nextAlloc(allocator, .alloc_always);
                    if (std.mem.indexOf(u8, token.allocated_number, ".") != null) {
                        const value = try std.fmt.parseFloat(f64, token.allocated_number);
                        try writer.writeByte(@intFromEnum(BSONValueType.double));
                        try writer.writeAll(key);
                        try writer.writeByte(0);
                        try writer.writeAll(&mem.toBytes(value));
                        allocator.free(token.allocated_number);
                    } else {
                        const value = try std.fmt.parseInt(i32, token.allocated_number, 10);
                        try writer.writeByte(@intFromEnum(BSONValueType.int32));
                        try writer.writeAll(key);
                        try writer.writeByte(0);
                        try writer.writeInt(i32, value, .little);
                        allocator.free(token.allocated_number);
                    }
                },
                .string => {
                    token = try json.nextAlloc(allocator, .alloc_always);

                    try writer.writeByte(@intFromEnum(BSONValueType.string));
                    try writer.writeAll(key);
                    try writer.writeByte(0);
                    try writer.writeInt(u32, @as(u32, @truncate(token.allocated_string.len)) + 1, .little);
                    try writer.writeAll(token.allocated_string);
                    try writer.writeByte(0);
                    allocator.free(token.allocated_string);
                },
                .true, .false => |tok| {
                    token = try json.nextAlloc(allocator, .alloc_always);
                    try writer.writeByte(@intFromEnum(BSONValueType.boolean));
                    try writer.writeAll(key);
                    try writer.writeByte(0);
                    try writer.writeByte(if (tok == .true) 0x01 else 0x00);
                },
                .null => {
                    token = try json.nextAlloc(allocator, .alloc_always);
                    try writer.writeByte(@intFromEnum(BSONValueType.null));
                    try writer.writeAll(key);
                    try writer.writeByte(0);
                },
                .object_begin => {
                    // token = try json.nextAlloc(allocator, .alloc_always);
                    const value = try jsonToDoc(json, allocator);
                    try writer.writeByte(@intFromEnum(BSONValueType.document));
                    try writer.writeAll(key);
                    try writer.writeByte(0);
                    try writer.writeAll(value);
                    allocator.free(value);
                },
                else => {
                    return error.InvalidJSON;
                },
            }
            allocator.free(key);
            token = try json.nextAlloc(allocator, .alloc_always);
        }
        std.mem.writeInt(u32, pairs.items[0..4], @truncate(pairs.items.len + 1), .little);
        try writer.writeByte(0); // Null terminator for the document
        return pairs.toOwnedSlice();
    }

    const PairIterator = struct {
        doc: *const BSONDocument,
        idx: usize,

        pub fn next(self: *PairIterator) ?BSONKeyValuePair {
            const pair: TypeNamePair = TypeNamePair.read(self.doc.buffer[self.idx..]) orelse return null;
            self.idx += pair.len;
            const pairSize = BSONValue.asessSize(self.doc.buffer[self.idx..], pair.type);
            defer {
                self.idx += pairSize;
            }
            return BSONKeyValuePair{
                .key = pair.name,
                .value = BSONValue.read(self.doc.buffer[self.idx..], pair.type),
            };
        }
    };

    pub fn iter(self: *const BSONDocument) PairIterator {
        return PairIterator{
            .doc = self,
            .idx = 4,
        };
    }

    pub fn fromPairs(allocator: mem.Allocator, pairs: []BSONKeyValuePair) !BSONDocument {
        var list = std.ArrayList(u8).init(allocator);
        const writer = list.writer();
        try writer.writeInt(u32, 0, .little);

        for (pairs) |pair| {
            try writer.writeByte(@intFromEnum(pair.value.valueType()));
            try writer.writeAll(pair.key);
            try writer.writeByte(0);
            try pair.value.write(writer);
        }
        try writer.writeByte(0); // Null terminator for the document

        std.mem.writeInt(u32, list.items[0..4], @as(u32, @truncate(list.items.len)), .little);

        return BSONDocument{ .buffer = try list.toOwnedSlice() };
    }

    pub fn write(self: *const BSONDocument, writer: anytype) !void {
        try writer.writeAll(self.buffer);
    }

    pub fn serializeToMemory(self: *const BSONDocument, memory: []u8) void {
        std.mem.copyForwards(u8, memory, self.buffer);
    }

    pub fn keyNumber(self: *const BSONDocument) u32 {
        var idx: usize = 4;
        var count: u32 = 0;
        while (TypeNamePair.read(self.buffer[idx..])) |pair| : (count += 1) {
            idx += pair.len + BSONValue.asessSize(self.buffer[(idx + pair.len)..], pair.type);
        }
        return count;
    }

    fn fastMemEql(a: []const u8, b: []const u8) bool {

        // Process 16 bytes at a time using vectors
        const Vec = @Vector(16, u8);
        var i: usize = 0;
        while (i + 16 <= a.len) : (i += 16) {
            const va = @as(Vec, @bitCast(a[i..][0..16].*));
            const vb = @as(Vec, @bitCast(b[i..][0..16].*));
            if (!@reduce(.And, va == vb)) return false;
        }

        // Handle remaining bytes
        while (i < a.len) : (i += 1) {
            if (a[i] != b[i]) return false;
        }
        return true;
    }

    /// Returns the value at the key in the document.
    /// Returns null if the key is not found.
    pub fn get(self: BSONDocument, key: []const u8) ?BSONValue {
        var idx: usize = 4;
        const key_len = key.len;
        const buffer = self.buffer;
        while (buffer[idx] != 0) {
            const typeByte: BSONValueType = @enumFromInt(buffer[idx]);
            const namePtr: [*:0]const u8 = @ptrCast(buffer.ptr + idx + 1);
            const name = std.mem.span(namePtr);
            idx += 1 + name.len + 1; // Move past type, name, and null terminator
            // Quick length check before any other operations

            if (name.len == key_len and name[0] == key[0]) {
                // First char check before full comparison
                if (fastMemEql(name, key)) {
                    return BSONValue.read(self.buffer[idx..], typeByte);
                }
            }
            const size = BSONValue.asessSize(self.buffer[idx..], typeByte);
            idx += size;
        }
        return null;
    }

    /// Triest to get the value at the path in the raw BSON data.
    /// Returns null if the path is not found.
    /// Path is a string of keys separated by dots.
    /// Example: "key1.key2.key3"
    pub fn getPath(self: *const BSONDocument, path: []const u8) ?BSONValue {
        return self.getPathRecursive(path);
    }

    fn getPathRecursive(self: BSONDocument, path: []const u8) ?BSONValue {
        const dotIdx = std.mem.indexOfScalar(u8, path, '.');
        const currentKey = if (dotIdx) |idx| path[0..idx] else path;

        const value = self.get(currentKey) orelse return null;

        if (dotIdx == null) return value;

        return switch (value) {
            .document, .array => |doc| doc.getPathRecursive(path[dotIdx.? + 1 ..]),
            else => null,
        };
    }

    test "Document.getPath single value" {
        const ally = std.testing.allocator;
        var doc = try BSONDocument.fromTuple(ally, .{
            .key = BSONValue{ .string = .{ .value = "test" } },
        });

        defer doc.deinit(ally);

        const value = doc.getPath("key") orelse unreachable;
        try std.testing.expectEqualStrings("test", value.string.value);
    }

    pub fn unset(self: *const BSONDocument, allocator: mem.Allocator, key: []const u8) !BSONDocument {
        var idx: usize = 4;
        var newSize: u32 = 4; // Start with the size placeholder

        // First pass: Assess the new size
        while (TypeNamePair.read(self.buffer[idx..])) |pair| {
            const valueSize = BSONValue.asessSize(self.buffer[idx + pair.len ..], pair.type);

            if (!std.mem.eql(u8, pair.name, key)) {
                newSize += 1 + @as(u32, @truncate(pair.name.len)) + 1 + valueSize; // type + key + null terminator + value
            }

            idx += pair.len + valueSize;
        }

        newSize += 1; // Add null terminator

        // Allocate new buffer
        const newBuffer = try allocator.alloc(u8, newSize);

        // Second pass: Write the new document
        idx = 4;
        var writeIdx: usize = 4;
        std.mem.writeInt(u32, newBuffer[0..4], newSize, .little);

        while (TypeNamePair.read(self.buffer[idx..])) |pair| {
            const valueSize = BSONValue.asessSize(self.buffer[idx + pair.len ..], pair.type);

            if (!std.mem.eql(u8, pair.name, key)) {
                newBuffer[writeIdx] = @intFromEnum(pair.type);
                writeIdx += 1;

                @memcpy(newBuffer[writeIdx .. writeIdx + pair.name.len], pair.name);
                writeIdx += pair.name.len;

                newBuffer[writeIdx] = 0; // Null terminator for key
                writeIdx += 1;

                @memcpy(newBuffer[writeIdx .. writeIdx + valueSize], self.buffer[idx + pair.len .. idx + pair.len + valueSize]);
                writeIdx += valueSize;
            }

            idx += pair.len + valueSize;
        }

        newBuffer[writeIdx] = 0; // Null terminator for document

        // Return the new document
        return BSONDocument.init(newBuffer);
    }

    pub fn set(self: BSONDocument, allocator: mem.Allocator, key: []const u8, value: BSONValue) !BSONDocument {
        var doc = self;
        var idx: usize = 4;
        var needsUnset = false;
        while (TypeNamePair.read(self.buffer[idx..])) |pair| {
            if (mem.eql(u8, pair.name, key)) {
                needsUnset = true;
                break;
            }

            idx += pair.len + BSONValue.asessSize(self.buffer[idx + pair.len ..], pair.type);
        }

        if (needsUnset) {
            // Use temporary variable to avoid reassigning self
            doc = try self.unset(allocator, key);
        }
        defer {
            if (needsUnset) {
                allocator.free(doc.buffer);
            }
        }

        const newSize = doc.buffer.len + 1 + key.len + 1 + value.size();

        const newBuffer = try allocator.alloc(u8, newSize);
        @memset(newBuffer, 0); // TODO: Remove
        // Copy the existing data to the new buffer
        std.mem.writeInt(u32, newBuffer[0..4], @truncate(newSize), .little);
        @memcpy(newBuffer[4 .. doc.buffer.len - 1], doc.buffer[4 .. doc.buffer.len - 1]);

        var writeIdx: usize = doc.buffer.len - 1;
        newBuffer[writeIdx] = @intFromEnum(value.valueType());
        @memcpy(newBuffer[writeIdx + 1 .. writeIdx + key.len + 1], key);
        newBuffer[writeIdx + key.len + 1] = 0; // Null terminator for key
        writeIdx += key.len + 2;
        switch (value) {
            .string => {
                value.string.write(newBuffer[writeIdx..]);
            },
            .double => {
                value.double.write(newBuffer[writeIdx..]);
            },
            .int32 => {
                value.int32.write(newBuffer[writeIdx..]);
            },
            .document, .array => |writable| {
                @memcpy(newBuffer[writeIdx .. writeIdx + writable.buffer.len], writable.buffer);
            },
            .datetime => {
                value.datetime.write(newBuffer[writeIdx..]);
            },
            .int64 => {
                value.int64.write(newBuffer[writeIdx..]);
            },
            .binary => {
                value.binary.write(newBuffer[writeIdx..]);
            },
            .boolean => {
                newBuffer[writeIdx] = if (value.boolean.value) 0x01 else 0x00;
            },
            .null => {},
            .objectId => {
                @memcpy(newBuffer[writeIdx .. writeIdx + 12], value.objectId.value.buffer[0..]);
            },
        }
        writeIdx += value.size();
        newBuffer[writeIdx] = 0; // Null terminator for document
        return BSONDocument.init(newBuffer);
    }

    pub fn deinit(self: *const BSONDocument, allocator: mem.Allocator) void {
        defer allocator.free(self.buffer);
    }
};

test "BSONDocument write" {
    const allocator = std.testing.allocator;
    var list = std.ArrayList(u8).init(allocator);
    defer list.deinit();
    var doc = BSONDocument.initEmpty();
    doc = try doc.set(allocator, "test", .{ .string = .{ .value = "test" } });
    defer doc.deinit(allocator);
    // const actual = allocator.alloc(u8, 100) catch unreachable;

    try doc.write(list.writer());
    const expected_string = ("\x14\x00\x00\x00\x02test\x00\x05\x00\x00\x00test\x00\x00");
    try std.testing.expectEqualSlices(u8, expected_string, list.allocatedSlice()[0..doc.buffer.len]);
}

test "BSONDocument serializeToMemory" {
    var allocator = std.testing.allocator;
    var doc = try BSONDocument.fromTuple(allocator, .{
        .key = BSONValue{ .string = .{ .value = "test" } },
    });
    defer doc.deinit(allocator);
    // std.debug.print("doc len: {d}", .{doc.buffer.len});
    const actual = allocator.alloc(u8, 100) catch unreachable;

    doc.serializeToMemory(actual);
    defer allocator.free(actual);
    const expected_string = ("\x13\x00\x00\x00\x02key\x00\x05\x00\x00\x00test\x00\x00");
    try std.testing.expectEqualSlices(u8, expected_string, actual[0..doc.buffer.len]);
}

test "BSONDoc read" {
    // std.AutoHashMap([]u8, BSONValue).init(allocator: Allocator)

    const a_is_one_buff = "\x0e\x00\x00\x00\x02\x31\x00\x02\x00\x00\x00\x61\x00\x00";

    var doc_a = BSONDocument.init(a_is_one_buff[0..]);

    const value = doc_a.get("\x31") orelse unreachable;
    try std.testing.expectEqualStrings("a", value.string.value);
}

test "BSONDoc embedded documents" {
    const obj = "\x16\x00\x00\x00\x03\x31\x00\x0e\x00\x00\x00\x02\x61\x00\x02\x00\x00\x00\x62\x00\x00\x00";

    var doc = BSONDocument.init(obj);

    const embedded = doc.getPath("1.a") orelse unreachable;
    const text = embedded.string.value;

    try std.testing.expectEqualStrings("\x62", text);

    // std.debug.print("\n Text: {x} \n", .{text});
}

test "BSONDoc set docs" {
    const obj = "\x16\x00\x00\x00\x03\x31\x00\x0e\x00\x00\x00\x02\x61\x00\x02\x00\x00\x00\x62\x00\x00\x00";

    var doc = BSONDocument.init(obj);

    const objid = ObjectId.init();

    doc = try doc.set(std.testing.allocator, "_id", .{ .objectId = .{ .value = objid } });
    defer doc.deinit(std.testing.allocator);
    const text = doc.get("_id").?.objectId.value;

    // std.debug.print("{any}\n", .{doc.values});

    try std.testing.expectEqual(text.toInt(), objid.toInt());

    // std.debug.print("\n Text: {x} \n", .{text});
}

test "BSONDoc embedded array" {
    const obj = "\x16\x00\x00\x00\x04\x61\x00\x0e\x00\x00\x00\x02\x30\x00\x02\x00\x00\x00\x62\x00\x00\x00";

    var doc = BSONDocument.init(obj);

    var embedded = doc.get("a") orelse unreachable;
    const text = embedded.array.get("0").?.string.value;

    try std.testing.expectEqualStrings("\x62", text);

    // std.debug.print("\n Text: {x} \n", .{text});
}
