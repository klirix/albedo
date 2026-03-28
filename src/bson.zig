const std = @import("std");
const mem = std.mem;
const platform = @import("platform.zig");
pub const ObjectId = @import("./object_id.zig").ObjectId;
pub const fmt = @import("./bson_formatter.zig");

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

test "BSONDocument.fromPairs encodes binary length correctly" {
    const allocator = std.testing.allocator;
    const payload = [_]u8{ 0x01, 0x02, 0x03 };

    var pairs = [_]BSONKeyValuePair{BSONKeyValuePair{
        .key = "bin",
        .value = BSONValue{ .binary = BSONBinary{ .value = &payload, .subtype = 0x00 } },
    }};
    const doc = try BSONDocument.fromPairs(allocator, pairs[0..]);
    defer doc.deinit(allocator);

    const expected = [_]u8{
        0x12, 0x00, 0x00, 0x00, // document length (18)
        0x05, // type: binary
        0x62, 0x69, 0x6e, 0x00, // key: "bin\0"
        0x03, 0x00, 0x00, 0x00, // payload length (3)
        0x00, // subtype
        0x01, 0x02, 0x03, // payload
        0x00, // document terminator
    };

    try std.testing.expectEqualSlices(u8, &expected, doc.buffer);
}

test "BSONDocument.fromPairs encodes trusted hello-world document" {
    const allocator = std.testing.allocator;

    var pairs = [_]BSONKeyValuePair{BSONKeyValuePair{
        .key = "hello",
        .value = BSONValue{ .string = .{ .value = "world" } },
    }};
    const doc = try BSONDocument.fromPairs(allocator, pairs[0..]);
    defer doc.deinit(allocator);

    // Canonical BSON bytes for {"hello": "world"}
    const expected = [_]u8{
        0x16, 0x00, 0x00, 0x00, // document length (22)
        0x02, // type: string
        0x68, 0x65, 0x6c, 0x6c, 0x6f, 0x00, // key: "hello\0"
        0x06, 0x00, 0x00, 0x00, // string length incl. terminator (6)
        0x77, 0x6f, 0x72, 0x6c, 0x64, 0x00, // "world\0"
        0x00, // document terminator
    };

    try std.testing.expectEqualSlices(u8, &expected, doc.buffer);
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

pub const BSONMinKey = struct {
    pub fn write(_: BSONMinKey, _: []u8) void {}

    pub fn size(_: BSONMinKey) u32 {
        return 0;
    }

    pub fn read(_: []const u8) BSONMinKey {
        return BSONMinKey{};
    }
};

pub const BSONMaxKey = struct {
    pub fn write(_: BSONMaxKey, _: []u8) void {}

    pub fn size(_: BSONMaxKey) u32 {
        return 0;
    }

    pub fn read(_: []const u8) BSONMaxKey {
        return BSONMaxKey{};
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
    maxKey: BSONMaxKey,
    minKey: BSONMinKey,

    pub const FromNativeError = error{
        IntegerOverflow,
    };

    pub fn fromNative(comptime T: type, value: T) FromNativeError!?BSONValue {
        return switch (@typeInfo(T)) {
            .bool => BSONValue{ .boolean = .{ .value = value } },
            .float => BSONValue{ .double = .{ .value = @as(f64, @floatCast(value)) } },
            .int => |int_info| blk: {
                if (int_info.signedness == .signed and int_info.bits <= 32) {
                    const casted_i32 = std.math.cast(i32, value) orelse return error.IntegerOverflow;
                    break :blk BSONValue{ .int32 = .{ .value = casted_i32 } };
                }
                const casted_i64 = std.math.cast(i64, value) orelse return error.IntegerOverflow;
                break :blk BSONValue{ .int64 = .{ .value = casted_i64 } };
            },
            .comptime_int => blk: {
                if (std.math.cast(i32, value)) |casted_i32| {
                    break :blk BSONValue{ .int32 = .{ .value = casted_i32 } };
                }
                if (std.math.cast(i64, value)) |casted_i64| {
                    break :blk BSONValue{ .int64 = .{ .value = casted_i64 } };
                }
                return error.IntegerOverflow;
            },
            .pointer => |ptr| switch (ptr.size) {
                .slice => if (ptr.child == u8) BSONValue{ .string = .{ .value = value } } else null,
                .one => blk: {
                    const child_info = @typeInfo(ptr.child);
                    if (child_info == .array) {
                        const arr = child_info.array;
                        if (arr.child == u8 and arr.sentinel() != null) {
                            break :blk BSONValue{ .string = .{ .value = value[0..value.len] } };
                        }
                    }
                    break :blk null;
                },
                else => null,
            },
            .array => |arr| if (arr.child == u8 and arr.sentinel() != null)
                BSONValue{ .string = .{ .value = value[0..value.len] } }
            else
                null,
            .@"struct" => if (T == ObjectId)
                BSONValue{ .objectId = .{ .value = value } }
            else
                null,
            else => null,
        };
    }

    pub fn init(value: anytype) BSONValue {
        return switch (@TypeOf(value)) {
            i32 => BSONValue{ .int32 = BSONInt32{ .value = value } },
            f64 => BSONValue{ .double = BSONDouble{ .value = value } },
            []u8 => BSONValue{ .string = BSONString{ .value = value } },
            []const u8 => BSONValue{ .string = BSONString{ .value = value } },
            BSONDocument => BSONValue{ .document = value },
            BSONBinary => BSONValue{ .binary = BSONBinary{ .value = value, .subtype = 1 } },
            ObjectId => BSONValue{ .objectId = .{ .value = value } },
            bool => BSONValue{ .boolean = BSONBoolean{ .value = value } },

            else => |unsupportedType| @compileError(std.fmt.comptimePrint("Unsupported BSONValue type: {any}", .{unsupportedType})),
        };
    }

    pub fn format(
        self: @This(),
        writer: *std.Io.Writer,
    ) error{WriteFailed}!void {
        switch (self) {
            .string => try writer.print("\"{s}\"", .{self.string.value}),
            .double => try writer.print("{d}", .{self.double.value}),
            .int32 => try writer.print("{d}", .{self.int32.value}),
            .document => try self.document.format(writer),
            .array => try self.array.format(writer),
            .datetime => try writer.print("{d}", .{self.datetime.value}),
            .int64 => try writer.print("{d}", .{self.int64.value}),
            .binary => try writer.print("Binary{{{s}}}", .{self.binary.value}),
            .boolean => try writer.print("{s}", .{if (self.boolean.value) "true" else "false"}),
            .null => try writer.writeAll("null"),
            .minKey => try writer.writeAll("MinKey"),
            .maxKey => try writer.writeAll("MaxKey"),
            .objectId => try writer.print("ObjectId(\"{s}\")", .{&self.objectId.value.toString()}),
        }
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
            .minKey => 0,
            .maxKey => 0,
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
            .minKey => .minKey,
            .maxKey => .maxKey,
            .objectId => .objectId,
        };
    }

    pub inline fn valueOrder(self: *const BSONValue) u8 {
        return switch (self.*) {
            .minKey => 0,
            .null => 1,
            .double, .int32, .int64 => 2,
            .string => 3,
            .document => 4,
            .array => 5,
            .binary => 6,
            .objectId => 7,
            .boolean => 8,
            .datetime => 9,
            .maxKey => 10,
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
            .minKey => 0,
            .maxKey => 0,
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
            .minKey => true,
            .maxKey => true,
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
            .minKey => .eq,
            .maxKey => .eq,
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

    pub fn write(self: *const BSONValue, writer: *std.Io.Writer) !void {
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
                try writer.writeInt(u32, @as(u32, @truncate(self.binary.value.len)), .little);
                try writer.writeByte(self.binary.subtype);
                try writer.writeAll(self.binary.value);
            },
            .boolean => {
                try writer.writeByte(if (self.boolean.value) 0x01 else 0x00);
            },
            .null => {
                // Null type does not need to write any data
            },
            .minKey => {
                // MinKey encodes without payload
            },
            .maxKey => {
                // MaxKey encodes without payload
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
            .minKey => BSONValue{ .minKey = BSONMinKey.read(memory) },
            .maxKey => BSONValue{ .maxKey = BSONMaxKey.read(memory) },
            .objectId => BSONValue{ .objectId = BSONObjectId.read(memory) },
        };
    }
};

test "BSONValue.eql array -> int32" {
    const array = try fmt.serialize(.{ .@"0" = 123, .@"1" = 123 }, std.testing.allocator);
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

test "BSONValue.order min/max key" {
    const min_val = BSONValue{ .minKey = BSONMinKey{} };
    const max_val = BSONValue{ .maxKey = BSONMaxKey{} };
    const null_val = BSONValue{ .null = BSONNull{} };
    const int_val = BSONValue{ .int32 = BSONInt32{ .value = 0 } };

    try std.testing.expect(min_val.order(null_val) == .lt);
    try std.testing.expect(max_val.order(int_val) == .gt);

    const another_min = BSONValue{ .minKey = BSONMinKey{} };
    const another_max = BSONValue{ .maxKey = BSONMaxKey{} };
    try std.testing.expect(min_val.eql(&another_min));
    try std.testing.expect(max_val.eql(&another_max));
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
    maxKey = 0x7F,
    minKey = 0xFF,
};

pub const TypeNamePair = struct {
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

    fn jsonToDoc(json: *std.json.Scanner, allocator: mem.Allocator) ![]u8 {
        var pairs = std.Io.Writer.Allocating.init(allocator);
        errdefer pairs.deinit();
        const writer = &pairs.writer;
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
        std.mem.writeInt(u32, pairs.written()[0..4], @truncate(pairs.written().len + 1), .little);
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
        var list = std.Io.Writer.Allocating.init(allocator);
        errdefer list.deinit();
        const writer = &list.writer;
        try writer.writeInt(u32, 0, .little);

        for (pairs) |pair| {
            try writer.writeByte(@intFromEnum(pair.value.valueType()));
            try writer.writeAll(pair.key);
            try writer.writeByte(0);
            try pair.value.write(writer);
        }
        try writer.writeByte(0); // Null terminator for the document

        std.mem.writeInt(u32, list.written()[0..4], @as(u32, @truncate(list.written().len)), .little);

        return BSONDocument{ .buffer = try list.toOwnedSlice() };
    }

    pub fn write(self: *const BSONDocument, writer: *std.Io.Writer) !void {
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

    /// Returns the value at the key in the document.
    /// Returns null if the key is not found.
    pub fn get(self: BSONDocument, key: []const u8) ?BSONValue {
        var idx: usize = 4;
        const key_len = key.len;
        const buffer = self.buffer;

        while (buffer[idx] != 0) {
            const typeByte: BSONValueType = @enumFromInt(buffer[idx]);
            idx += 1;

            // Calculate name length by scanning for null terminator
            const nameStart = idx;
            var nameLen: usize = 0;
            while (buffer[idx + nameLen] != 0) : (nameLen += 1) {}

            const name = buffer[nameStart..][0..nameLen];
            idx += nameLen + 1; // Move past name and null terminator

            // Fast path: check first char and length before full comparison
            if (nameLen == key_len and name[0] == key[0]) {
                if (mem.eql(u8, name, key)) {
                    return BSONValue.read(buffer[idx..], typeByte);
                }
            }

            const size = BSONValue.asessSize(buffer[idx..], typeByte);
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

    pub fn format(
        self: BSONDocument,
        writer: *std.Io.Writer,
    ) error{WriteFailed}!void {
        var pairIter = self.iter();
        try writer.writeAll("{ ");
        var first = true;
        while (pairIter.next()) |pair| {
            if (!first) {
                try writer.writeAll(", ");
            }
            first = false;
            try writer.writeAll(pair.key);
            try writer.writeAll(": ");
            try pair.value.format(writer);
        }
        try writer.writeAll(" }");
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
        var doc = try fmt.serialize(.{ .key = "test" }, ally);

        defer doc.deinit(ally);

        const value = doc.getPath("key") orelse unreachable;
        try std.testing.expectEqualStrings("test", value.string.value);
    }

    pub fn unset(self: *const BSONDocument, allocator: mem.Allocator, key: []const u8) !BSONDocument {
        var editor = Editor.init(allocator, self.*);
        defer editor.deinit();
        try editor.unset(key);
        return try editor.finish();
    }

    pub fn set(self: BSONDocument, allocator: mem.Allocator, key: []const u8, value: BSONValue) !BSONDocument {
        var editor = Editor.init(allocator, self);
        defer editor.deinit();
        try editor.setValue(key, value);
        return try editor.finish();
    }

    pub fn deinit(self: *const BSONDocument, allocator: mem.Allocator) void {
        defer allocator.free(self.buffer);
    }
};

const ContainerKind = enum { document, array };

const ContainerFrame = struct {
    kind: ContainerKind,
    len_pos: usize,
    next_index: usize = 0,
};

pub const Builder = struct {
    allocator: mem.Allocator,
    buffer: std.Io.Writer.Allocating,
    frames: std.ArrayList(ContainerFrame) = .empty,
    finished: bool = false,

    pub fn init(allocator: mem.Allocator) !Builder {
        var builder = Builder{
            .allocator = allocator,
            .buffer = std.Io.Writer.Allocating.init(allocator),
        };
        errdefer builder.buffer.deinit();
        try builder.buffer.writer.writeInt(u32, 0, .little);
        try builder.frames.append(allocator, .{
            .kind = .document,
            .len_pos = 0,
        });
        return builder;
    }

    pub fn put(self: *Builder, key: []const u8, value: anytype) !void {
        var root = self.rootEncoder();
        try root.put(key, value);
    }

    pub fn putValue(self: *Builder, key: []const u8, value: BSONValue) !void {
        var root = self.rootEncoder();
        try root.putValue(key, value);
    }

    pub fn putNull(self: *Builder, key: []const u8) !void {
        var root = self.rootEncoder();
        try root.putNull(key);
    }

    pub fn object(self: *Builder, key: []const u8) !DocEncoder {
        var root = self.rootEncoder();
        return try root.object(key);
    }

    pub fn array(self: *Builder, key: []const u8) !ArrayEncoder {
        var root = self.rootEncoder();
        return try root.array(key);
    }

    pub fn finish(self: *Builder) !BSONDocument {
        if (self.finished) return error.BuilderFinished;
        if (self.frames.items.len != 1) return error.UnclosedContainer;

        try self.closeContainer(0, true);
        self.finished = true;
        const buffer = try self.buffer.toOwnedSlice();
        return BSONDocument.init(buffer);
    }

    pub fn deinit(self: *Builder) void {
        self.buffer.deinit();
        self.frames.deinit(self.allocator);
    }

    fn rootEncoder(self: *Builder) DocEncoder {
        return .{
            .builder = self,
            .frame_index = 0,
        };
    }

    pub fn docEncoder(self: *Builder) DocEncoder {
        return self.rootEncoder();
    }

    fn ensureFrame(self: *Builder, frame_index: usize, expected: ContainerKind) !void {
        if (self.finished) return error.BuilderFinished;
        if (frame_index >= self.frames.items.len) return error.ContainerClosed;
        if (self.frames.items[frame_index].kind != expected) return error.InvalidContainerType;
        if (frame_index != self.frames.items.len - 1) return error.UnclosedContainer;
    }

    fn openContainer(self: *Builder, kind: ContainerKind, key: []const u8) !usize {
        const parent_index = self.frames.items.len - 1;
        switch (self.frames.items[parent_index].kind) {
            .document => try self.appendElementHeader(kindValueType(kind), key),
            .array => try self.appendArrayElementHeader(kindValueType(kind), parent_index),
        }

        const len_pos = self.buffer.written().len;
        try self.buffer.writer.writeInt(u32, 0, .little);

        try self.frames.append(self.allocator, .{
            .kind = kind,
            .len_pos = len_pos,
        });
        return self.frames.items.len - 1;
    }

    fn openArrayChild(self: *Builder, kind: ContainerKind, frame_index: usize) !usize {
        try self.ensureFrame(frame_index, .array);
        try self.appendArrayElementHeader(kindValueType(kind), frame_index);
        const len_pos = self.buffer.written().len;
        try self.buffer.writer.writeInt(u32, 0, .little);

        try self.frames.append(self.allocator, .{
            .kind = kind,
            .len_pos = len_pos,
        });
        return self.frames.items.len - 1;
    }

    fn closeContainer(self: *Builder, frame_index: usize, allow_root_end: bool) !void {
        if (frame_index == 0 and !allow_root_end) return error.CannotEndRoot;
        if (frame_index >= self.frames.items.len) return error.ContainerClosed;
        if (frame_index != self.frames.items.len - 1) return error.UnclosedContainer;

        const frame = self.frames.items[frame_index];
        try self.buffer.writer.writeByte(0);
        const len: u32 = @intCast(self.buffer.written().len - frame.len_pos);
        std.mem.writeInt(u32, self.buffer.written()[frame.len_pos..][0..4], len, .little);
        _ = self.frames.pop();
    }

    fn appendElementHeader(self: *Builder, value_type: BSONValueType, key: []const u8) !void {
        try self.buffer.writer.writeByte(@intFromEnum(value_type));
        try self.buffer.writer.writeAll(key);
        try self.buffer.writer.writeByte(0);
    }

    fn appendArrayElementHeader(self: *Builder, value_type: BSONValueType, frame_index: usize) !void {
        var key_buf: [32]u8 = undefined;
        const frame = &self.frames.items[frame_index];
        const key = std.fmt.bufPrint(&key_buf, "{d}", .{frame.next_index}) catch unreachable;
        frame.next_index += 1;
        try self.appendElementHeader(value_type, key);
    }

    fn appendValuePayload(self: *Builder, value: BSONValue) !void {
        try value.write(&self.buffer.writer);
    }
};

pub const DocEncoder = struct {
    builder: *Builder,
    frame_index: usize,

    pub fn put(self: *DocEncoder, key: []const u8, value: anytype) !void {
        try fmt.encodeField(self, key, value);
    }

    pub fn putValue(self: *DocEncoder, key: []const u8, value: BSONValue) !void {
        try self.builder.ensureFrame(self.frame_index, .document);
        try self.builder.appendElementHeader(value.valueType(), key);
        try self.builder.appendValuePayload(value);
    }

    pub fn putNull(self: *DocEncoder, key: []const u8) !void {
        try self.putValue(key, .{ .null = .{} });
    }

    pub fn object(self: *DocEncoder, key: []const u8) !DocEncoder {
        try self.builder.ensureFrame(self.frame_index, .document);
        const frame_index = try self.builder.openContainer(.document, key);
        return .{
            .builder = self.builder,
            .frame_index = frame_index,
        };
    }

    pub fn array(self: *DocEncoder, key: []const u8) !ArrayEncoder {
        try self.builder.ensureFrame(self.frame_index, .document);
        const frame_index = try self.builder.openContainer(.array, key);
        return .{
            .builder = self.builder,
            .frame_index = frame_index,
        };
    }

    pub fn end(self: *DocEncoder) !void {
        try self.builder.closeContainer(self.frame_index, false);
    }
};

pub const ArrayEncoder = struct {
    builder: *Builder,
    frame_index: usize,

    pub fn append(self: *ArrayEncoder, value: anytype) !void {
        try fmt.encodeArrayValue(self, value);
    }

    pub fn appendValue(self: *ArrayEncoder, value: BSONValue) !void {
        try self.builder.ensureFrame(self.frame_index, .array);
        try self.builder.appendArrayElementHeader(value.valueType(), self.frame_index);
        try self.builder.appendValuePayload(value);
    }

    pub fn object(self: *ArrayEncoder) !DocEncoder {
        const frame_index = try self.builder.openArrayChild(.document, self.frame_index);
        return .{
            .builder = self.builder,
            .frame_index = frame_index,
        };
    }

    pub fn array(self: *ArrayEncoder) !ArrayEncoder {
        const frame_index = try self.builder.openArrayChild(.array, self.frame_index);
        return .{
            .builder = self.builder,
            .frame_index = frame_index,
        };
    }

    pub fn end(self: *ArrayEncoder) !void {
        try self.builder.closeContainer(self.frame_index, false);
    }
};

pub const Editor = struct {
    allocator: mem.Allocator,
    current: BSONDocument,
    owns_current: bool = false,
    finished: bool = false,

    pub fn init(allocator: mem.Allocator, source_doc: BSONDocument) Editor {
        return .{
            .allocator = allocator,
            .current = source_doc,
        };
    }

    pub fn set(self: *Editor, key: []const u8, value: anytype) !void {
        std.debug.assert(!self.finished);

        var single_field = try Builder.init(self.allocator);
        defer single_field.deinit();
        try single_field.put(key, value);
        const encoded = try single_field.finish();
        defer encoded.deinit(self.allocator);

        const next = try rewriteDocumentSet(self.current, self.allocator, key, encoded);
        self.replaceCurrent(next);
    }

    pub fn setValue(self: *Editor, key: []const u8, value: BSONValue) !void {
        std.debug.assert(!self.finished);

        var pairs = [1]BSONKeyValuePair{.{
            .key = key,
            .value = value,
        }};
        const encoded = try BSONDocument.fromPairs(self.allocator, pairs[0..]);
        defer encoded.deinit(self.allocator);

        const next = try rewriteDocumentSet(self.current, self.allocator, key, encoded);
        self.replaceCurrent(next);
    }

    pub fn unset(self: *Editor, key: []const u8) !void {
        std.debug.assert(!self.finished);
        const next = try rewriteDocumentUnset(self.current, self.allocator, key);
        self.replaceCurrent(next);
    }

    pub fn finish(self: *Editor) !BSONDocument {
        std.debug.assert(!self.finished);
        self.finished = true;

        if (self.owns_current) {
            const out = self.current;
            self.current = BSONDocument.initEmpty();
            self.owns_current = false;
            return out;
        }

        return try cloneDocument(self.allocator, self.current);
    }

    pub fn deinit(self: *Editor) void {
        if (self.owns_current) {
            self.current.deinit(self.allocator);
            self.owns_current = false;
        }
    }

    fn replaceCurrent(self: *Editor, next: BSONDocument) void {
        if (self.owns_current) {
            self.current.deinit(self.allocator);
        }
        self.current = next;
        self.owns_current = true;
    }
};

fn kindValueType(kind: ContainerKind) BSONValueType {
    return switch (kind) {
        .document => .document,
        .array => .array,
    };
}

fn cloneDocument(allocator: mem.Allocator, doc: BSONDocument) !BSONDocument {
    const buffer = try allocator.alloc(u8, doc.buffer.len);
    @memcpy(buffer, doc.buffer);
    return BSONDocument.init(buffer);
}

fn appendExistingElement(
    writer: *std.Io.Writer,
    doc: BSONDocument,
    idx: usize,
    pair: TypeNamePair,
    value_size: u32,
) !void {
    try writer.writeByte(@intFromEnum(pair.type));
    try writer.writeAll(pair.name);
    try writer.writeByte(0);
    try writer.writeAll(doc.buffer[idx + pair.len .. idx + pair.len + value_size]);
}

fn appendSingleFieldElement(
    writer: *std.Io.Writer,
    single_field_doc: BSONDocument,
) !void {
    const pair = TypeNamePair.read(single_field_doc.buffer[4..]) orelse unreachable;
    const value_size = BSONValue.asessSize(single_field_doc.buffer[4 + pair.len ..], pair.type);
    try writer.writeAll(single_field_doc.buffer[4 .. 4 + pair.len + value_size]);
}

fn rewriteDocumentSet(
    doc: BSONDocument,
    allocator: mem.Allocator,
    key: []const u8,
    encoded_field: BSONDocument,
) !BSONDocument {
    var list = std.Io.Writer.Allocating.init(allocator);
    errdefer list.deinit();
    try list.writer.writeInt(u32, 0, .little);
    var idx: usize = 4;
    while (TypeNamePair.read(doc.buffer[idx..])) |pair| {
        const value_size = BSONValue.asessSize(doc.buffer[idx + pair.len ..], pair.type);
        if (!mem.eql(u8, pair.name, key)) {
            try appendExistingElement(&list.writer, doc, idx, pair, value_size);
        }
        idx += pair.len + value_size;
    }

    try appendSingleFieldElement(&list.writer, encoded_field);
    try list.writer.writeByte(0);

    const final_buffer = try list.toOwnedSlice();
    std.mem.writeInt(u32, final_buffer[0..4], @intCast(final_buffer.len), .little);
    return BSONDocument.init(final_buffer);
}

fn rewriteDocumentUnset(
    doc: BSONDocument,
    allocator: mem.Allocator,
    key: []const u8,
) !BSONDocument {
    var list = std.Io.Writer.Allocating.init(allocator);
    errdefer list.deinit();
    try list.writer.writeInt(u32, 0, .little);
    var idx: usize = 4;
    while (TypeNamePair.read(doc.buffer[idx..])) |pair| {
        const value_size = BSONValue.asessSize(doc.buffer[idx + pair.len ..], pair.type);
        if (!mem.eql(u8, pair.name, key)) {
            try appendExistingElement(&list.writer, doc, idx, pair, value_size);
        }
        idx += pair.len + value_size;
    }

    try list.writer.writeByte(0);

    const final_buffer = try list.toOwnedSlice();
    std.mem.writeInt(u32, final_buffer[0..4], @intCast(final_buffer.len), .little);
    return BSONDocument.init(final_buffer);
}

test "Builder composes flat and nested documents" {
    const allocator = std.testing.allocator;
    var builder = try Builder.init(allocator);
    defer builder.deinit();

    try builder.put("name", "Alice");
    try builder.put("age", @as(i32, 32));

    var profile = try builder.object("profile");
    try profile.put("city", "Paris");
    try profile.put("active", true);
    try profile.end();

    var tags = try builder.array("tags");
    try tags.append("db");
    try tags.append("zig");
    try tags.end();

    const doc = try builder.finish();
    defer doc.deinit(allocator);

    try std.testing.expectEqualStrings("Alice", doc.get("name").?.string.value);
    try std.testing.expectEqual(@as(i32, 32), doc.get("age").?.int32.value);
    try std.testing.expectEqualStrings("Paris", doc.getPath("profile.city").?.string.value);
    try std.testing.expectEqual(true, doc.getPath("profile.active").?.boolean.value);
    try std.testing.expectEqualStrings("db", doc.getPath("tags.0").?.string.value);
    try std.testing.expectEqualStrings("zig", doc.getPath("tags.1").?.string.value);
}

test "Builder supports runtime keys" {
    const allocator = std.testing.allocator;
    const dynamic_key = try allocator.dupe(u8, "dynamic");
    defer allocator.free(dynamic_key);

    var builder = try Builder.init(allocator);
    defer builder.deinit();
    try builder.put(dynamic_key, @as(i32, 7));

    const doc = try builder.finish();
    defer doc.deinit(allocator);

    try std.testing.expectEqual(@as(i32, 7), doc.get("dynamic").?.int32.value);
}

test "Builder finish rejects unclosed containers" {
    const allocator = std.testing.allocator;
    var builder = try Builder.init(allocator);
    defer builder.deinit();

    _ = try builder.object("profile");
    try std.testing.expectError(error.UnclosedContainer, builder.finish());
}

test "Editor updates and unsets top-level fields" {
    const allocator = std.testing.allocator;
    var base = try fmt.serialize(.{
        .a = @as(i32, 1),
        .b = @as(i32, 2),
        .c = @as(i32, 3),
        .nested = .{ .flag = true },
    }, allocator);
    defer base.deinit(allocator);

    var editor = Editor.init(allocator, base);
    defer editor.deinit();
    try editor.set("b", @as(i32, 20));
    try editor.unset("a");

    const doc = try editor.finish();
    defer doc.deinit(allocator);

    try std.testing.expect(doc.get("a") == null);
    try std.testing.expectEqual(@as(i32, 20), doc.get("b").?.int32.value);
    try std.testing.expectEqual(true, doc.getPath("nested.flag").?.boolean.value);

    var iter = doc.iter();
    try std.testing.expectEqualStrings("c", iter.next().?.key);
    try std.testing.expectEqualStrings("nested", iter.next().?.key);
    try std.testing.expectEqualStrings("b", iter.next().?.key);
    try std.testing.expect(iter.next() == null);
}

test "format" {
    const allocator = std.testing.allocator;
    var doc = try fmt.serialize(.{ .key = "test" }, allocator);
    defer doc.deinit(allocator);

    var arrList = std.Io.Writer.Allocating.init(allocator);
    defer arrList.deinit();
    try doc.format(&arrList.writer);

    try std.testing.expectEqualStrings("{ key: \"test\" }", arrList.written());
}

test "BSONDocument write" {
    const allocator = std.testing.allocator;
    var list = std.Io.Writer.Allocating.init(allocator);
    defer list.deinit();
    var doc = BSONDocument.initEmpty();
    doc = try doc.set(allocator, "test", .{ .string = .{ .value = "test" } });
    defer doc.deinit(allocator);
    // const actual = allocator.alloc(u8, 100) catch unreachable;

    try doc.write(&list.writer);
    const expected_string = ("\x14\x00\x00\x00\x02test\x00\x05\x00\x00\x00test\x00\x00");
    try std.testing.expectEqualSlices(u8, expected_string, list.written());
}

test "BSONDocument serializeToMemory" {
    var allocator = std.testing.allocator;
    var doc = try fmt.serialize(.{ .key = "test" }, allocator);
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

    const objid = try ObjectId.init(platform.testing_platform);

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
