const std = @import("std");
const bson = @import("./bson.zig");
const ObjectId = @import("./object_id.zig").ObjectId;

pub const Error = error{
    WriteFailed,
    UnsupportedType,
    MissingField,
    TypeMismatch,
    IntegerOverflow,
    BuilderFinished,
    UnclosedContainer,
    ContainerClosed,
    InvalidContainerType,
    CannotEndRoot,
};

pub fn ParseResult(comptime T: type) type {
    return struct {
        value: T,
        arena: std.heap.ArenaAllocator,

        pub fn deinit(self: *@This()) void {
            self.arena.deinit();
        }
    };
}

pub fn serialize(value: anytype, allocator: std.mem.Allocator) (Error || std.mem.Allocator.Error)!bson.BSONDocument {
    const T = @TypeOf(value);

    if (T == bson.BSONDocument) {
        return try copyDocument(value, allocator);
    }

    if (T == bson.BSONValue) {
        return switch (value) {
            .document => |doc| try copyDocument(doc, allocator),
            else => Error.UnsupportedType,
        };
    }

    var builder = try bson.Builder.init(allocator);
    defer builder.deinit();

    var root = builder.docEncoder();
    if (comptime hasBsonEncode(T)) {
        try callBsonEncode(T, value, &root);
    } else {
        try encodeFields(value, &root);
    }

    return try builder.finish();
}

pub fn parse(comptime T: type, doc: bson.BSONDocument, allocator: std.mem.Allocator) (Error || std.mem.Allocator.Error)!ParseResult(T) {
    var arena = std.heap.ArenaAllocator.init(allocator);
    errdefer arena.deinit();

    const parsed_value = try decodeRoot(T, doc, arena.allocator());
    return .{
        .value = parsed_value,
        .arena = arena,
    };
}

pub fn encodeFields(value: anytype, doc: *bson.DocEncoder) (Error || std.mem.Allocator.Error)!void {
    const T = @TypeOf(value);
    const type_info = @typeInfo(T);
    if (type_info != .@"struct") return Error.UnsupportedType;

    inline for (type_info.@"struct".fields) |field| {
        try encodeField(doc, field.name, @field(value, field.name));
    }
}

pub fn decodeFields(comptime T: type, doc: bson.BSONDocument, allocator: std.mem.Allocator) (Error || std.mem.Allocator.Error)!T {
    const type_info = @typeInfo(T);
    if (type_info != .@"struct") return Error.UnsupportedType;

    var result: T = undefined;
    inline for (type_info.@"struct".fields) |field| {
        const field_type = field.type;
        const maybe_value = doc.get(field.name);

        if (maybe_value) |bson_value| {
            @field(result, field.name) = try decodeValue(field_type, bson_value, allocator);
        } else if (field.defaultValue()) |default| {
            @field(result, field.name) = default;
        } else if (@typeInfo(field_type) == .optional) {
            @field(result, field.name) = null;
        } else {
            return Error.MissingField;
        }
    }

    return result;
}

pub fn encodeField(doc: *bson.DocEncoder, key: []const u8, value: anytype) (Error || std.mem.Allocator.Error)!void {
    try encodeValueIntoDoc(@TypeOf(value), value, doc, key);
}

pub fn encodeArrayValue(array: *bson.ArrayEncoder, value: anytype) (Error || std.mem.Allocator.Error)!void {
    try encodeValueIntoArray(@TypeOf(value), value, array);
}

fn decodeRoot(comptime T: type, doc: bson.BSONDocument, allocator: std.mem.Allocator) (Error || std.mem.Allocator.Error)!T {
    if (T == bson.BSONDocument) return try copyDocument(doc, allocator);
    if (comptime hasBsonDecode(T)) return try callBsonDecode(T, doc, allocator);
    return try decodeFields(T, doc, allocator);
}

fn encodeValueIntoDoc(
    comptime T: type,
    value: T,
    doc: *bson.DocEncoder,
    key: []const u8,
) (Error || std.mem.Allocator.Error)!void {
    if (try tryPassThroughBsonValue(T, value)) |native| {
        try doc.putValue(key, native);
        return;
    }

    if (comptime hasBsonEncode(T)) {
        var child = try doc.object(key);
        try callBsonEncode(T, value, &child);
        try child.end();
        return;
    }

    switch (@typeInfo(T)) {
        .optional => |opt| {
            if (value) |inner| {
                try encodeValueIntoDoc(opt.child, inner, doc, key);
            } else {
                try doc.putNull(key);
            }
        },
        .pointer => |ptr| switch (ptr.size) {
            .slice => if (ptr.child != u8) {
                var array = try doc.array(key);
                for (value) |item| {
                    try encodeValueIntoArray(ptr.child, item, &array);
                }
                try array.end();
            } else return Error.UnsupportedType,
            else => return Error.UnsupportedType,
        },
        .array => |arr| if (arr.child != u8 or arr.sentinel() == null) {
            var array = try doc.array(key);
            for (value) |item| {
                try encodeValueIntoArray(arr.child, item, &array);
            }
            try array.end();
        } else return Error.UnsupportedType,
        .@"struct" => {
            var child = try doc.object(key);
            try encodeFields(value, &child);
            try child.end();
        },
        .@"enum" => {
            try doc.putValue(key, .{ .string = .{ .value = @tagName(value) } });
        },
        .@"union" => |u| if (u.tag_type != null) {
            const active_tag = std.meta.activeTag(value);
            inline for (u.fields) |field| {
                if (active_tag == @field(std.meta.Tag(T), field.name)) {
                    if (field.type == void) {
                        try doc.putValue(key, .{ .string = .{ .value = field.name } });
                    } else {
                        var child = try doc.object(key);
                        try encodeValueIntoDoc(field.type, @field(value, field.name), &child, field.name);
                        try child.end();
                    }
                    return;
                }
            }
            return Error.UnsupportedType;
        } else return Error.UnsupportedType,
        else => return Error.UnsupportedType,
    }
}

fn encodeValueIntoArray(
    comptime T: type,
    value: T,
    array: *bson.ArrayEncoder,
) (Error || std.mem.Allocator.Error)!void {
    if (try tryPassThroughBsonValue(T, value)) |native| {
        try array.appendValue(native);
        return;
    }

    if (comptime hasBsonEncode(T)) {
        var child = try array.object();
        try callBsonEncode(T, value, &child);
        try child.end();
        return;
    }

    switch (@typeInfo(T)) {
        .optional => |opt| {
            if (value) |inner| {
                try encodeValueIntoArray(opt.child, inner, array);
            } else {
                try array.appendValue(.{ .null = .{} });
            }
        },
        .pointer => |ptr| switch (ptr.size) {
            .slice => if (ptr.child != u8) {
                var child = try array.array();
                for (value) |item| {
                    try encodeValueIntoArray(ptr.child, item, &child);
                }
                try child.end();
            } else return Error.UnsupportedType,
            else => return Error.UnsupportedType,
        },
        .array => |arr| if (arr.child != u8 or arr.sentinel() == null) {
            var child = try array.array();
            for (value) |item| {
                try encodeValueIntoArray(arr.child, item, &child);
            }
            try child.end();
        } else return Error.UnsupportedType,
        .@"struct" => {
            var child = try array.object();
            try encodeFields(value, &child);
            try child.end();
        },
        .@"enum" => {
            try array.appendValue(.{ .string = .{ .value = @tagName(value) } });
        },
        .@"union" => |u| if (u.tag_type != null) {
            const active_tag = std.meta.activeTag(value);
            inline for (u.fields) |field| {
                if (active_tag == @field(std.meta.Tag(T), field.name)) {
                    if (field.type == void) {
                        try array.appendValue(.{ .string = .{ .value = field.name } });
                    } else {
                        var child = try array.object();
                        try encodeValueIntoDoc(field.type, @field(value, field.name), &child, field.name);
                        try child.end();
                    }
                    return;
                }
            }
            return Error.UnsupportedType;
        } else return Error.UnsupportedType,
        else => return Error.UnsupportedType,
    }
}

fn tryPassThroughBsonValue(comptime T: type, value: T) (Error || std.mem.Allocator.Error)!?bson.BSONValue {
    switch (T) {
        bson.BSONValue => return value,
        bson.BSONDocument => return bson.BSONValue{ .document = value },
        bson.BSONString => return bson.BSONValue{ .string = value },
        bson.BSONDouble => return bson.BSONValue{ .double = value },
        bson.BSONInt32 => return bson.BSONValue{ .int32 = value },
        bson.BSONInt64 => return bson.BSONValue{ .int64 = value },
        bson.BSONDatetime => return bson.BSONValue{ .datetime = value },
        bson.BSONBinary => return bson.BSONValue{ .binary = value },
        bson.BSONBoolean => return bson.BSONValue{ .boolean = value },
        bson.BSONNull => return bson.BSONValue{ .null = value },
        bson.BSONMinKey => return bson.BSONValue{ .minKey = value },
        bson.BSONMaxKey => return bson.BSONValue{ .maxKey = value },
        bson.BSONObjectId => return bson.BSONValue{ .objectId = value },
        else => {},
    }

    if (bson.BSONValue.fromNative(T, value) catch |err| switch (err) {
        error.IntegerOverflow => return Error.IntegerOverflow,
    }) |native| {
        return native;
    }

    return null;
}

fn decodeValue(comptime T: type, bson_value: bson.BSONValue, allocator: std.mem.Allocator) (Error || std.mem.Allocator.Error)!T {
    if (T == bson.BSONValue) return try copyValue(bson_value, allocator);
    if (T == bson.BSONDocument) return switch (bson_value) {
        .document => try copyDocument(bson_value.document, allocator),
        else => Error.TypeMismatch,
    };
    if (T == bson.BSONString) return switch (bson_value) {
        .string => .{ .value = try allocator.dupe(u8, bson_value.string.value) },
        else => Error.TypeMismatch,
    };
    if (T == bson.BSONBinary) return switch (bson_value) {
        .binary => .{
            .value = try allocator.dupe(u8, bson_value.binary.value),
            .subtype = bson_value.binary.subtype,
        },
        else => Error.TypeMismatch,
    };
    if (T == bson.BSONDouble) return switch (bson_value) {
        .double => bson_value.double,
        else => Error.TypeMismatch,
    };
    if (T == bson.BSONInt32) return switch (bson_value) {
        .int32 => bson_value.int32,
        else => Error.TypeMismatch,
    };
    if (T == bson.BSONInt64) return switch (bson_value) {
        .int64 => bson_value.int64,
        else => Error.TypeMismatch,
    };
    if (T == bson.BSONDatetime) return switch (bson_value) {
        .datetime => bson_value.datetime,
        else => Error.TypeMismatch,
    };
    if (T == bson.BSONBoolean) return switch (bson_value) {
        .boolean => bson_value.boolean,
        else => Error.TypeMismatch,
    };
    if (T == bson.BSONNull) return switch (bson_value) {
        .null => bson_value.null,
        else => Error.TypeMismatch,
    };
    if (T == bson.BSONMinKey) return switch (bson_value) {
        .minKey => bson_value.minKey,
        else => Error.TypeMismatch,
    };
    if (T == bson.BSONMaxKey) return switch (bson_value) {
        .maxKey => bson_value.maxKey,
        else => Error.TypeMismatch,
    };
    if (T == bson.BSONObjectId) return switch (bson_value) {
        .objectId => bson_value.objectId,
        else => Error.TypeMismatch,
    };
    if (T == ObjectId) return switch (bson_value) {
        .objectId => bson_value.objectId.value,
        else => Error.TypeMismatch,
    };

    if (comptime hasBsonDecode(T)) {
        return switch (bson_value) {
            .document => try callBsonDecode(T, bson_value.document, allocator),
            else => Error.TypeMismatch,
        };
    }

    return switch (@typeInfo(T)) {
        .bool => switch (bson_value) {
            .boolean => bson_value.boolean.value,
            else => Error.TypeMismatch,
        },
        .float => switch (bson_value) {
            .double => @as(T, @floatCast(bson_value.double.value)),
            .int32 => @as(T, @floatFromInt(bson_value.int32.value)),
            .int64 => @as(T, @floatFromInt(bson_value.int64.value)),
            else => Error.TypeMismatch,
        },
        .int => switch (bson_value) {
            .int32 => std.math.cast(T, bson_value.int32.value) orelse Error.IntegerOverflow,
            .int64 => std.math.cast(T, bson_value.int64.value) orelse Error.IntegerOverflow,
            .double => std.math.cast(T, @as(i64, @intFromFloat(bson_value.double.value))) orelse Error.IntegerOverflow,
            else => Error.TypeMismatch,
        },
        .optional => |opt| switch (bson_value) {
            .null => null,
            else => try decodeValue(opt.child, bson_value, allocator),
        },
        .pointer => |ptr| switch (ptr.size) {
            .slice => if (ptr.child == u8)
                switch (bson_value) {
                    .string => try allocator.dupe(u8, bson_value.string.value),
                    else => Error.TypeMismatch,
                }
            else switch (bson_value) {
                .array => try parseBsonArrayToSlice(ptr.child, bson_value.array, allocator),
                else => Error.TypeMismatch,
            },
            else => Error.UnsupportedType,
        },
        .array => |arr| if (arr.child == u8 and arr.sentinel() != null)
            switch (bson_value) {
                .string => try allocator.dupeZ(u8, bson_value.string.value),
                else => Error.TypeMismatch,
            }
        else switch (bson_value) {
            .array => try parseBsonArrayToFixedArray(T, arr.child, bson_value.array, allocator),
            else => Error.TypeMismatch,
        },
        .@"struct" => switch (bson_value) {
            .document => try decodeFields(T, bson_value.document, allocator),
            else => Error.TypeMismatch,
        },
        .@"enum" => |e| switch (bson_value) {
            .string => {
                const str = bson_value.string.value;
                inline for (e.fields) |field| {
                    if (std.mem.eql(u8, str, field.name)) {
                        return @enumFromInt(field.value);
                    }
                }
                return Error.TypeMismatch;
            },
            else => Error.TypeMismatch,
        },
        .@"union" => |u| if (u.tag_type != null) blk: {
            if (bson_value == .string) {
                const str = bson_value.string.value;
                inline for (u.fields) |field| {
                    if (field.type == void and std.mem.eql(u8, str, field.name)) {
                        break :blk @unionInit(T, field.name, {});
                    }
                }
                break :blk Error.TypeMismatch;
            }

            if (bson_value == .document) {
                inline for (u.fields) |field| {
                    if (field.type != void) {
                        if (bson_value.document.get(field.name)) |inner| {
                            break :blk @unionInit(T, field.name, try decodeValue(field.type, inner, allocator));
                        }
                    }
                }
                break :blk Error.TypeMismatch;
            }

            break :blk Error.TypeMismatch;
        } else Error.UnsupportedType,
        else => Error.UnsupportedType,
    };
}

fn parseBsonArrayToSlice(
    comptime Elem: type,
    array_doc: bson.BSONDocument,
    allocator: std.mem.Allocator,
) (Error || std.mem.Allocator.Error)![]Elem {
    const len: usize = @intCast(array_doc.keyNumber());
    const out = try allocator.alloc(Elem, len);

    for (0..len) |idx| {
        var key_buf: [20]u8 = undefined;
        const key = std.fmt.bufPrint(&key_buf, "{d}", .{idx}) catch unreachable;
        const value = array_doc.get(key) orelse return Error.MissingField;
        out[idx] = try decodeValue(Elem, value, allocator);
    }

    return out;
}

fn parseBsonArrayToFixedArray(
    comptime ArrT: type,
    comptime Elem: type,
    array_doc: bson.BSONDocument,
    allocator: std.mem.Allocator,
) (Error || std.mem.Allocator.Error)!ArrT {
    var out: ArrT = undefined;

    for (0..out.len) |idx| {
        var key_buf: [20]u8 = undefined;
        const key = std.fmt.bufPrint(&key_buf, "{d}", .{idx}) catch unreachable;
        const value = array_doc.get(key) orelse return Error.MissingField;
        out[idx] = try decodeValue(Elem, value, allocator);
    }

    return out;
}

fn copyDocument(doc: bson.BSONDocument, allocator: std.mem.Allocator) !bson.BSONDocument {
    const buffer = try allocator.alloc(u8, doc.buffer.len);
    @memcpy(buffer, doc.buffer);
    return bson.BSONDocument.init(buffer);
}

fn copyValue(value: bson.BSONValue, allocator: std.mem.Allocator) !bson.BSONValue {
    return switch (value) {
        .string => .{ .string = .{ .value = try allocator.dupe(u8, value.string.value) } },
        .document => .{ .document = try copyDocument(value.document, allocator) },
        .array => .{ .array = try copyDocument(value.array, allocator) },
        .binary => .{ .binary = .{
            .value = try allocator.dupe(u8, value.binary.value),
            .subtype = value.binary.subtype,
        } },
        else => value,
    };
}

fn hasBsonEncode(comptime T: type) bool {
    return switch (@typeInfo(T)) {
        .@"struct", .@"union", .@"enum" => @hasDecl(T, "bsonEncode"),
        else => false,
    };
}

fn hasBsonDecode(comptime T: type) bool {
    return switch (@typeInfo(T)) {
        .@"struct", .@"union", .@"enum" => @hasDecl(T, "bsonDecode"),
        else => false,
    };
}

fn callBsonEncode(comptime T: type, value: T, doc: *bson.DocEncoder) (Error || std.mem.Allocator.Error)!void {
    const encode_fn = @field(T, "bsonEncode");
    const fn_info = @typeInfo(@TypeOf(encode_fn)).@"fn";
    const self_type = fn_info.params[0].type orelse @compileError("bsonEncode must accept self as its first parameter");

    if (self_type == T) {
        return try encode_fn(value, doc);
    }
    if (self_type == *const T) {
        const temp = value;
        return try encode_fn(&temp, doc);
    }
    if (self_type == *T) {
        var temp = value;
        return try encode_fn(&temp, doc);
    }

    @compileError("bsonEncode must have signature fn(self: @This() or *const/@This(), doc: *bson.DocEncoder) !void");
}

fn callBsonDecode(comptime T: type, doc: bson.BSONDocument, allocator: std.mem.Allocator) (Error || std.mem.Allocator.Error)!T {
    return try @field(T, "bsonDecode")(doc, allocator);
}

test "serialize/parse struct roundtrip" {
    const T = struct {
        name: []const u8,
        age: i32,
    };

    var doc = try serialize(T{ .name = "Alice", .age = 32 }, std.testing.allocator);
    defer doc.deinit(std.testing.allocator);

    var parsed = try parse(T, doc, std.testing.allocator);
    defer parsed.deinit();

    try std.testing.expectEqualStrings("Alice", parsed.value.name);
    try std.testing.expectEqual(@as(i32, 32), parsed.value.age);
}

test "parse optional missing field" {
    const T = struct {
        name: []const u8,
        age: ?i32,
    };

    var doc = try serialize(.{ .name = "Alice", .age = @as(?i32, null) }, std.testing.allocator);
    defer doc.deinit(std.testing.allocator);

    var parsed = try parse(T, doc, std.testing.allocator);
    defer parsed.deinit();

    try std.testing.expectEqualStrings("Alice", parsed.value.name);
    try std.testing.expectEqual(@as(?i32, null), parsed.value.age);
}

test "serialize/parse nested struct roundtrip" {
    const T = struct {
        name: []const u8,
        profile: struct {
            age: i32,
            city: []const u8,
        },
    };

    var doc = try serialize(T{
        .name = "Alice",
        .profile = .{
            .age = 32,
            .city = "Paris",
        },
    }, std.testing.allocator);
    defer doc.deinit(std.testing.allocator);

    var parsed = try parse(T, doc, std.testing.allocator);
    defer parsed.deinit();

    try std.testing.expectEqualStrings("Alice", parsed.value.name);
    try std.testing.expectEqual(@as(i32, 32), parsed.value.profile.age);
    try std.testing.expectEqualStrings("Paris", parsed.value.profile.city);
}

test "serialize/parse nested fixed array in struct" {
    const T = struct {
        name: []const u8,
        profile: struct {
            age: i32,
            scores: [3]i32,
        },
    };

    var doc = try serialize(T{
        .name = "Alice",
        .profile = .{
            .age = 32,
            .scores = .{ 10, 20, 30 },
        },
    }, std.testing.allocator);
    defer doc.deinit(std.testing.allocator);

    var parsed = try parse(T, doc, std.testing.allocator);
    defer parsed.deinit();

    try std.testing.expectEqualStrings("Alice", parsed.value.name);
    try std.testing.expectEqual(@as(i32, 32), parsed.value.profile.age);
    try std.testing.expectEqual(@as(i32, 10), parsed.value.profile.scores[0]);
    try std.testing.expectEqual(@as(i32, 20), parsed.value.profile.scores[1]);
    try std.testing.expectEqual(@as(i32, 30), parsed.value.profile.scores[2]);
}

test "serialize/parse nested slice in struct" {
    const T = struct {
        name: []const u8,
        profile: struct {
            tags: []const []const u8,
        },
    };

    const tags = [_][]const u8{ "db", "zig", "bson" };
    var doc = try serialize(T{
        .name = "Alice",
        .profile = .{
            .tags = tags[0..],
        },
    }, std.testing.allocator);
    defer doc.deinit(std.testing.allocator);

    var parsed = try parse(T, doc, std.testing.allocator);
    defer parsed.deinit();

    try std.testing.expectEqualStrings("Alice", parsed.value.name);
    try std.testing.expectEqual(@as(usize, 3), parsed.value.profile.tags.len);
    try std.testing.expectEqualStrings("db", parsed.value.profile.tags[0]);
    try std.testing.expectEqualStrings("zig", parsed.value.profile.tags[1]);
    try std.testing.expectEqualStrings("bson", parsed.value.profile.tags[2]);
}

test "serialize/parse enum roundtrip" {
    const Color = enum { red, green, blue };
    const T = struct { color: Color };

    var doc = try serialize(T{ .color = .green }, std.testing.allocator);
    defer doc.deinit(std.testing.allocator);

    var parsed = try parse(T, doc, std.testing.allocator);
    defer parsed.deinit();

    try std.testing.expectEqual(Color.green, parsed.value.color);
}

test "serialize/parse tagged union void variant roundtrip" {
    const Mode = union(enum) { all, periodic: u32, manual };
    const T = struct { mode: Mode };

    var doc = try serialize(T{ .mode = .all }, std.testing.allocator);
    defer doc.deinit(std.testing.allocator);

    var parsed = try parse(T, doc, std.testing.allocator);
    defer parsed.deinit();

    try std.testing.expectEqual(Mode.all, parsed.value.mode);
}

test "serialize/parse tagged union payload variant roundtrip" {
    const Mode = union(enum) { all, periodic: u32, manual };
    const T = struct { mode: Mode };

    var doc = try serialize(T{ .mode = .{ .periodic = 50 } }, std.testing.allocator);
    defer doc.deinit(std.testing.allocator);

    var parsed = try parse(T, doc, std.testing.allocator);
    defer parsed.deinit();

    try std.testing.expectEqual(Mode{ .periodic = 50 }, parsed.value.mode);
}

test "serialize/parse raw bson document field" {
    const Inner = try serialize(.{ .city = "Paris" }, std.testing.allocator);
    defer Inner.deinit(std.testing.allocator);

    const T = struct {
        name: []const u8,
        raw: bson.BSONDocument,
    };

    var doc = try serialize(T{
        .name = "Alice",
        .raw = Inner,
    }, std.testing.allocator);
    defer doc.deinit(std.testing.allocator);

    var parsed = try parse(T, doc, std.testing.allocator);
    defer parsed.deinit();

    try std.testing.expectEqualStrings("Alice", parsed.value.name);
    try std.testing.expectEqualStrings("Paris", parsed.value.raw.get("city").?.string.value);
}

test "custom bson hooks override reflection" {
    const Custom = struct {
        name: []const u8,

        pub fn bsonEncode(self: @This(), doc: *bson.DocEncoder) !void {
            try doc.put("full_name", self.name);
        }

        pub fn bsonDecode(doc: bson.BSONDocument, allocator: std.mem.Allocator) !@This() {
            return .{
                .name = try allocator.dupe(u8, doc.get("full_name").?.string.value),
            };
        }
    };

    var doc = try serialize(Custom{ .name = "Alice" }, std.testing.allocator);
    defer doc.deinit(std.testing.allocator);

    try std.testing.expect(doc.get("name") == null);
    try std.testing.expectEqualStrings("Alice", doc.get("full_name").?.string.value);

    var parsed = try parse(Custom, doc, std.testing.allocator);
    defer parsed.deinit();

    try std.testing.expectEqualStrings("Alice", parsed.value.name);
}

test "custom bson hooks work inside reflected outer struct" {
    const Custom = struct {
        value: []const u8,

        pub fn bsonEncode(self: @This(), doc: *bson.DocEncoder) !void {
            try doc.put("wrapped", self.value);
        }

        pub fn bsonDecode(doc: bson.BSONDocument, allocator: std.mem.Allocator) !@This() {
            return .{
                .value = try allocator.dupe(u8, doc.get("wrapped").?.string.value),
            };
        }
    };

    const Outer = struct {
        id: i32,
        custom: Custom,
    };

    var doc = try serialize(Outer{
        .id = 7,
        .custom = .{ .value = "hello" },
    }, std.testing.allocator);
    defer doc.deinit(std.testing.allocator);

    try std.testing.expectEqualStrings("hello", doc.getPath("custom.wrapped").?.string.value);

    var parsed = try parse(Outer, doc, std.testing.allocator);
    defer parsed.deinit();

    try std.testing.expectEqual(@as(i32, 7), parsed.value.id);
    try std.testing.expectEqualStrings("hello", parsed.value.custom.value);
}
