const std = @import("std");
const crypto = std.crypto;

// var process_rand = rand: {
//     var arr = [5]u8{ 0, 0, 0, 0, 0 };
//     crypto.random.bytes(&arr);
//     break :rand arr;
// };

pub const ObjectId = struct {
    buffer: [12]u8, // 12

    pub fn init() ObjectId {
        const time = @as(i32, @truncate(std.time.timestamp()));
        var rand = [8]u8{ 0, 0, 0, 0, 0, 0, 0, 0 };
        crypto.random.bytes(&rand);
        var buffer: [12]u8 = undefined;
        std.mem.writeInt(i32, buffer[0..4], time, .big);
        @memcpy(buffer[4..], &rand);
        return ObjectId{ .buffer = buffer };
    }

    pub fn parseString(str: []const u8) !ObjectId {
        var buffer: [12]u8 = @splat(0);

        _ = try std.fmt.hexToBytes(&buffer, str[0..24]);
        return ObjectId{ .buffer = buffer };
    }

    /// Returns time in seconds since epoch
    pub fn timestamp(self: ObjectId) u32 {
        return std.mem.readInt(u32, self.buffer[0..4], .big);
    }

    const hexCharset = "0123456789abcdef";

    pub fn toString(self: ObjectId) [24]u8 {
        var res: [24]u8 = @splat(0);
        inline for (self.buffer, 0..) |byte, i| {
            res[i * 2 + 0] = hexCharset[byte >> 4];
            res[i * 2 + 1] = hexCharset[byte & 0xF];
        }
        return res;
    }

    pub inline fn toInt(self: ObjectId) u96 {
        return std.mem.readInt(u96, self.buffer[0..12], .little);
    }

    pub fn fromInt(value: u96) ObjectId {
        var buffer: [12]u8 = undefined;
        std.mem.writeInt(u96, &buffer, value, .little);
        return ObjectId{ .buffer = buffer };
    }
};

test "test parse string" {
    const objid = try ObjectId.parseString("507c7f79bcf86cd7994f6c0e");
    try std.testing.expectEqualSlices(
        u8,
        "\x50\x7c\x7f\x79\xbc\xf8\x6c\xd7\x99\x4f\x6c\x0e",
        &objid.buffer,
    );
}

test "test timestamp get" {
    const objid = try ObjectId.parseString("507c7f79bcf86cd7994f6c0e");
    // std.debug.print("{x} {x}", .{ 0x507c7f79, objid.timestamp() });
    try std.testing.expectEqual(0x507c7f79, objid.timestamp());
}

test "Test2" {
    _ = ObjectId.init();
}

test "test gen str" {
    const objid = try ObjectId.parseString(@constCast("507c7f79bcf86cd7994f6c0e"));
    try std.testing.expectEqualSlices(
        u8,
        "507c7f79bcf86cd7994f6c0e",
        &objid.toString(),
    );
}
