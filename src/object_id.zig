const std = @import("std");
const crypto = std.crypto;

// var process_rand = rand: {
//     var arr = [5]u8{ 0, 0, 0, 0, 0 };
//     crypto.random.bytes(&arr);
//     break :rand arr;
// };

pub const ObjectId = struct {
    buffer: []u8, // 12

    pub fn init() ObjectId {
        const time = @as(i32, @truncate(std.time.timestamp()));
        var rand = [8]u8{ 0, 0, 0, 0, 0, 0, 0, 0 };
        crypto.random.bytes(&rand);
        var buffer: [12]u8 = undefined;
        std.mem.writeInt(i32, buffer[0..4], time, .big);
        @memcpy(buffer[4..], &rand);
        return ObjectId{ .buffer = buffer };
    }

    pub fn parseString(str: []const u8) ObjectId {
        var buffer: [12]u8 = [12]u8{ 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 };
        for (str, 0..) |digit, i| {
            const idx = @divFloor(i, 2);
            const rd = @rem(i, 2);

            if (rd == 1) {
                buffer[idx] = buffer[idx] << 4;
            }
            const digit_normalized = switch (digit) {
                '0'...'9' => digit - '0',
                'a'...'f' => digit - 'a' + 10,
                else => unreachable,
            };
            buffer[idx] += digit_normalized;
            // std.debug.print("{x} {x}\n", .{ buffer, digit_normalized });
        }
        return ObjectId{ .buffer = buffer };
    }

    /// Returns time in seconds since epoch
    pub fn timestamp(self: ObjectId) u32 {
        return std.mem.readInt(u32, self.buffer[0..4], .big);
    }

    pub fn toString(self: ObjectId) [24]u8 {
        var res = ([_]u8{0} ** 24);
        inline for (self.buffer, 0..) |byte, i| {
            const high = byte >> 4;
            const low = byte & 0xF;
            res[i * 2] = if (high < 0xa) high + '0' else high + 'a' - 10;
            res[i * 2 + 1] = if (low < 0xa) low + '0' else low + 'a' - 10;
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
    const objid = ObjectId.parseString("507c7f79bcf86cd7994f6c0e");
    try std.testing.expectEqualSlices(
        u8,
        "\x50\x7c\x7f\x79\xbc\xf8\x6c\xd7\x99\x4f\x6c\x0e",
        &objid.buffer,
    );
}

test "test timestamp get" {
    const objid = ObjectId.parseString("507c7f79bcf86cd7994f6c0e");
    // std.debug.print("{x} {x}", .{ 0x507c7f79, objid.timestamp() });
    try std.testing.expectEqual(0x507c7f79, objid.timestamp());
}

test "Test2" {
    _ = ObjectId.init();
}

test "test gen str" {
    const objid = ObjectId.parseString(@constCast("507c7f79bcf86cd7994f6c0e"));
    try std.testing.expectEqualSlices(
        u8,
        "507c7f79bcf86cd7994f6c0e",
        &objid.toString(),
    );
}
