const std = @import("std");
const crypto = std.crypto;

var process_rand = rand: {
    const arr = [5]u8{ 0, 0, 0, 0, 0 };
    crypto.random.bytes(arr);
    break :rand arr;
};

pub const ObjectId = struct {
    buffer: [12:0]u8,

    pub fn init() ObjectId {
        const time = @as(i32, @truncate(std.time.timestamp()));
        const rand = [3]u8{ 0, 0, 0 };
        crypto.random.bytes(rand);
        return ObjectId{ .buffer = .{time ++ process_rand ++ rand} };
    }

    pub fn parseString(str: *[24]u8) ObjectId {
        var buffer: [12:0]u8 = [12:0]u8{ 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 };
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

    // returns time in seconds
    pub fn timestamp(self: ObjectId) i32 {
        const beBuffer = &[_]u8{ self.buffer[3], self.buffer[2], self.buffer[1], self.buffer[0] };
        return std.mem.bytesToValue(i32, beBuffer);
    }

    pub fn toString(self: ObjectId) [24:0]u8 {
        var res = ([_:0]u8{0} ** 24);
        inline for (self.buffer, 0..) |byte, i| {
            const high = byte >> 4;
            const low = byte & 0xF;
            res[i * 2] = (high + '0') * @intFromBool(high < 0xa) | (high + 'a' - 10) * @intFromBool(high > 0xa);
            res[i * 2 + 1] = (low + '0') * @intFromBool(low < 0xa) | (low + 'a' - 10) * @intFromBool(low > 0xa);
        }
        return res;
    }
};

test "test parse string" {
    const objid = ObjectId.parseString(@constCast("507c7f79bcf86cd7994f6c0e"));
    try std.testing.expectEqualSlices(
        u8,
        "\x50\x7c\x7f\x79\xbc\xf8\x6c\xd7\x99\x4f\x6c\x0e",
        &objid.buffer,
    );
}

test "test timestamp get" {
    const objid = ObjectId.parseString(@constCast("507c7f79bcf86cd7994f6c0e"));
    // std.debug.print("{x} {x}", .{ 0x507c7f79, objid.timestamp() });
    try std.testing.expectEqual(0x507c7f79, objid.timestamp());
}

test "Test" {
    std.debug.print("{x}", .{@as(i32, @truncate(std.time.timestamp()))});
}

test "test gen str" {
    const objid = ObjectId.parseString(@constCast("507c7f79bcf86cd7994f6c0e"));
    try std.testing.expectEqualSlices(
        u8,
        "507c7f79bcf86cd7994f6c0e",
        &objid.toString(),
    );
}
