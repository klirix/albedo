const std = @import("std");
const bson = @import("bson.zig");

const Allocator = std.mem.Allocator;

pub const UpdateProgram = struct {
    raw: bson.BSONDocument,

    pub const ApplyError = error{
        InvalidUpdateProgram,
        InvalidUpdateStage,
        InvalidUpdateOperator,
        InvalidUpdatePath,
        InvalidUpdateValue,
        InvalidExpression,
        InvalidExpressionArgument,
        InvalidExpressionArgumentCount,
        InvalidDateTime,
        IntegerOverflow,
        OutOfMemory,
    };

    pub const UpdateProgramParsingErrors = ApplyError;

    const OwnedValue = struct {
        value: bson.BSONValue,

        fn deinit(self: *const OwnedValue, ally: Allocator) void {
            freeOwnedValue(ally, self.value);
        }

        fn literal(ally: Allocator, value: bson.BSONValue) ApplyError!OwnedValue {
            const owned = dupeOwnedValue(ally, value) catch return error.OutOfMemory;
            return .{ .value = owned };
        }
    };

    const Numeric = union(enum) {
        int32: i32,
        int64: i64,
        double: f64,

        fn fromValue(value: bson.BSONValue) ?Numeric {
            return switch (value) {
                .int32 => |v| .{ .int32 = v.value },
                .int64 => |v| .{ .int64 = v.value },
                .double => |v| .{ .double = v.value },
                else => null,
            };
        }

        fn toValue(self: Numeric) bson.BSONValue {
            return switch (self) {
                .int32 => |v| .{ .int32 = .{ .value = v } },
                .int64 => |v| .{ .int64 = .{ .value = v } },
                .double => |v| .{ .double = .{ .value = v } },
            };
        }

        fn asI64(self: Numeric) i64 {
            return switch (self) {
                .int32 => |v| v,
                .int64 => |v| v,
                .double => unreachable,
            };
        }
    };

    pub fn parseRaw(ally: Allocator, raw_program: []const u8) UpdateProgramParsingErrors!UpdateProgram {
        return parse(ally, bson.BSONDocument.init(raw_program));
    }

    pub fn parse(ally: Allocator, raw_program: bson.BSONDocument) UpdateProgramParsingErrors!UpdateProgram {
        try validateProgram(raw_program);
        return .{ .raw = try raw_program.clone(ally) };
    }

    pub fn deinit(self: *UpdateProgram, ally: Allocator) void {
        self.raw.deinit(ally);
    }

    pub fn apply(self: UpdateProgram, ally: Allocator, source: bson.BSONDocument) ApplyError!bson.BSONDocument {
        const now_raw = std.time.milliTimestamp();
        const now_ms: u64 = if (now_raw < 0) 0 else @intCast(now_raw);
        return self.applyWithNowMillis(ally, source, now_ms);
    }

    fn applyWithNowMillis(self: UpdateProgram, ally: Allocator, source: bson.BSONDocument, now_ms: u64) ApplyError!bson.BSONDocument {
        var current = try source.clone(ally);
        errdefer current.deinit(ally);

        if (looksLikeStageArray(self.raw)) {
            var iter = self.raw.iter();
            while (iter.next()) |pair| {
                if (pair.value != .document) return error.InvalidUpdateStage;
                const next = try applyStage(ally, current, pair.value.document, now_ms);
                current.deinit(ally);
                current = next;
            }
        } else {
            const next = try applyStage(ally, current, self.raw, now_ms);
            current.deinit(ally);
            current = next;
        }

        return current;
    }

    fn validateProgram(raw_program: bson.BSONDocument) UpdateProgramParsingErrors!void {
        if (raw_program.keyNumber() == 0) return error.InvalidUpdateProgram;

        if (looksLikeStageArray(raw_program)) {
            var iter = raw_program.iter();
            while (iter.next()) |pair| {
                if (pair.value != .document) return error.InvalidUpdateStage;
                try validateStage(pair.value.document);
            }
            return;
        }

        try validateStage(raw_program);
    }

    fn validateStage(stage: bson.BSONDocument) UpdateProgramParsingErrors!void {
        if (stage.keyNumber() == 0) return error.InvalidUpdateStage;

        var iter = stage.iter();
        while (iter.next()) |pair| {
            if (std.mem.eql(u8, pair.key, "$set")) {
                if (pair.value != .document) return error.InvalidUpdateValue;
                try validateSetMap(pair.value.document);
                continue;
            }
            if (std.mem.eql(u8, pair.key, "$unset")) {
                try validateUnsetValue(pair.value);
                continue;
            }
            if (pair.key.len != 0 and pair.key[0] == '$') return error.InvalidUpdateOperator;

            try validatePath(pair.key);
            try validateExpression(pair.value);
        }
    }

    fn validateSetMap(set_doc: bson.BSONDocument) UpdateProgramParsingErrors!void {
        var iter = set_doc.iter();
        while (iter.next()) |pair| {
            try validatePath(pair.key);
            try validateExpression(pair.value);
        }
    }

    fn validateUnsetValue(value: bson.BSONValue) UpdateProgramParsingErrors!void {
        switch (value) {
            .string => |str| try validatePath(str.value),
            .array => |arr| {
                var iter = arr.iter();
                while (iter.next()) |pair| {
                    if (pair.value != .string) return error.InvalidUpdateValue;
                    try validatePath(pair.value.string.value);
                }
            },
            .document => |doc| {
                if (looksLikeStageArray(doc)) {
                    var iter = doc.iter();
                    while (iter.next()) |pair| {
                        if (pair.value != .string) return error.InvalidUpdateValue;
                        try validatePath(pair.value.string.value);
                    }
                    return;
                }
                return error.InvalidUpdateValue;
            },
            else => return error.InvalidUpdateValue,
        }
    }

    fn validateExpression(raw: bson.BSONValue) UpdateProgramParsingErrors!void {
        switch (raw) {
            .string => |str| {
                if (std.mem.eql(u8, str.value, "$$now")) return;
                if (std.mem.startsWith(u8, str.value, "$.")) {
                    if (str.value.len <= 2) return error.InvalidExpression;
                }
            },
            .document => |doc| {
                if (try validateOperatorDocument(doc)) return;
                var iter = doc.iter();
                while (iter.next()) |pair| {
                    try validateExpression(pair.value);
                }
            },
            .array => |arr| {
                var iter = arr.iter();
                while (iter.next()) |pair| {
                    try validateExpression(pair.value);
                }
            },
            else => {},
        }
    }

    fn validateOperatorDocument(raw: bson.BSONDocument) UpdateProgramParsingErrors!bool {
        if (raw.keyNumber() != 1) return false;

        var iter = raw.iter();
        const pair = iter.next() orelse return false;
        if (std.mem.eql(u8, pair.key, "$plus")) {
            try validateVariadicExpressionList(pair.value, true);
            return true;
        }
        if (std.mem.eql(u8, pair.key, "$minus")) {
            try validateVariadicExpressionList(pair.value, true);
            return true;
        }
        if (std.mem.eql(u8, pair.key, "$concat")) {
            try validateVariadicExpressionList(pair.value, false);
            return true;
        }
        if (std.mem.eql(u8, pair.key, "$isoDateTime")) {
            try validateExpression(pair.value);
            return true;
        }
        if (pair.key.len != 0 and pair.key[0] == '$') return error.InvalidExpression;
        return false;
    }

    fn validateVariadicExpressionList(value: bson.BSONValue, require_two: bool) UpdateProgramParsingErrors!void {
        const args = arrayLikeDocument(value) orelse return error.InvalidExpressionArgument;
        if (require_two) {
            if (args.keyNumber() < 2) return error.InvalidExpressionArgumentCount;
        } else if (args.keyNumber() == 0) {
            return error.InvalidExpressionArgumentCount;
        }

        var iter = args.iter();
        while (iter.next()) |pair| {
            try validateExpression(pair.value);
        }
    }

    fn applyStage(
        ally: Allocator,
        stage_source: bson.BSONDocument,
        stage: bson.BSONDocument,
        now_ms: u64,
    ) ApplyError!bson.BSONDocument {
        if (stage.keyNumber() == 0) return error.InvalidUpdateStage;

        var editor = bson.Editor.init(ally, stage_source);
        defer editor.deinit();

        var iter = stage.iter();
        while (iter.next()) |pair| {
            if (std.mem.eql(u8, pair.key, "$set")) {
                if (pair.value != .document) return error.InvalidUpdateValue;
                try applySetMap(ally, &editor, stage_source, pair.value.document, now_ms);
                continue;
            }
            if (std.mem.eql(u8, pair.key, "$unset")) {
                try applyUnsetValue(&editor, pair.value);
                continue;
            }
            if (pair.key.len != 0 and pair.key[0] == '$') {
                return error.InvalidUpdateOperator;
            }

            try validatePath(pair.key);
            var evaluated = try evalValue(ally, stage_source, pair.value, now_ms);
            defer evaluated.deinit(ally);
            editor.setPathValue(pair.key, evaluated.value) catch |err| switch (err) {
                error.OutOfMemory => return error.OutOfMemory,
                error.InvalidPath => return error.InvalidUpdatePath,
            };
        }

        return try editor.finish();
    }

    fn applySetMap(
        ally: Allocator,
        editor: *bson.Editor,
        stage_source: bson.BSONDocument,
        set_doc: bson.BSONDocument,
        now_ms: u64,
    ) ApplyError!void {
        var iter = set_doc.iter();
        while (iter.next()) |pair| {
            try validatePath(pair.key);
            var evaluated = try evalValue(ally, stage_source, pair.value, now_ms);
            defer evaluated.deinit(ally);
            editor.setPathValue(pair.key, evaluated.value) catch |err| switch (err) {
                error.OutOfMemory => return error.OutOfMemory,
                error.InvalidPath => return error.InvalidUpdatePath,
            };
        }
    }

    fn applyUnsetValue(editor: *bson.Editor, value: bson.BSONValue) ApplyError!void {
        switch (value) {
            .string => |str| {
                try validatePath(str.value);
                editor.unsetPath(str.value) catch |err| switch (err) {
                    error.OutOfMemory => return error.OutOfMemory,
                    error.InvalidPath => return error.InvalidUpdatePath,
                };
            },
            .array => |arr| {
                var iter = arr.iter();
                while (iter.next()) |pair| {
                    if (pair.value != .string) return error.InvalidUpdateValue;
                    try validatePath(pair.value.string.value);
                    editor.unsetPath(pair.value.string.value) catch |err| switch (err) {
                        error.OutOfMemory => return error.OutOfMemory,
                        error.InvalidPath => return error.InvalidUpdatePath,
                    };
                }
            },
            .document => |doc| {
                var iter = doc.iter();
                while (iter.next()) |pair| {
                    if (pair.value != .string) return error.InvalidUpdateValue;
                    try validatePath(pair.value.string.value);
                    editor.unsetPath(pair.value.string.value) catch |err| switch (err) {
                        error.OutOfMemory => return error.OutOfMemory,
                        error.InvalidPath => return error.InvalidUpdatePath,
                    };
                }
            },
            else => return error.InvalidUpdateValue,
        }
    }

    fn evalValue(
        ally: Allocator,
        stage_source: bson.BSONDocument,
        raw: bson.BSONValue,
        now_ms: u64,
    ) ApplyError!OwnedValue {
        return switch (raw) {
            .string => |str| evalString(ally, stage_source, str.value, now_ms),
            .document => |doc| try evalDocument(ally, stage_source, doc, now_ms),
            .array => |arr| try evalLiteralArray(ally, stage_source, arr, now_ms),
            else => OwnedValue.literal(ally, raw),
        };
    }

    fn evalString(
        ally: Allocator,
        stage_source: bson.BSONDocument,
        raw: []const u8,
        now_ms: u64,
    ) ApplyError!OwnedValue {
        if (std.mem.eql(u8, raw, "$$now")) {
            return .{ .value = .{ .datetime = .{ .value = now_ms } } };
        }
        if (std.mem.startsWith(u8, raw, "$.") and raw.len > 2) {
            const resolved = stage_source.getPath(raw[2..]) orelse bson.BSONValue{ .null = .{} };
            return OwnedValue.literal(ally, resolved);
        }
        return OwnedValue.literal(ally, .{ .string = .{ .value = raw } });
    }

    fn evalDocument(
        ally: Allocator,
        stage_source: bson.BSONDocument,
        raw: bson.BSONDocument,
        now_ms: u64,
    ) ApplyError!OwnedValue {
        if (try evalOperatorDocument(ally, stage_source, raw, now_ms)) |value| {
            return value;
        }
        return evalLiteralDocument(ally, stage_source, raw, now_ms);
    }

    fn evalOperatorDocument(
        ally: Allocator,
        stage_source: bson.BSONDocument,
        raw: bson.BSONDocument,
        now_ms: u64,
    ) ApplyError!?OwnedValue {
        if (raw.keyNumber() != 1) return null;

        var iter = raw.iter();
        const pair = iter.next() orelse return null;
        if (std.mem.eql(u8, pair.key, "$plus")) return try evalPlus(ally, stage_source, pair.value, now_ms);
        if (std.mem.eql(u8, pair.key, "$minus")) return try evalMinus(ally, stage_source, pair.value, now_ms);
        if (std.mem.eql(u8, pair.key, "$concat")) return try evalConcat(ally, stage_source, pair.value, now_ms);
        if (std.mem.eql(u8, pair.key, "$isoDateTime")) return try evalIsoDateTime(ally, stage_source, pair.value, now_ms);
        if (pair.key.len != 0 and pair.key[0] == '$') return error.InvalidExpression;
        return null;
    }

    fn evalLiteralDocument(
        ally: Allocator,
        stage_source: bson.BSONDocument,
        raw: bson.BSONDocument,
        now_ms: u64,
    ) ApplyError!OwnedValue {
        var builder = try bson.Builder.init(ally);
        defer builder.deinit();

        var iter = raw.iter();
        while (iter.next()) |pair| {
            var evaluated = try evalValue(ally, stage_source, pair.value, now_ms);
            defer evaluated.deinit(ally);
            builder.putValue(pair.key, evaluated.value) catch |err| switch (err) {
                error.OutOfMemory => return error.OutOfMemory,
                else => return error.InvalidExpression,
            };
        }

        const doc = builder.finish() catch |err| switch (err) {
            error.OutOfMemory => return error.OutOfMemory,
            else => return error.InvalidExpression,
        };
        return .{ .value = .{ .document = doc } };
    }

    fn evalLiteralArray(
        ally: Allocator,
        stage_source: bson.BSONDocument,
        raw: bson.BSONDocument,
        now_ms: u64,
    ) ApplyError!OwnedValue {
        var builder = try bson.Builder.init(ally);
        defer builder.deinit();

        var index: usize = 0;
        var iter = raw.iter();
        while (iter.next()) |pair| : (index += 1) {
            var key_buf: [32]u8 = undefined;
            const key = std.fmt.bufPrint(&key_buf, "{d}", .{index}) catch unreachable;
            var evaluated = try evalValue(ally, stage_source, pair.value, now_ms);
            defer evaluated.deinit(ally);
            builder.putValue(key, evaluated.value) catch |err| switch (err) {
                error.OutOfMemory => return error.OutOfMemory,
                else => return error.InvalidExpression,
            };
        }

        const doc = builder.finish() catch |err| switch (err) {
            error.OutOfMemory => return error.OutOfMemory,
            else => return error.InvalidExpression,
        };
        return .{ .value = .{ .array = doc } };
    }

    fn evalPlus(
        ally: Allocator,
        stage_source: bson.BSONDocument,
        operand: bson.BSONValue,
        now_ms: u64,
    ) ApplyError!OwnedValue {
        const args = arrayLikeDocument(operand) orelse return error.InvalidExpressionArgument;
        if (args.keyNumber() < 2) return error.InvalidExpressionArgumentCount;

        var iter = args.iter();
        const first = iter.next() orelse return error.InvalidExpressionArgumentCount;
        var acc = try evalNumericOperand(ally, stage_source, first.value, now_ms);
        while (iter.next()) |pair| {
            const rhs = try evalNumericOperand(ally, stage_source, pair.value, now_ms);
            acc = try addNumeric(acc, rhs);
        }

        return .{ .value = acc.toValue() };
    }

    fn evalMinus(
        ally: Allocator,
        stage_source: bson.BSONDocument,
        operand: bson.BSONValue,
        now_ms: u64,
    ) ApplyError!OwnedValue {
        const args = arrayLikeDocument(operand) orelse return error.InvalidExpressionArgument;
        if (args.keyNumber() < 2) return error.InvalidExpressionArgumentCount;

        var iter = args.iter();
        const first = iter.next() orelse return error.InvalidExpressionArgumentCount;
        var acc = try evalNumericOperand(ally, stage_source, first.value, now_ms);
        while (iter.next()) |pair| {
            const rhs = try evalNumericOperand(ally, stage_source, pair.value, now_ms);
            acc = try subNumeric(acc, rhs);
        }

        return .{ .value = acc.toValue() };
    }

    fn evalConcat(
        ally: Allocator,
        stage_source: bson.BSONDocument,
        operand: bson.BSONValue,
        now_ms: u64,
    ) ApplyError!OwnedValue {
        const args = arrayLikeDocument(operand) orelse return error.InvalidExpressionArgument;
        if (args.keyNumber() == 0) return error.InvalidExpressionArgumentCount;

        var buffer = std.ArrayList(u8){};
        defer buffer.deinit(ally);

        var iter = args.iter();
        while (iter.next()) |pair| {
            var evaluated = try evalValue(ally, stage_source, pair.value, now_ms);
            defer evaluated.deinit(ally);
            if (evaluated.value != .string) return error.InvalidExpressionArgument;
            try buffer.appendSlice(ally, evaluated.value.string.value);
        }

        return .{ .value = .{ .string = .{ .value = try buffer.toOwnedSlice(ally) } } };
    }

    fn evalIsoDateTime(
        ally: Allocator,
        stage_source: bson.BSONDocument,
        operand: bson.BSONValue,
        now_ms: u64,
    ) ApplyError!OwnedValue {
        var evaluated = try evalValue(ally, stage_source, operand, now_ms);
        defer evaluated.deinit(ally);

        return switch (evaluated.value) {
            .datetime => |dt| .{ .value = .{ .datetime = dt } },
            .string => |str| .{ .value = .{ .datetime = .{ .value = try parseIsoDateTimeMillis(str.value) } } },
            else => error.InvalidExpressionArgument,
        };
    }

    fn evalNumericOperand(
        ally: Allocator,
        stage_source: bson.BSONDocument,
        raw: bson.BSONValue,
        now_ms: u64,
    ) ApplyError!Numeric {
        var evaluated = try evalValue(ally, stage_source, raw, now_ms);
        defer evaluated.deinit(ally);
        return Numeric.fromValue(evaluated.value) orelse error.InvalidExpressionArgument;
    }

    fn addNumeric(a: Numeric, b: Numeric) ApplyError!Numeric {
        if (a == .double or b == .double) {
            const lhs = switch (a) {
                .double => |v| v,
                .int32 => |v| @as(f64, @floatFromInt(v)),
                .int64 => |v| @as(f64, @floatFromInt(v)),
            };
            const rhs = switch (b) {
                .double => |v| v,
                .int32 => |v| @as(f64, @floatFromInt(v)),
                .int64 => |v| @as(f64, @floatFromInt(v)),
            };
            return .{ .double = lhs + rhs };
        }

        const sum = std.math.add(i64, a.asI64(), b.asI64()) catch return error.IntegerOverflow;
        if (a == .int32 and b == .int32) {
            if (std.math.cast(i32, sum)) |narrow| {
                return .{ .int32 = narrow };
            }
        }
        return .{ .int64 = sum };
    }

    fn subNumeric(a: Numeric, b: Numeric) ApplyError!Numeric {
        if (a == .double or b == .double) {
            const lhs = switch (a) {
                .double => |v| v,
                .int32 => |v| @as(f64, @floatFromInt(v)),
                .int64 => |v| @as(f64, @floatFromInt(v)),
            };
            const rhs = switch (b) {
                .double => |v| v,
                .int32 => |v| @as(f64, @floatFromInt(v)),
                .int64 => |v| @as(f64, @floatFromInt(v)),
            };
            return .{ .double = lhs - rhs };
        }

        const diff = std.math.sub(i64, a.asI64(), b.asI64()) catch return error.IntegerOverflow;
        if (a == .int32 and b == .int32) {
            if (std.math.cast(i32, diff)) |narrow| {
                return .{ .int32 = narrow };
            }
        }
        return .{ .int64 = diff };
    }

    fn validatePath(path: []const u8) ApplyError!void {
        if (path.len == 0 or path[0] == '.' or path[path.len - 1] == '.') {
            return error.InvalidUpdatePath;
        }
        if (std.mem.indexOf(u8, path, "..") != null) return error.InvalidUpdatePath;
    }

    fn arrayLikeDocument(value: bson.BSONValue) ?bson.BSONDocument {
        return switch (value) {
            .array => |arr| arr,
            .document => |doc| if (looksLikeStageArray(doc)) doc else null,
            else => null,
        };
    }

    fn looksLikeStageArray(doc: bson.BSONDocument) bool {
        if (doc.keyNumber() == 0) return false;

        var expected: usize = 0;
        var iter = doc.iter();
        while (iter.next()) |pair| : (expected += 1) {
            const idx = std.fmt.parseUnsigned(usize, pair.key, 10) catch return false;
            if (idx != expected) return false;
        }
        return true;
    }

    fn parseIsoDateTimeMillis(raw: []const u8) ApplyError!u64 {
        var index: usize = 0;

        const year = try parseFixed(raw, &index, 4);
        if (!consume(raw, &index, '-')) return error.InvalidDateTime;
        const month = try parseFixed(raw, &index, 2);
        if (!consume(raw, &index, '-')) return error.InvalidDateTime;
        const day = try parseFixed(raw, &index, 2);
        if (!(consume(raw, &index, 'T') or consume(raw, &index, 't'))) return error.InvalidDateTime;
        const hour = try parseFixed(raw, &index, 2);
        if (!consume(raw, &index, ':')) return error.InvalidDateTime;
        const minute = try parseFixed(raw, &index, 2);
        if (!consume(raw, &index, ':')) return error.InvalidDateTime;
        const second = try parseFixed(raw, &index, 2);

        var millis: u32 = 0;
        if (consume(raw, &index, '.')) {
            const frac_start = index;
            while (index < raw.len and std.ascii.isDigit(raw[index])) : (index += 1) {}
            if (frac_start == index) return error.InvalidDateTime;
            millis = try parseMillis(raw[frac_start..index]);
        }

        const tz_offset_seconds = try parseTimezone(raw, &index);
        if (index != raw.len) return error.InvalidDateTime;

        if (month == 0 or month > 12) return error.InvalidDateTime;
        if (day == 0 or day > daysInMonth(year, month)) return error.InvalidDateTime;
        if (hour > 23 or minute > 59 or second > 59) return error.InvalidDateTime;

        const days = daysFromCivil(year, month, day);
        if (days < 0) return error.InvalidDateTime;

        const day_seconds = std.math.add(
            i64,
            std.math.mul(i64, days, 86_400) catch return error.IntegerOverflow,
            @as(i64, hour) * 3600 + @as(i64, minute) * 60 + @as(i64, second),
        ) catch return error.IntegerOverflow;
        const utc_seconds = std.math.sub(i64, day_seconds, tz_offset_seconds) catch return error.IntegerOverflow;
        if (utc_seconds < 0) return error.InvalidDateTime;

        const total_ms = std.math.add(
            i64,
            std.math.mul(i64, utc_seconds, 1000) catch return error.IntegerOverflow,
            millis,
        ) catch return error.IntegerOverflow;
        return @intCast(total_ms);
    }

    fn parseFixed(raw: []const u8, index: *usize, digits: usize) ApplyError!u32 {
        if (index.* + digits > raw.len) return error.InvalidDateTime;
        const slice = raw[index.* .. index.* + digits];
        for (slice) |ch| {
            if (!std.ascii.isDigit(ch)) return error.InvalidDateTime;
        }
        index.* += digits;
        return std.fmt.parseUnsigned(u32, slice, 10) catch return error.InvalidDateTime;
    }

    fn parseMillis(raw: []const u8) ApplyError!u32 {
        var millis: u32 = 0;
        var digits: usize = 0;
        while (digits < raw.len and digits < 3) : (digits += 1) {
            millis = millis * 10 + (raw[digits] - '0');
        }
        while (digits < 3) : (digits += 1) {
            millis *= 10;
        }
        return millis;
    }

    fn parseTimezone(raw: []const u8, index: *usize) ApplyError!i64 {
        if (consume(raw, index, 'Z') or consume(raw, index, 'z')) return 0;
        if (index.* >= raw.len) return error.InvalidDateTime;

        const sign: i64 = switch (raw[index.*]) {
            '+' => 1,
            '-' => -1,
            else => return error.InvalidDateTime,
        };
        index.* += 1;

        const hours = try parseFixed(raw, index, 2);
        if (!consume(raw, index, ':')) return error.InvalidDateTime;
        const minutes = try parseFixed(raw, index, 2);
        if (hours > 23 or minutes > 59) return error.InvalidDateTime;
        return sign * (@as(i64, hours) * 3600 + @as(i64, minutes) * 60);
    }

    fn consume(raw: []const u8, index: *usize, ch: u8) bool {
        if (index.* >= raw.len or raw[index.*] != ch) return false;
        index.* += 1;
        return true;
    }

    fn daysInMonth(year: u32, month: u32) u32 {
        return switch (month) {
            1, 3, 5, 7, 8, 10, 12 => 31,
            4, 6, 9, 11 => 30,
            2 => if (isLeapYear(year)) 29 else 28,
            else => 0,
        };
    }

    fn isLeapYear(year: u32) bool {
        return (year % 4 == 0 and year % 100 != 0) or (year % 400 == 0);
    }

    fn daysFromCivil(year: u32, month: u32, day: u32) i64 {
        const y_input: i64 = year;
        const m_input: i64 = month;
        const d_input: i64 = day;

        const y = y_input - if (m_input <= 2) @as(i64, 1) else @as(i64, 0);
        const era = @divFloor(y, 400);
        const yoe = y - era * 400;
        const mp = m_input + if (m_input > 2) @as(i64, -3) else @as(i64, 9);
        const doy = @divFloor(153 * mp + 2, 5) + d_input - 1;
        const doe = yoe * 365 + @divFloor(yoe, 4) - @divFloor(yoe, 100) + doy;
        return era * 146_097 + doe - 719_468;
    }
};

fn dupeOwnedValue(ally: Allocator, v: bson.BSONValue) Allocator.Error!bson.BSONValue {
    return switch (v) {
        .string => |s| bson.BSONValue{ .string = .{ .value = try ally.dupe(u8, s.value) } },
        .binary => |b| bson.BSONValue{ .binary = .{ .value = try ally.dupe(u8, b.value), .subtype = b.subtype } },
        .array => |a| bson.BSONValue{ .array = bson.BSONDocument{ .buffer = try ally.dupe(u8, a.buffer) } },
        .document => |d| bson.BSONValue{ .document = bson.BSONDocument{ .buffer = try ally.dupe(u8, d.buffer) } },
        else => v,
    };
}

fn freeOwnedValue(ally: Allocator, v: bson.BSONValue) void {
    switch (v) {
        .string => |s| ally.free(s.value),
        .binary => |b| ally.free(b.value),
        .array => |a| ally.free(a.buffer),
        .document => |d| ally.free(d.buffer),
        else => {},
    }
}

test "UpdateProgram applies staged transfiguration expressions" {
    const ally = std.testing.allocator;

    var source = try bson.fmt.serialize(.{
        .name = "stark",
        .age = 40,
        .marriage = "pepper",
    }, ally);
    defer source.deinit(ally);

    var raw_program = try bson.fmt.serialize(.{
        .@"0" = .{
            .@"$set" = .{
                .age = .{ .@"$plus" = .{ "$.age", 1 } },
            },
            .state = "dead",
        },
        .@"1" = .{
            .@"$unset" = "marriage",
        },
    }, ally);
    defer raw_program.deinit(ally);

    var program = try UpdateProgram.parse(ally, raw_program);
    defer program.deinit(ally);

    const updated = try program.applyWithNowMillis(ally, source, 1_700_000_000_000);
    defer updated.deinit(ally);

    try std.testing.expectEqualStrings("stark", updated.get("name").?.string.value);
    try std.testing.expectEqual(@as(i32, 41), updated.get("age").?.int32.value);
    try std.testing.expectEqualStrings("dead", updated.get("state").?.string.value);
    try std.testing.expect(updated.get("marriage") == null);
}

test "UpdateProgram evaluates concat isoDateTime and $$now" {
    const ally = std.testing.allocator;

    var source = try bson.fmt.serialize(.{
        .first = "Tony",
        .last = "Stark",
    }, ally);
    defer source.deinit(ally);

    var raw_program = try bson.fmt.serialize(.{
        .@"$set" = .{
            .full = .{ .@"$concat" = .{ "$.first", " ", "$.last" } },
            .seenAt = "$$now",
            .diedAt = .{ .@"$isoDateTime" = "2024-04-22T10:20:30.456Z" },
            .yearsLeft = .{ .@"$minus" = .{ 10, 3 } },
        },
    }, ally);
    defer raw_program.deinit(ally);

    var program = try UpdateProgram.parse(ally, raw_program);
    defer program.deinit(ally);

    const updated = try program.applyWithNowMillis(ally, source, 1_700_000_123_456);
    defer updated.deinit(ally);

    try std.testing.expectEqualStrings("Tony Stark", updated.get("full").?.string.value);
    try std.testing.expectEqual(@as(u64, 1_700_000_123_456), updated.get("seenAt").?.datetime.value);
    try std.testing.expectEqual(@as(u64, 1_713_781_230_456), updated.get("diedAt").?.datetime.value);
    try std.testing.expectEqual(@as(i32, 7), updated.get("yearsLeft").?.int32.value);
}
