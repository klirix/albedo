/// queries will be simple
/// There are filters, sorts, sectoring and projections
/// filters: $eq, $ne, $lt, $lte, $gt, $gte, $in, $between, $startsWith, $endsWith, $exists, $notExists, $vectorSearch
/// sorts: $asc, $desc
/// sectoring: $offset, $limit
///
/// All of the fields are optional and can be omitted
/// The query will be a BSON document with the following structure:
/// {
///  "query": {"field.path": {"$eq": "value"}}, // Flat field.path -> filter
///  "sort": {"asc": "field"} | {"desc": "field"}, // Sort by field, only one field allowed
///  "sector": {"offset": 0, "limit": 10},  // Offset and limit for pagination
///  "projection": {"omit": ["path"] } | {"pick": ["path"]} // Projection of fields wither
/// }
const std = @import("std");
const bson = @import("bson.zig");
const BSONDoc = bson.BSONDocument;
const Allocator = std.mem.Allocator;
const algos = @import("./vector_search.zig");

const Path = [][]const u8; // "field.subfield".split('.')

pub const FilterType = enum { eq, ne, lt, lte, gt, gte, in, between, startsWith, endsWith, exists, notExists, vectorSearch };

pub const PathValuePair = struct {
    path: []const u8,
    value: bson.BSONValue,
};

pub fn parsePath(ally: Allocator, pathSrc: []const u8) ![][]const u8 {
    var pathIter = std.mem.splitScalar(u8, pathSrc, '.');
    const pathLen = std.mem.count(u8, pathSrc, ".") + 1;
    var path: Path = try ally.alloc([]const u8, pathLen);
    var crumbleIdx: usize = 0;
    while (pathIter.next()) |crumble| : (crumbleIdx += 1) {
        path[crumbleIdx] = crumble;
    }
    return path;
}

test "test parse path" {
    const ally = std.testing.allocator;
    const path = try parsePath(ally, "field.subfield");
    defer ally.free(path);
    try std.testing.expectEqualSlices(u8, "field", path[0]);
    try std.testing.expectEqualSlices(u8, "subfield", path[1]);
}

const VectorSearchType = enum {
    cosine,
    dot,
    euclidean,
};

const vectorSearchTypeMap = std.StaticStringMap(VectorSearchType).initComptime(.{
    .{ "cosine", .cosine },
    .{ "dot", .dot },
    .{ "euclidean", .euclidean },
});

const filterNameMap = std.StaticStringMap(FilterType).initComptime(.{
    .{ "$eq", .eq },
    .{ "$ne", .ne },
    .{ "$lt", .lt },
    .{ "$lte", .lte },
    .{ "$gt", .gt },
    .{ "$gte", .gte },
    .{ "$in", .in },
    .{ "$between", .between },
    .{ "$startsWith", .startsWith },
    .{ "$endsWith", .endsWith },
    .{ "$exists", .exists },
    .{ "$notExists", .notExists },
    .{ "$vectorSearch", .vectorSearch },
});

const VectorSearchOperand = union(enum) {
    gte: f32,
    lte: f32,
    gt: f32,
    lt: f32,
};

const vectorSearchOperandMap = std.StaticStringMap(std.meta.Tag(VectorSearchOperand)).initComptime(.{
    .{ "gte", .gte },
    .{ "lte", .lte },
    .{ "gt", .gt },
    .{ "lt", .lt },
    .{ "$gte", .gte },
    .{ "$lte", .lte },
    .{ "$gt", .gt },
    .{ "$lt", .lt },
});

pub const Filter = union(FilterType) {
    eq: PathValuePair,
    ne: PathValuePair,
    lt: PathValuePair,
    lte: PathValuePair,
    gt: PathValuePair,
    gte: PathValuePair,
    in: PathValuePair,
    between: PathValuePair,
    startsWith: PathValuePair,
    endsWith: PathValuePair,
    exists: PathValuePair,
    notExists: PathValuePair,
    vectorSearch: struct {
        path: []const u8,
        array: []const f32,
        operand: VectorSearchOperand,
        type: VectorSearchType,
    }, // value is expected to be an array of floats representing the vector

    pub fn deinit(self: *Filter, ally: Allocator) void {
        switch (self.*) {
            .in => |*inFilter| {
                ally.free(inFilter.path);
            },
            .between => |*betweenFilter| {
                ally.free(betweenFilter.path);
            },
            .eq, .ne, .lt, .gt, .lte, .gte, .startsWith, .endsWith, .exists, .notExists => |*filter| {
                ally.free(filter.path);
            },
            .vectorSearch => |*filter| {
                ally.free(filter.path);
                ally.free(filter.array);
            },
        }
    }

    pub const FilterParsingErrors = error{
        InvalidQueryFilter,
        InvalidQueryPath,
        InvalidQueryOperator,
        InvalidQueryOperatorSize,
        InvalidInOperatorValue,
        InvalidQueryRoot,
        InvalidQueryOperatorParameter,
        InvalidQueryOperatorBetweenSize,
        OutOfMemory,
    };

    pub const FilterMatchErrors = error{
        InvalidVectorType,
        InvalidVectorByteLength,
        InvalidVectorDimension,
        InvalidVectorAlignment,
        InvalidVectorComputation,
    };

    fn parseDoc(ally: Allocator, doc: bson.BSONDocument) FilterParsingErrors![]Filter {
        var filters = std.ArrayListUnmanaged(Filter){};
        defer filters.deinit(ally);

        var iter = doc.iter();
        while (iter.next()) |pair| {
            const path = pair.key;
            if (path.len == 0) return FilterParsingErrors.InvalidQueryPath;

            switch (pair.value) {
                .document => |*operatorDoc| {
                    var op_iter = operatorDoc.iter();
                    var matched = false;
                    while (op_iter.next()) |op_pair| {
                        const operand: bson.BSONValue = op_pair.value;
                        const operator = filterNameMap.get(op_pair.key) orelse return FilterParsingErrors.InvalidQueryOperator;
                        switch (operator) {
                            inline .eq, .in, .ne, .lt, .lte, .gt, .gte, .startsWith, .endsWith, .exists, .notExists => |op| {
                                const path_copy = try ally.dupe(u8, path);
                                errdefer ally.free(path_copy);
                                const filter = @unionInit(Filter, @tagName(op), PathValuePair{ .path = path_copy, .value = operand });

                                try filters.append(ally, filter);
                            },
                            .vectorSearch => {
                                if (operand != .document) return FilterParsingErrors.InvalidQueryOperatorParameter;
                                const path_copy = try ally.dupe(u8, path);
                                errdefer ally.free(path_copy);
                                const algo_value = operand.document.get("algo") orelse return FilterParsingErrors.InvalidQueryOperatorParameter;
                                const algo_str = switch (algo_value) {
                                    .string => |s| s.value,
                                    else => return FilterParsingErrors.InvalidQueryOperatorParameter,
                                };
                                const search_algo = vectorSearchTypeMap.get(algo_str) orelse return FilterParsingErrors.InvalidQueryOperatorParameter;
                                const operand_value = operand.document.get("operand") orelse return FilterParsingErrors.InvalidQueryOperatorParameter;
                                const operand_f32 = switch (operand_value) {
                                    .document => |operand_doc| blk: {
                                        var operand_iter = operand_doc.iter();
                                        const operand_pair = operand_iter.next() orelse return FilterParsingErrors.InvalidQueryOperatorParameter;
                                        if (operand_iter.next()) |_| return FilterParsingErrors.InvalidQueryOperatorParameter;

                                        const operand_tag = vectorSearchOperandMap.get(operand_pair.key) orelse return FilterParsingErrors.InvalidQueryOperatorParameter;
                                        const operand_threshold: f32 = switch (operand_pair.value) {
                                            .double => |d| @floatCast(d.value),
                                            .int32 => |v| @floatFromInt(v.value),
                                            .int64 => |v| @floatFromInt(v.value),
                                            else => return FilterParsingErrors.InvalidQueryOperatorParameter,
                                        };

                                        break :blk switch (operand_tag) {
                                            .gte => VectorSearchOperand{ .gte = operand_threshold },
                                            .lte => VectorSearchOperand{ .lte = operand_threshold },
                                            .gt => VectorSearchOperand{ .gt = operand_threshold },
                                            .lt => VectorSearchOperand{ .lt = operand_threshold },
                                        };
                                    },
                                    else => return FilterParsingErrors.InvalidQueryOperatorParameter,
                                };

                                const vector_value = operand.document.get("vector") orelse return FilterParsingErrors.InvalidQueryOperatorParameter;
                                const vector_bin: []const f32 = switch (vector_value) {
                                    .binary => |b| blk: {
                                        if (b.value.len % @sizeOf(f32) != 0) return FilterParsingErrors.InvalidQueryOperatorParameter;
                                        const vector_len = b.value.len / @sizeOf(f32);
                                        const vector_copy = try ally.alloc(f32, vector_len);
                                        for (0..vector_len) |idx| {
                                            const start = idx * @sizeOf(f32);
                                            vector_copy[idx] = std.mem.bytesToValue(f32, b.value[start .. start + @sizeOf(f32)]);
                                        }
                                        break :blk vector_copy;
                                    },
                                    else => return FilterParsingErrors.InvalidQueryOperatorParameter,
                                };
                                errdefer ally.free(vector_bin);

                                try filters.append(ally, .{ .vectorSearch = .{
                                    .path = path_copy,
                                    .array = vector_bin,
                                    .type = search_algo,
                                    .operand = operand_f32,
                                } });
                            },
                            .between => {
                                if (operand != .array) return FilterParsingErrors.InvalidQueryOperatorParameter;
                                if (operand.array.keyNumber() != 2) return FilterParsingErrors.InvalidQueryOperatorBetweenSize;
                                const path_copy = try ally.dupe(u8, path);
                                errdefer ally.free(path_copy);
                                try filters.append(ally, .{ .between = .{ .path = path_copy, .value = operand } });
                            },
                        }
                        matched = true;
                    }

                    if (!matched) return FilterParsingErrors.InvalidQueryOperator;
                },
                .string, .int32, .int64, .objectId, .datetime, .boolean, .null, .double => {
                    const path_copy = try ally.dupe(u8, path);
                    errdefer ally.free(path_copy);
                    try filters.append(ally, .{ .eq = .{ .path = path_copy, .value = pair.value } });
                },

                else => return FilterParsingErrors.InvalidQueryFilter,
            }
        }

        return filters.toOwnedSlice(ally);
    }

    pub fn parse(ally: Allocator, doc: bson.BSONValue) FilterParsingErrors![]Filter {
        if (doc != bson.BSONValueType.document) return FilterParsingErrors.InvalidQueryRoot;
        return try parseDoc(ally, doc.document);
    }

    pub fn matchValue(self: *const Filter, valueToMatch: bson.BSONValue) FilterMatchErrors!bool {
        switch (self.*) {
            .eq => |eqFilter| {
                return valueToMatch.eql(&eqFilter.value);
            },
            .ne => |neFilter| {
                return !valueToMatch.eql(&neFilter.value);
            },
            .lt => |ltFilter| {
                return valueToMatch.order(ltFilter.value) == .lt;
            },
            .gt => |gtFilter| {
                return valueToMatch.order(gtFilter.value) == .gt;
            },
            .lte => |lteFilter| {
                return valueToMatch.order(lteFilter.value) != .gt;
            },
            .gte => |lteFilter| {
                return valueToMatch.order(lteFilter.value) != .lt;
            },
            .in => |inFilter| {
                var inIter = inFilter.value.array.iter();
                while (inIter.next()) |pair| {
                    if (valueToMatch.eql(&pair.value)) return true;
                }
                return false;
            },
            .between => |betweenFilter| {
                const lowerBound = betweenFilter.value.array.get("0") orelse unreachable;
                const upperBound = betweenFilter.value.array.get("1") orelse unreachable;

                return valueToMatch.order(lowerBound) == .gt and valueToMatch.order(upperBound) == .lt;
            },
            .startsWith => |startsWithFilter| {
                if (valueToMatch != bson.BSONValueType.string) return false;
                const strValue = valueToMatch.string.value;
                const prefix = startsWithFilter.value.string.value;
                if (strValue.len < prefix.len) return false;
                return std.mem.eql(u8, strValue[0..prefix.len], prefix);
            },
            .endsWith => |endsWithFilter| {
                if (valueToMatch != bson.BSONValueType.string) return false;
                const strValue = valueToMatch.string.value;
                const suffix = endsWithFilter.value.string.value;
                if (strValue.len < suffix.len) return false;
                return std.mem.eql(u8, strValue[strValue.len - suffix.len ..], suffix);
            },
            .vectorSearch => |vectorFilter| {
                const vecValueBytes: []const u8 = switch (valueToMatch) {
                    .binary => |b| b.value,
                    else => return error.InvalidVectorType,
                };
                if (vecValueBytes.len % @sizeOf(f32) != 0) return error.InvalidVectorByteLength;
                const vec_len = vecValueBytes.len / @sizeOf(f32);
                if (vec_len != vectorFilter.array.len) return error.InvalidVectorDimension;

                if (@intFromPtr(vecValueBytes.ptr) % @alignOf(f32) != 0) {
                    return error.InvalidVectorAlignment;
                }
                const aligned_bytes: []align(@alignOf(f32)) const u8 = @alignCast(vecValueBytes);
                const vecValue = std.mem.bytesAsSlice(f32, aligned_bytes);
                const res = (switch (vectorFilter.type) {
                    .dot => algos.dot_product(vecValue, vectorFilter.array),
                    .cosine => algos.cosine_similarity(vecValue, vectorFilter.array),
                    .euclidean => algos.euclidean_distance(vecValue, vectorFilter.array),
                }) catch return error.InvalidVectorComputation;

                return switch (vectorFilter.operand) {
                    .gte => |threshold| res >= threshold,
                    .lte => |threshold| res <= threshold,
                    .gt => |threshold| res > threshold,
                    .lt => |threshold| res < threshold,
                };
            },
            .exists => |_| {
                return true;
            },
            .notExists => |_| {
                return false;
            },
        }
    }

    pub fn match(self: *const Filter, doc: *const bson.BSONDocument) FilterMatchErrors!bool {
        const valueToMatch = switch (self.*) {
            .notExists => |e| blk: {
                // For notExists, return true if field doesn't exist, false if it exists
                break :blk doc.getPath(e.path) orelse return true;
            },
            inline .eq, .ne, .lt, .lte, .gte, .gt, .in, .between, .startsWith, .endsWith, .exists, .vectorSearch => |e| blk: {
                break :blk doc.getPath(e.path) orelse return false;
            },
        };
        return try matchValue(self, valueToMatch);
    }
};

test "Filter.parse" {
    const ally = std.testing.allocator;
    const filterDoc = try bson.fmt.serialize(.{ .@"field.subfield" = .{ .@"$eq" = "value" } }, ally);
    defer filterDoc.deinit(ally);
    const filters = try Filter.parse(ally, bson.BSONValue{ .document = filterDoc });
    defer {
        for (filters) |*f| f.deinit(ally);
        ally.free(filters);
    }
    try std.testing.expectEqual(filters.len, 1);
    const filter = filters[0];
    switch (filter) {
        .eq => |*eqFilter| {
            try std.testing.expectEqualSlices(u8, "field.subfield", eqFilter.path);
            try std.testing.expectEqualStrings(eqFilter.value.string.value, "value");
        },
        else => unreachable,
    }
}

test "Filter.parse handles gte" {
    const ally = std.testing.allocator;
    const filterDoc = try bson.fmt.serialize(.{ .score = .{ .@"$gte" = 10 } }, ally);
    defer filterDoc.deinit(ally);
    const filters = try Filter.parse(ally, bson.BSONValue{ .document = filterDoc });
    defer {
        for (filters) |*f| f.deinit(ally);
        ally.free(filters);
    }
    try std.testing.expectEqual(filters.len, 1);
    switch (filters[0]) {
        .gte => |*gteFilter| {
            try std.testing.expectEqualSlices(u8, "score", gteFilter.path);
            try std.testing.expectEqual(@as(i32, 10), gteFilter.value.int32.value);
        },
        else => unreachable,
    }
}

test "Filter.parse handles lte" {
    const ally = std.testing.allocator;
    const filterDoc = try bson.fmt.serialize(.{ .score = .{ .@"$lte" = 20 } }, ally);
    defer filterDoc.deinit(ally);
    const filters = try Filter.parse(ally, bson.BSONValue{ .document = filterDoc });
    defer {
        for (filters) |*f| f.deinit(ally);
        ally.free(filters);
    }
    try std.testing.expectEqual(filters.len, 1);
    switch (filters[0]) {
        .lte => |*lteFilter| {
            try std.testing.expectEqualSlices(u8, "score", lteFilter.path);
            try std.testing.expectEqual(@as(i32, 20), lteFilter.value.int32.value);
        },
        else => unreachable,
    }
}

test "Filter.parse handles startsWith" {
    const ally = std.testing.allocator;
    const filterDoc = try bson.fmt.serialize(.{ .name = .{ .@"$startsWith" = "prefix" } }, ally);
    defer filterDoc.deinit(ally);
    const filters = try Filter.parse(ally, bson.BSONValue{ .document = filterDoc });
    defer {
        for (filters) |*f| f.deinit(ally);
        ally.free(filters);
    }
    try std.testing.expectEqual(filters.len, 1);
    switch (filters[0]) {
        .startsWith => |*startsWithFilter| {
            try std.testing.expectEqualSlices(u8, "name", startsWithFilter.path);
            try std.testing.expectEqualStrings("prefix", startsWithFilter.value.string.value);
        },
        else => unreachable,
    }
}

test "Filter.matchValue startsWith matches correctly" {
    const ally = std.testing.allocator;
    const filterDoc = try bson.fmt.serialize(.{ .message = .{ .@"$startsWith" = "hello" } }, ally);
    defer filterDoc.deinit(ally);
    const filters = try Filter.parse(ally, bson.BSONValue{ .document = filterDoc });
    defer {
        for (filters) |*f| f.deinit(ally);
        ally.free(filters);
    }
    const filter = &filters[0];

    // Test positive cases
    const matchingValue1 = bson.BSONValue{ .string = bson.BSONString{ .value = "hello world" } };
    try std.testing.expect(try filter.matchValue(matchingValue1));

    const matchingValue2 = bson.BSONValue{ .string = bson.BSONString{ .value = "hello" } };
    try std.testing.expect(try filter.matchValue(matchingValue2));

    // Test negative cases
    const nonMatchingValue1 = bson.BSONValue{ .string = bson.BSONString{ .value = "world hello" } };
    try std.testing.expect(!(try filter.matchValue(nonMatchingValue1)));

    const nonMatchingValue2 = bson.BSONValue{ .string = bson.BSONString{ .value = "hel" } };
    try std.testing.expect(!(try filter.matchValue(nonMatchingValue2)));

    const emptyString = bson.BSONValue{ .string = bson.BSONString{ .value = "" } };
    try std.testing.expect(!(try filter.matchValue(emptyString)));

    // Test non-string value
    const nonStringValue = bson.BSONValue{ .int32 = .{ .value = 42 } };
    try std.testing.expect(!(try filter.matchValue(nonStringValue)));
}

test "Filter.matchValue startsWith with empty prefix" {
    const ally = std.testing.allocator;
    const filterDoc = try bson.fmt.serialize(.{ .message = .{ .@"$startsWith" = "" } }, ally);
    defer filterDoc.deinit(ally);
    const filters = try Filter.parse(ally, bson.BSONValue{ .document = filterDoc });
    defer {
        for (filters) |*f| f.deinit(ally);
        ally.free(filters);
    }
    const filter = &filters[0];

    // Empty prefix should match any string
    const anyString = bson.BSONValue{ .string = bson.BSONString{ .value = "anything" } };
    try std.testing.expect(try filter.matchValue(anyString));

    const emptyString = bson.BSONValue{ .string = bson.BSONString{ .value = "" } };
    try std.testing.expect(try filter.matchValue(emptyString));
}

test "Filter.parse handles endsWith" {
    const ally = std.testing.allocator;
    const filterDoc = try bson.fmt.serialize(.{ .filename = .{ .@"$endsWith" = "suffix" } }, ally);
    defer filterDoc.deinit(ally);
    const filters = try Filter.parse(ally, bson.BSONValue{ .document = filterDoc });
    defer {
        for (filters) |*f| f.deinit(ally);
        ally.free(filters);
    }
    try std.testing.expectEqual(filters.len, 1);
    switch (filters[0]) {
        .endsWith => |*endsWithFilter| {
            try std.testing.expectEqualSlices(u8, "filename", endsWithFilter.path);
            try std.testing.expectEqualStrings("suffix", endsWithFilter.value.string.value);
        },
        else => unreachable,
    }
}

test "Filter.matchValue endsWith matches correctly" {
    const ally = std.testing.allocator;
    const filterDoc = try bson.fmt.serialize(.{ .filename = .{ .@"$endsWith" = ".txt" } }, ally);
    defer filterDoc.deinit(ally);
    const filters = try Filter.parse(ally, bson.BSONValue{ .document = filterDoc });
    defer {
        for (filters) |*f| f.deinit(ally);
        ally.free(filters);
    }
    const filter = &filters[0];

    // Test positive cases
    const matchingValue1 = bson.BSONValue{ .string = bson.BSONString{ .value = "document.txt" } };
    try std.testing.expect(try filter.matchValue(matchingValue1));

    const matchingValue2 = bson.BSONValue{ .string = bson.BSONString{ .value = ".txt" } };
    try std.testing.expect(try filter.matchValue(matchingValue2));

    // Test negative cases
    const nonMatchingValue1 = bson.BSONValue{ .string = bson.BSONString{ .value = "document.pdf" } };
    try std.testing.expect(!(try filter.matchValue(nonMatchingValue1)));

    const nonMatchingValue2 = bson.BSONValue{ .string = bson.BSONString{ .value = "txt" } };
    try std.testing.expect(!(try filter.matchValue(nonMatchingValue2)));

    const emptyString = bson.BSONValue{ .string = bson.BSONString{ .value = "" } };
    try std.testing.expect(!(try filter.matchValue(emptyString)));

    // Test non-string value
    const nonStringValue = bson.BSONValue{ .int32 = .{ .value = 42 } };
    try std.testing.expect(!(try filter.matchValue(nonStringValue)));
}

test "Filter.matchValue endsWith with empty suffix" {
    const ally = std.testing.allocator;
    const filterDoc = try bson.fmt.serialize(.{ .filename = .{ .@"$endsWith" = "" } }, ally);
    defer filterDoc.deinit(ally);
    const filters = try Filter.parse(ally, bson.BSONValue{ .document = filterDoc });
    defer {
        for (filters) |*f| f.deinit(ally);
        ally.free(filters);
    }
    const filter = &filters[0];

    // Empty suffix should match any string
    const anyString = bson.BSONValue{ .string = bson.BSONString{ .value = "anything" } };
    try std.testing.expect(try filter.matchValue(anyString));

    const emptyString = bson.BSONValue{ .string = bson.BSONString{ .value = "" } };
    try std.testing.expect(try filter.matchValue(emptyString));
}

test "Filter.parse handles exists" {
    const ally = std.testing.allocator;
    const filterDoc = try bson.fmt.serialize(.{ .email = .{ .@"$exists" = true } }, ally);
    defer filterDoc.deinit(ally);
    const filters = try Filter.parse(ally, bson.BSONValue{ .document = filterDoc });
    defer {
        for (filters) |*f| f.deinit(ally);
        ally.free(filters);
    }
    try std.testing.expectEqual(filters.len, 1);
    switch (filters[0]) {
        .exists => |*existsFilter| {
            try std.testing.expectEqualSlices(u8, "email", existsFilter.path);
        },
        else => unreachable,
    }
}

test "Filter.matchValue exists always returns true" {
    const ally = std.testing.allocator;
    const filterDoc = try bson.fmt.serialize(.{ .field = .{ .@"$exists" = true } }, ally);
    defer filterDoc.deinit(ally);
    const filters = try Filter.parse(ally, bson.BSONValue{ .document = filterDoc });
    defer {
        for (filters) |*f| f.deinit(ally);
        ally.free(filters);
    }
    const filter = &filters[0];

    // Exists filter should always return true for any value when field exists
    const stringValue = bson.BSONValue{ .string = bson.BSONString{ .value = "test" } };
    try std.testing.expect(try filter.matchValue(stringValue));

    const intValue = bson.BSONValue{ .int32 = .{ .value = 42 } };
    try std.testing.expect(try filter.matchValue(intValue));

    const boolValue = bson.BSONValue{ .boolean = .{ .value = false } };
    try std.testing.expect(try filter.matchValue(boolValue));
}

test "Filter.match exists checks field presence in document" {
    const ally = std.testing.allocator;
    const filterDoc = try bson.fmt.serialize(.{ .email = .{ .@"$exists" = true } }, ally);
    defer filterDoc.deinit(ally);
    const filters = try Filter.parse(ally, bson.BSONValue{ .document = filterDoc });
    defer {
        for (filters) |*f| f.deinit(ally);
        ally.free(filters);
    }
    const filter = &filters[0];

    // Document with the field should match
    var docWithField = bson.BSONDocument.initEmpty();
    docWithField = try docWithField.set(ally, "email", bson.BSONValue{ .string = bson.BSONString{ .value = "test@example.com" } });
    defer docWithField.deinit(ally);
    try std.testing.expect(try filter.match(&docWithField));

    // Document without the field should not match
    var docWithoutField = bson.BSONDocument.initEmpty();
    docWithoutField = try docWithoutField.set(ally, "name", bson.BSONValue{ .string = bson.BSONString{ .value = "John" } });
    defer docWithoutField.deinit(ally);
    try std.testing.expect(!(try filter.match(&docWithoutField)));
}

test "Filter.parse handles notExists" {
    const ally = std.testing.allocator;
    const filterDoc = try bson.fmt.serialize(.{ .deletedAt = .{ .@"$notExists" = true } }, ally);
    defer filterDoc.deinit(ally);
    const filters = try Filter.parse(ally, bson.BSONValue{ .document = filterDoc });
    defer {
        for (filters) |*f| f.deinit(ally);
        ally.free(filters);
    }
    try std.testing.expectEqual(filters.len, 1);
    switch (filters[0]) {
        .notExists => |*notExistsFilter| {
            try std.testing.expectEqualSlices(u8, "deletedAt", notExistsFilter.path);
        },
        else => unreachable,
    }
}

test "Filter.matchValue notExists always returns false" {
    const ally = std.testing.allocator;
    const filterDoc = try bson.fmt.serialize(.{ .field = .{ .@"$notExists" = true } }, ally);
    defer filterDoc.deinit(ally);
    const filters = try Filter.parse(ally, bson.BSONValue{ .document = filterDoc });
    defer {
        for (filters) |*f| f.deinit(ally);
        ally.free(filters);
    }
    const filter = &filters[0];

    // notExists filter should always return false when a value exists
    const stringValue = bson.BSONValue{ .string = bson.BSONString{ .value = "test" } };
    try std.testing.expect(!(try filter.matchValue(stringValue)));

    const intValue = bson.BSONValue{ .int32 = .{ .value = 42 } };
    try std.testing.expect(!(try filter.matchValue(intValue)));

    const boolValue = bson.BSONValue{ .boolean = .{ .value = false } };
    try std.testing.expect(!(try filter.matchValue(boolValue)));
}

test "Filter.match notExists checks field absence in document" {
    const ally = std.testing.allocator;
    const filterDoc = try bson.fmt.serialize(.{ .deletedAt = .{ .@"$notExists" = true } }, ally);
    defer filterDoc.deinit(ally);
    const filters = try Filter.parse(ally, bson.BSONValue{ .document = filterDoc });
    defer {
        for (filters) |*f| f.deinit(ally);
        ally.free(filters);
    }
    const filter = &filters[0];

    // Document with the field should not match
    var docWithField = bson.BSONDocument.initEmpty();
    docWithField = try docWithField.set(ally, "deletedAt", bson.BSONValue{ .string = bson.BSONString{ .value = "2024-01-01" } });
    defer docWithField.deinit(ally);
    try std.testing.expect(!(try filter.match(&docWithField)));

    // Document without the field should match (returns true when field doesn't exist)
    var docWithoutField = bson.BSONDocument.initEmpty();
    docWithoutField = try docWithoutField.set(ally, "createdAt", bson.BSONValue{ .string = bson.BSONString{ .value = "2024-01-01" } });
    defer docWithoutField.deinit(ally);
    try std.testing.expect(try filter.match(&docWithoutField));
}

fn buildVectorSearchFilterDoc(
    ally: Allocator,
    path: []const u8,
    algo: []const u8,
    vector_value: bson.BSONValue,
    operand_pairs: []bson.BSONKeyValuePair,
) !bson.BSONDocument {
    const operand_doc = try bson.BSONDocument.fromPairs(ally, operand_pairs);
    defer operand_doc.deinit(ally);

    var vector_search_pairs = [_]bson.BSONKeyValuePair{
        .{ .key = "algo", .value = .{ .string = .{ .value = algo } } },
        .{ .key = "vector", .value = vector_value },
        .{ .key = "operand", .value = .{ .document = operand_doc } },
    };
    const vector_search_doc = try bson.BSONDocument.fromPairs(ally, vector_search_pairs[0..]);
    defer vector_search_doc.deinit(ally);

    var operator_pairs = [_]bson.BSONKeyValuePair{
        .{ .key = "$vectorSearch", .value = .{ .document = vector_search_doc } },
    };
    const operator_doc = try bson.BSONDocument.fromPairs(ally, operator_pairs[0..]);
    defer operator_doc.deinit(ally);

    var filter_pairs = [_]bson.BSONKeyValuePair{
        .{ .key = path, .value = .{ .document = operator_doc } },
    };
    return bson.BSONDocument.fromPairs(ally, filter_pairs[0..]);
}

test "Filter.parse handles vectorSearch" {
    const ally = std.testing.allocator;
    const query_vector = [_]f32{ 1.0, 2.0, 3.0 };
    var operand_pairs = [_]bson.BSONKeyValuePair{
        .{ .key = "$gte", .value = .{ .double = .{ .value = 0.99 } } },
    };
    const filterDoc = try buildVectorSearchFilterDoc(ally, "embedding", "cosine", .{
        .binary = .{
            .value = std.mem.sliceAsBytes(query_vector[0..]),
            .subtype = 0x00,
        },
    }, operand_pairs[0..]);
    defer filterDoc.deinit(ally);

    const filters = try Filter.parse(ally, bson.BSONValue{ .document = filterDoc });
    defer {
        for (filters) |*f| f.deinit(ally);
        ally.free(filters);
    }

    try std.testing.expectEqual(@as(usize, 1), filters.len);
    switch (filters[0]) {
        .vectorSearch => |vector_filter| {
            try std.testing.expectEqualStrings("embedding", vector_filter.path);
            try std.testing.expectEqual(VectorSearchType.cosine, vector_filter.type);
            try std.testing.expectEqualSlices(f32, query_vector[0..], vector_filter.array);
            switch (vector_filter.operand) {
                .gte => |threshold| try std.testing.expectApproxEqAbs(@as(f32, 0.99), threshold, 0.0001),
                else => unreachable,
            }
        },
        else => unreachable,
    }
}

test "Filter.matchValue vectorSearch matches with cosine threshold" {
    const ally = std.testing.allocator;
    const query_vector = [_]f32{ 1.0, 2.0, 3.0 };
    var operand_pairs = [_]bson.BSONKeyValuePair{
        .{ .key = "$gte", .value = .{ .double = .{ .value = 0.99 } } },
    };
    const filterDoc = try buildVectorSearchFilterDoc(ally, "embedding", "cosine", .{
        .binary = .{
            .value = std.mem.sliceAsBytes(query_vector[0..]),
            .subtype = 0x00,
        },
    }, operand_pairs[0..]);
    defer filterDoc.deinit(ally);

    const filters = try Filter.parse(ally, bson.BSONValue{ .document = filterDoc });
    defer {
        for (filters) |*f| f.deinit(ally);
        ally.free(filters);
    }

    const filter = &filters[0];
    const same_vector = [_]f32{ 1.0, 2.0, 3.0 };
    const same_value = bson.BSONValue{
        .binary = .{
            .value = std.mem.sliceAsBytes(same_vector[0..]),
            .subtype = 0x00,
        },
    };
    try std.testing.expect(try filter.matchValue(same_value));

    const short_vector = [_]f32{ 1.0, 2.0 };
    const short_value = bson.BSONValue{
        .binary = .{
            .value = std.mem.sliceAsBytes(short_vector[0..]),
            .subtype = 0x00,
        },
    };
    try std.testing.expectError(error.InvalidVectorDimension, filter.matchValue(short_value));
}

test "Filter.matchValue vectorSearch matches with euclidean lt threshold" {
    const ally = std.testing.allocator;
    const query_vector = [_]f32{ 0.5, 0.25, 0.125 };
    var operand_pairs = [_]bson.BSONKeyValuePair{
        .{ .key = "lt", .value = .{ .double = .{ .value = 0.00001 } } },
    };
    const filterDoc = try buildVectorSearchFilterDoc(ally, "embedding", "euclidean", .{
        .binary = .{
            .value = std.mem.sliceAsBytes(query_vector[0..]),
            .subtype = 0x00,
        },
    }, operand_pairs[0..]);
    defer filterDoc.deinit(ally);

    const filters = try Filter.parse(ally, bson.BSONValue{ .document = filterDoc });
    defer {
        for (filters) |*f| f.deinit(ally);
        ally.free(filters);
    }

    const filter = &filters[0];
    const same_value = bson.BSONValue{
        .binary = .{
            .value = std.mem.sliceAsBytes(query_vector[0..]),
            .subtype = 0x00,
        },
    };
    try std.testing.expect(try filter.matchValue(same_value));
}

test "Filter.parse vectorSearch rejects invalid operand operator" {
    const ally = std.testing.allocator;
    const query_vector = [_]f32{ 1.0, 2.0, 3.0 };
    var operand_pairs = [_]bson.BSONKeyValuePair{
        .{ .key = "invalid", .value = .{ .double = .{ .value = 0.5 } } },
    };
    const filterDoc = try buildVectorSearchFilterDoc(ally, "embedding", "dot", .{
        .binary = .{
            .value = std.mem.sliceAsBytes(query_vector[0..]),
            .subtype = 0x00,
        },
    }, operand_pairs[0..]);
    defer filterDoc.deinit(ally);

    try std.testing.expectError(
        error.InvalidQueryOperatorParameter,
        Filter.parse(ally, bson.BSONValue{ .document = filterDoc }),
    );
}

test "Filter.parse vectorSearch rejects multiple operand operators" {
    const ally = std.testing.allocator;
    const query_vector = [_]f32{ 1.0, 2.0, 3.0 };
    var operand_pairs = [_]bson.BSONKeyValuePair{
        .{ .key = "gt", .value = .{ .double = .{ .value = 0.3 } } },
        .{ .key = "lt", .value = .{ .double = .{ .value = 0.9 } } },
    };
    const filterDoc = try buildVectorSearchFilterDoc(ally, "embedding", "dot", .{
        .binary = .{
            .value = std.mem.sliceAsBytes(query_vector[0..]),
            .subtype = 0x00,
        },
    }, operand_pairs[0..]);
    defer filterDoc.deinit(ally);

    try std.testing.expectError(
        error.InvalidQueryOperatorParameter,
        Filter.parse(ally, bson.BSONValue{ .document = filterDoc }),
    );
}

test "Filter.parse vectorSearch rejects non-binary vector" {
    const ally = std.testing.allocator;
    var operand_pairs = [_]bson.BSONKeyValuePair{
        .{ .key = "gte", .value = .{ .double = .{ .value = 0.1 } } },
    };
    const filterDoc = try buildVectorSearchFilterDoc(ally, "embedding", "cosine", .{
        .string = .{ .value = "not-binary" },
    }, operand_pairs[0..]);
    defer filterDoc.deinit(ally);

    try std.testing.expectError(
        error.InvalidQueryOperatorParameter,
        Filter.parse(ally, bson.BSONValue{ .document = filterDoc }),
    );
}

const SortType = enum {
    asc,
    desc,
};

const SortConfig = union(SortType) {
    asc: []const u8,
    desc: []const u8,

    pub const SortParsingErrors = error{
        InvalidQuerySort,
        InvalidQuerySortParamLength,
        InvalidQuerySortParamType,
        InvalidQueryPath,
    };

    pub fn parse(ally: Allocator, doc: bson.BSONValue) (SortParsingErrors || Allocator.Error)!SortConfig {
        if (doc != bson.BSONValueType.document) return error.InvalidQuerySort;

        if (doc.document.keyNumber() != 1) return error.InvalidQuerySortParamLength;

        var iter = doc.document.iter();
        const pair = iter.next() orelse return error.InvalidQuerySort;

        if (pair.value != bson.BSONValueType.string) return error.InvalidQuerySortParamType;

        const path = pair.value.string.value;
        if (path.len == 0) return error.InvalidQueryPath;
        const path_copy = try ally.dupe(u8, path);
        errdefer ally.free(path_copy);
        if (std.mem.eql(u8, pair.key, "asc")) {
            return SortConfig{ .asc = path_copy };
        } else if (std.mem.eql(u8, pair.key, "desc")) {
            return SortConfig{ .desc = path_copy };
        } else {
            return error.InvalidQuerySort;
        }
    }
};

test "Sort.parse" {
    const ally = std.testing.allocator;
    const sortDoc = try bson.fmt.serialize(.{ .asc = "field" }, ally);
    defer sortDoc.deinit(ally);
    const sort = try SortConfig.parse(ally, bson.BSONValue{ .document = sortDoc });
    defer switch (sort) {
        .asc => |path| ally.free(path),
        .desc => |path| ally.free(path),
    };

    switch (sort) {
        .asc => |ascSort| {
            try std.testing.expectEqualSlices(u8, "field", ascSort);
        },
        else => unreachable,
    }
}

const Sector = struct {
    offset: ?u64,
    limit: ?u64,

    pub const SectorParsingErrors = error{
        InvalidQuerySector,
        InvalidQueryOffset,
        InvalidQueryLimit,
        OffsetValueNegative,
        LimitValueNegative,
    };

    pub fn parse(doc: *const bson.BSONValue) SectorParsingErrors!Sector {
        if (doc.* != bson.BSONValueType.document) return SectorParsingErrors.InvalidQuerySector;
        var offset: ?u64 = null;
        const document = doc.document;
        if (document.get("offset")) |offsetVal| switch (offsetVal) {
            .int32 => |*offsetValue| {
                if (offsetValue.value < 0) return SectorParsingErrors.OffsetValueNegative;
                offset = @intCast(offsetValue.value);
            },

            .int64 => |*offsetValue| {
                if (offsetValue.value < 0) return SectorParsingErrors.OffsetValueNegative;
                offset = @intCast(offsetValue.value);
            },

            .double => |*offsetValue| {
                if (offsetValue.value < 0) return SectorParsingErrors.OffsetValueNegative;
                offset = @intFromFloat(offsetValue.value);
            },
            else => return SectorParsingErrors.InvalidQueryOffset,
        };
        var limit: ?u64 = null;
        if (document.get("limit")) |limitVal| switch (limitVal) {
            .int32 => |*limitValue| {
                if (limitValue.value < 0) return SectorParsingErrors.LimitValueNegative;
                limit = @intCast(limitValue.value);
            },

            .int64 => |*limitValue| {
                if (limitValue.value < 0) return SectorParsingErrors.LimitValueNegative;
                limit = @intCast(limitValue.value);
            },

            .double => |*limitValue| {
                if (limitValue.value < 0) return SectorParsingErrors.LimitValueNegative;
                limit = @intFromFloat(limitValue.value);
            },
            else => return SectorParsingErrors.InvalidQueryOffset,
        };
        const sector = Sector{
            .offset = offset,
            .limit = limit,
        };
        return sector;
    }
};

test "Sector.parse" {
    const ally = std.testing.allocator;
    const sectorDoc = try bson.fmt.serialize(.{ .offset = 10, .limit = 10 }, ally);
    defer sectorDoc.deinit(ally);
    const sector = try Sector.parse(&bson.BSONValue{ .document = sectorDoc });
    try std.testing.expectEqual(sector.offset, 10);
    try std.testing.expectEqual(sector.limit, 10);
}

pub const CursorMode = enum {
    full_scan,
    index_range,
};

pub const CursorAnchor = struct {
    doc_id: bson.ObjectId,
    user_id: bson.BSONValue,
    page_id: u64,
    offset: u16,
};

pub const Cursor = struct {
    version: u32,
    mode: CursorMode,
    index_path: ?[]const u8,
    anchor: ?CursorAnchor,

    pub const CursorParsingErrors = error{
        InvalidCursor,
        InvalidCursorVersion,
        InvalidCursorMode,
        MissingCursorIndexPath,
        InvalidCursorAnchor,
        InvalidCursorDocId,
        InvalidCursorPageId,
        InvalidCursorOffset,
    };

    fn parsePositiveInteger(value: bson.BSONValue) CursorParsingErrors!u64 {
        return switch (value) {
            .int32 => |v| blk: {
                if (v.value < 0) return error.InvalidCursor;
                break :blk @intCast(v.value);
            },
            .int64 => |v| blk: {
                if (v.value < 0) return error.InvalidCursor;
                break :blk @intCast(v.value);
            },
            .double => |v| blk: {
                if (v.value < 0) return error.InvalidCursor;
                break :blk @intFromFloat(v.value);
            },
            else => return error.InvalidCursor,
        };
    }

    pub fn parse(doc: *const bson.BSONValue) CursorParsingErrors!Cursor {
        if (doc.* != .document) return error.InvalidCursor;
        const cursor_doc = doc.document;

        const version_value = cursor_doc.get("version") orelse return error.InvalidCursorVersion;
        const version = try parsePositiveInteger(version_value);
        if (version != 1) return error.InvalidCursorVersion;

        const mode_value = cursor_doc.get("mode") orelse return error.InvalidCursorMode;
        if (mode_value != .string) return error.InvalidCursorMode;
        const mode = if (std.mem.eql(u8, mode_value.string.value, "full_scan"))
            CursorMode.full_scan
        else if (std.mem.eql(u8, mode_value.string.value, "index_range"))
            CursorMode.index_range
        else
            return error.InvalidCursorMode;

        const index_path = if (cursor_doc.get("indexPath")) |value| blk: {
            if (value != .string or value.string.value.len == 0) return error.MissingCursorIndexPath;
            break :blk value.string.value;
        } else null;

        if (mode == .index_range and index_path == null) {
            return error.MissingCursorIndexPath;
        }

        const anchor = if (cursor_doc.get("anchor")) |anchor_value| blk: {
            if (anchor_value != .document) return error.InvalidCursorAnchor;
            const anchor_doc = anchor_value.document;

            const doc_id_value = anchor_doc.get("docId") orelse return error.InvalidCursorDocId;
            if (doc_id_value != .objectId) return error.InvalidCursorDocId;

            const user_id = anchor_doc.get("_id") orelse return error.InvalidCursorAnchor;
            const page_id_value = anchor_doc.get("pageId") orelse return error.InvalidCursorPageId;
            const offset_value = anchor_doc.get("offset") orelse return error.InvalidCursorOffset;
            const page_id = try parsePositiveInteger(page_id_value);
            const offset_u64 = try parsePositiveInteger(offset_value);
            if (offset_u64 > std.math.maxInt(u16)) return error.InvalidCursorOffset;

            break :blk CursorAnchor{
                .doc_id = doc_id_value.objectId.value,
                .user_id = user_id,
                .page_id = page_id,
                .offset = @intCast(offset_u64),
            };
        } else null;

        return .{
            .version = @intCast(version),
            .mode = mode,
            .index_path = index_path,
            .anchor = anchor,
        };
    }
};

pub const Query = struct {
    filters: []Filter,
    sortConfig: ?SortConfig,
    sector: ?Sector,
    cursor: ?Cursor,
    // projection: bson.Document,

    pub fn parseRaw(ally: Allocator, rawQuery: []const u8) QueryParsingErrors!Query {
        const queryDoc = bson.BSONDocument.init(rawQuery);

        return parse(ally, queryDoc);
    }

    pub const QueryParsingErrors = (SortConfig.SortParsingErrors ||
        Filter.FilterParsingErrors ||
        Sector.SectorParsingErrors ||
        Cursor.CursorParsingErrors ||
        Allocator.Error);

    pub fn parse(ally: Allocator, queryDoc: bson.BSONDocument) QueryParsingErrors!Query {
        const filterDoc = queryDoc.get("query");
        const filters = if (filterDoc) |doc| try Filter.parse(ally, doc) else try ally.alloc(Filter, 0);
        const sortDoc = queryDoc.get("sort");
        const sortConfig = if (sortDoc) |doc| try SortConfig.parse(ally, doc) else null;
        const sectorDoc = queryDoc.get("sector");
        const sector = if (sectorDoc) |*doc| try Sector.parse(doc) else null;
        const cursorDoc = queryDoc.get("cursor");
        const cursor = if (cursorDoc) |*doc| try Cursor.parse(doc) else null;

        const query = Query{
            .filters = filters,
            .sortConfig = sortConfig,
            .sector = sector,
            .cursor = cursor,
            // .projection = queryDoc.get("projection") catch bson.Document{},
        };

        return query;
    }

    pub fn deinit(self: *Query, ally: Allocator) void {
        for (self.filters) |*filter| filter.deinit(ally);
        ally.free(self.filters);
        if (self.sortConfig) |sortConfig| {
            switch (sortConfig) {
                .asc => |ascSort| ally.free(ascSort),
                .desc => |descSort| ally.free(descSort),
            }
        }
    }

    pub fn match(self: Query, doc: *const bson.BSONDocument) Filter.FilterMatchErrors!bool {
        for (self.filters) |filter| {
            if (!try filter.match(doc)) return false;
        }
        return true;
    }

    pub fn sort(sortConfig: SortConfig, a: bson.BSONDocument, b: bson.BSONDocument) bool {
        const path = switch (sortConfig) {
            .asc => |ascSort| ascSort,
            .desc => |descSort| descSort,
        };

        const aValue = a.getPath(path);
        const bValue = b.getPath(path);
        if (aValue) |aVal| {
            if (bValue) |bVal| {
                if (sortConfig == .asc) {
                    return aVal.order(bVal) == .lt;
                } else {
                    return aVal.order(bVal) == .gt;
                }
            } else {
                return true;
            }
        } else {
            return false;
        }
    }
};

test "Query.parse" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    const ally = arena.allocator();
    defer arena.deinit();

    const queryDoc = try bson.fmt.serialize(.{
        .query = .{
            .@"field.subfield" = .{
                .@"$eq" = "value",
            },
        },
        .sort = .{
            .asc = "field",
        },
        .sector = .{
            .offset = 0,
            .limit = 10,
        },
    }, ally);
    // const buffer = try ally.alloc(u8, queryDoc.len);
    // queryDoc.serializeToMemory(buffer);
    var query = try Query.parse(ally, queryDoc);
    defer query.deinit(ally);
}

test "Query.parse cursor full_scan" {
    const ally = std.testing.allocator;
    const doc_id = try bson.ObjectId.parseString("507c7f79bcf86cd7994f6c0e");

    var anchor = bson.BSONDocument.initEmpty();
    var next_anchor = try anchor.set(ally, "docId", bson.BSONValue.init(doc_id));
    defer anchor.deinit(ally);
    anchor = next_anchor;
    next_anchor = try anchor.set(ally, "_id", .{ .string = .{ .value = "user-1" } });
    anchor.deinit(ally);
    anchor = next_anchor;
    next_anchor = try anchor.set(ally, "pageId", .{ .int64 = .{ .value = 4 } });
    anchor.deinit(ally);
    anchor = next_anchor;
    next_anchor = try anchor.set(ally, "offset", .{ .int32 = .{ .value = 16 } });
    anchor.deinit(ally);
    anchor = next_anchor;

    var cursor = bson.BSONDocument.initEmpty();
    var next_cursor = try cursor.set(ally, "version", .{ .int32 = .{ .value = 1 } });
    defer cursor.deinit(ally);
    cursor = next_cursor;
    next_cursor = try cursor.set(ally, "mode", .{ .string = .{ .value = "full_scan" } });
    cursor.deinit(ally);
    cursor = next_cursor;
    next_cursor = try cursor.set(ally, "anchor", bson.BSONValue.init(anchor));
    cursor.deinit(ally);
    cursor = next_cursor;

    var queryDoc = bson.BSONDocument.initEmpty();
    const next_query_doc = try queryDoc.set(ally, "cursor", bson.BSONValue.init(cursor));
    defer queryDoc.deinit(ally);
    queryDoc = next_query_doc;

    var parsed = try Query.parse(ally, queryDoc);
    defer parsed.deinit(ally);
    try std.testing.expect(parsed.cursor != null);
    try std.testing.expectEqual(CursorMode.full_scan, parsed.cursor.?.mode);
    try std.testing.expectEqual(doc_id.toInt(), parsed.cursor.?.anchor.?.doc_id.toInt());
    try std.testing.expectEqual(@as(u64, 4), parsed.cursor.?.anchor.?.page_id);
    try std.testing.expectEqual(@as(u16, 16), parsed.cursor.?.anchor.?.offset);
}

test "Query.parse cursor index_range" {
    const ally = std.testing.allocator;
    const doc_id = try bson.ObjectId.parseString("507c7f79bcf86cd7994f6c0f");

    var anchor = bson.BSONDocument.initEmpty();
    var next_anchor = try anchor.set(ally, "docId", bson.BSONValue.init(doc_id));
    defer anchor.deinit(ally);
    anchor = next_anchor;
    next_anchor = try anchor.set(ally, "_id", .{ .string = .{ .value = "user-2" } });
    anchor.deinit(ally);
    anchor = next_anchor;
    next_anchor = try anchor.set(ally, "pageId", .{ .int64 = .{ .value = 8 } });
    anchor.deinit(ally);
    anchor = next_anchor;
    next_anchor = try anchor.set(ally, "offset", .{ .int32 = .{ .value = 24 } });
    anchor.deinit(ally);
    anchor = next_anchor;

    var cursor = bson.BSONDocument.initEmpty();
    var next_cursor = try cursor.set(ally, "version", .{ .int32 = .{ .value = 1 } });
    defer cursor.deinit(ally);
    cursor = next_cursor;
    next_cursor = try cursor.set(ally, "mode", .{ .string = .{ .value = "index_range" } });
    cursor.deinit(ally);
    cursor = next_cursor;
    next_cursor = try cursor.set(ally, "indexPath", .{ .string = .{ .value = "age" } });
    cursor.deinit(ally);
    cursor = next_cursor;
    next_cursor = try cursor.set(ally, "anchor", bson.BSONValue.init(anchor));
    cursor.deinit(ally);
    cursor = next_cursor;

    var queryDoc = bson.BSONDocument.initEmpty();
    const next_query_doc = try queryDoc.set(ally, "cursor", bson.BSONValue.init(cursor));
    defer queryDoc.deinit(ally);
    queryDoc = next_query_doc;

    var parsed = try Query.parse(ally, queryDoc);
    defer parsed.deinit(ally);
    try std.testing.expect(parsed.cursor != null);
    try std.testing.expectEqual(CursorMode.index_range, parsed.cursor.?.mode);
    try std.testing.expect(std.mem.eql(u8, parsed.cursor.?.index_path.?, "age"));
}

test "Query.parse cursor rejects invalid version" {
    const ally = std.testing.allocator;
    var cursor = bson.BSONDocument.initEmpty();
    var next_cursor = try cursor.set(ally, "version", .{ .int32 = .{ .value = 2 } });
    defer cursor.deinit(ally);
    cursor = next_cursor;
    next_cursor = try cursor.set(ally, "mode", .{ .string = .{ .value = "full_scan" } });
    cursor.deinit(ally);
    cursor = next_cursor;

    var queryDoc = bson.BSONDocument.initEmpty();
    const next_query_doc = try queryDoc.set(ally, "cursor", bson.BSONValue.init(cursor));
    defer queryDoc.deinit(ally);
    queryDoc = next_query_doc;

    try std.testing.expectError(error.InvalidCursorVersion, Query.parse(ally, queryDoc));
}

test "Query.parse cursor rejects missing index path" {
    const ally = std.testing.allocator;
    var cursor = bson.BSONDocument.initEmpty();
    var next_cursor = try cursor.set(ally, "version", .{ .int32 = .{ .value = 1 } });
    defer cursor.deinit(ally);
    cursor = next_cursor;
    next_cursor = try cursor.set(ally, "mode", .{ .string = .{ .value = "index_range" } });
    cursor.deinit(ally);
    cursor = next_cursor;

    var queryDoc = bson.BSONDocument.initEmpty();
    const next_query_doc = try queryDoc.set(ally, "cursor", bson.BSONValue.init(cursor));
    defer queryDoc.deinit(ally);
    queryDoc = next_query_doc;

    try std.testing.expectError(error.MissingCursorIndexPath, Query.parse(ally, queryDoc));
}

test "Query.match matches objectId correctly" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    const ally = arena.allocator();
    defer arena.deinit();

    const objId = try bson.ObjectId.init();

    var doc = bson.BSONDocument.initEmpty();
    doc = try doc.set(ally, "_id", bson.BSONValue.init(objId));
    defer doc.deinit(ally);

    const objId2 = try bson.ObjectId.init();
    var doc2 = bson.BSONDocument.initEmpty();
    doc2 = try doc.set(ally, "_id", bson.BSONValue.init(objId2));
    defer doc2.deinit(ally);

    var queryDoc = bson.BSONDocument.initEmpty();
    queryDoc = try queryDoc.set(ally, "query", bson.BSONValue.init(doc));
    defer queryDoc.deinit(ally);
    // const buffer = try ally.alloc(u8, queryDoc.len);
    // queryDoc.serializeToMemory(buffer);
    var query = try Query.parse(ally, queryDoc);

    try std.testing.expect(try query.match(&doc));
    try std.testing.expect(!(try query.match(&doc2)));
    defer query.deinit(ally);
}
