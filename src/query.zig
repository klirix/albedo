/// queries will be simple
/// There are filters, sorts, sectoring and projections
/// filters: $eq, $ne, $lt, $gt, $in
/// sorts: $asc, $desc
/// sectoring: $offset, $limit
/// projections: $fields
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

const Path = [][]const u8; // "field.subfield".split('.')

pub const FilterType = enum { eq, ne, lt, lte, gt, gte, in, between, startsWith, endsWith, exists, notExists };

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

    fn parseDoc(ally: Allocator, doc: bson.BSONDocument) FilterParsingErrors![]Filter {
        const filterNameMap = comptime std.StaticStringMap(FilterType).initComptime(.{
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
        });
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
                .string, .int32, .int64, .objectId, .datetime, .boolean, .null => {
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

    pub fn matchValue(self: *const Filter, valueToMatch: bson.BSONValue) bool {
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
            .exists => |_| {
                return true;
            },
            .notExists => |_| {
                return false;
            },
        }
    }

    pub fn match(self: *const Filter, doc: *const bson.BSONDocument) bool {
        const valueToMatch = switch (self.*) {
            .notExists => |e| blk: {
                // For notExists, return true if field doesn't exist, false if it exists
                break :blk doc.getPath(e.path) orelse return true;
            },
            inline .eq, .ne, .lt, .lte, .gte, .gt, .in, .between, .startsWith, .endsWith, .exists => |e| blk: {
                break :blk doc.getPath(e.path) orelse return false;
            },
        };
        return matchValue(self, valueToMatch);
    }
};

test "Filter.parse" {
    const ally = std.testing.allocator;
    var opPairs = [_]bson.BSONKeyValuePair{
        .{ .key = "$eq", .value = .{ .string = bson.BSONString{ .value = "value" } } },
    };
    const opDoc = try bson.BSONDocument.fromPairs(ally, &opPairs);
    defer opDoc.deinit(ally);
    var filterPairs = [_]bson.BSONKeyValuePair{
        .{ .key = "field.subfield", .value = .{ .document = opDoc } },
    };
    const filterDoc = try bson.BSONDocument.fromPairs(ally, &filterPairs);
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
    var opPairs = [_]bson.BSONKeyValuePair{
        .{ .key = "$gte", .value = .{ .int32 = .{ .value = 10 } } },
    };
    const opDoc = try bson.BSONDocument.fromPairs(ally, &opPairs);
    defer opDoc.deinit(ally);
    var filterPairs = [_]bson.BSONKeyValuePair{
        .{ .key = "score", .value = .{ .document = opDoc } },
    };
    const filterDoc = try bson.BSONDocument.fromPairs(ally, &filterPairs);
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
    var opPairs = [_]bson.BSONKeyValuePair{
        .{ .key = "$lte", .value = .{ .int32 = .{ .value = 20 } } },
    };
    const opDoc = try bson.BSONDocument.fromPairs(ally, &opPairs);
    defer opDoc.deinit(ally);
    var filterPairs = [_]bson.BSONKeyValuePair{
        .{ .key = "score", .value = .{ .document = opDoc } },
    };
    const filterDoc = try bson.BSONDocument.fromPairs(ally, &filterPairs);
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
    var opPairs = [_]bson.BSONKeyValuePair{
        .{ .key = "$startsWith", .value = .{ .string = bson.BSONString{ .value = "prefix" } } },
    };
    const opDoc = try bson.BSONDocument.fromPairs(ally, &opPairs);
    defer opDoc.deinit(ally);
    var filterPairs = [_]bson.BSONKeyValuePair{
        .{ .key = "name", .value = .{ .document = opDoc } },
    };
    const filterDoc = try bson.BSONDocument.fromPairs(ally, &filterPairs);
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
    var opPairs = [_]bson.BSONKeyValuePair{
        .{ .key = "$startsWith", .value = .{ .string = bson.BSONString{ .value = "hello" } } },
    };
    const opDoc = try bson.BSONDocument.fromPairs(ally, &opPairs);
    defer opDoc.deinit(ally);
    var filterPairs = [_]bson.BSONKeyValuePair{
        .{ .key = "message", .value = .{ .document = opDoc } },
    };
    const filterDoc = try bson.BSONDocument.fromPairs(ally, &filterPairs);
    defer filterDoc.deinit(ally);
    const filters = try Filter.parse(ally, bson.BSONValue{ .document = filterDoc });
    defer {
        for (filters) |*f| f.deinit(ally);
        ally.free(filters);
    }
    const filter = &filters[0];

    // Test positive cases
    const matchingValue1 = bson.BSONValue{ .string = bson.BSONString{ .value = "hello world" } };
    try std.testing.expect(filter.matchValue(matchingValue1));

    const matchingValue2 = bson.BSONValue{ .string = bson.BSONString{ .value = "hello" } };
    try std.testing.expect(filter.matchValue(matchingValue2));

    // Test negative cases
    const nonMatchingValue1 = bson.BSONValue{ .string = bson.BSONString{ .value = "world hello" } };
    try std.testing.expect(!filter.matchValue(nonMatchingValue1));

    const nonMatchingValue2 = bson.BSONValue{ .string = bson.BSONString{ .value = "hel" } };
    try std.testing.expect(!filter.matchValue(nonMatchingValue2));

    const emptyString = bson.BSONValue{ .string = bson.BSONString{ .value = "" } };
    try std.testing.expect(!filter.matchValue(emptyString));

    // Test non-string value
    const nonStringValue = bson.BSONValue{ .int32 = .{ .value = 42 } };
    try std.testing.expect(!filter.matchValue(nonStringValue));
}

test "Filter.matchValue startsWith with empty prefix" {
    const ally = std.testing.allocator;
    var opPairs = [_]bson.BSONKeyValuePair{
        .{ .key = "$startsWith", .value = .{ .string = bson.BSONString{ .value = "" } } },
    };
    const opDoc = try bson.BSONDocument.fromPairs(ally, &opPairs);
    defer opDoc.deinit(ally);
    var filterPairs = [_]bson.BSONKeyValuePair{
        .{ .key = "message", .value = .{ .document = opDoc } },
    };
    const filterDoc = try bson.BSONDocument.fromPairs(ally, &filterPairs);
    defer filterDoc.deinit(ally);
    const filters = try Filter.parse(ally, bson.BSONValue{ .document = filterDoc });
    defer {
        for (filters) |*f| f.deinit(ally);
        ally.free(filters);
    }
    const filter = &filters[0];

    // Empty prefix should match any string
    const anyString = bson.BSONValue{ .string = bson.BSONString{ .value = "anything" } };
    try std.testing.expect(filter.matchValue(anyString));

    const emptyString = bson.BSONValue{ .string = bson.BSONString{ .value = "" } };
    try std.testing.expect(filter.matchValue(emptyString));
}

test "Filter.parse handles endsWith" {
    const ally = std.testing.allocator;
    var opPairs = [_]bson.BSONKeyValuePair{
        .{ .key = "$endsWith", .value = .{ .string = bson.BSONString{ .value = "suffix" } } },
    };
    const opDoc = try bson.BSONDocument.fromPairs(ally, &opPairs);
    defer opDoc.deinit(ally);
    var filterPairs = [_]bson.BSONKeyValuePair{
        .{ .key = "filename", .value = .{ .document = opDoc } },
    };
    const filterDoc = try bson.BSONDocument.fromPairs(ally, &filterPairs);
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
    var opPairs = [_]bson.BSONKeyValuePair{
        .{ .key = "$endsWith", .value = .{ .string = bson.BSONString{ .value = ".txt" } } },
    };
    const opDoc = try bson.BSONDocument.fromPairs(ally, &opPairs);
    defer opDoc.deinit(ally);
    var filterPairs = [_]bson.BSONKeyValuePair{
        .{ .key = "filename", .value = .{ .document = opDoc } },
    };
    const filterDoc = try bson.BSONDocument.fromPairs(ally, &filterPairs);
    defer filterDoc.deinit(ally);
    const filters = try Filter.parse(ally, bson.BSONValue{ .document = filterDoc });
    defer {
        for (filters) |*f| f.deinit(ally);
        ally.free(filters);
    }
    const filter = &filters[0];

    // Test positive cases
    const matchingValue1 = bson.BSONValue{ .string = bson.BSONString{ .value = "document.txt" } };
    try std.testing.expect(filter.matchValue(matchingValue1));

    const matchingValue2 = bson.BSONValue{ .string = bson.BSONString{ .value = ".txt" } };
    try std.testing.expect(filter.matchValue(matchingValue2));

    // Test negative cases
    const nonMatchingValue1 = bson.BSONValue{ .string = bson.BSONString{ .value = "document.pdf" } };
    try std.testing.expect(!filter.matchValue(nonMatchingValue1));

    const nonMatchingValue2 = bson.BSONValue{ .string = bson.BSONString{ .value = "txt" } };
    try std.testing.expect(!filter.matchValue(nonMatchingValue2));

    const emptyString = bson.BSONValue{ .string = bson.BSONString{ .value = "" } };
    try std.testing.expect(!filter.matchValue(emptyString));

    // Test non-string value
    const nonStringValue = bson.BSONValue{ .int32 = .{ .value = 42 } };
    try std.testing.expect(!filter.matchValue(nonStringValue));
}

test "Filter.matchValue endsWith with empty suffix" {
    const ally = std.testing.allocator;
    var opPairs = [_]bson.BSONKeyValuePair{
        .{ .key = "$endsWith", .value = .{ .string = bson.BSONString{ .value = "" } } },
    };
    const opDoc = try bson.BSONDocument.fromPairs(ally, &opPairs);
    defer opDoc.deinit(ally);
    var filterPairs = [_]bson.BSONKeyValuePair{
        .{ .key = "filename", .value = .{ .document = opDoc } },
    };
    const filterDoc = try bson.BSONDocument.fromPairs(ally, &filterPairs);
    defer filterDoc.deinit(ally);
    const filters = try Filter.parse(ally, bson.BSONValue{ .document = filterDoc });
    defer {
        for (filters) |*f| f.deinit(ally);
        ally.free(filters);
    }
    const filter = &filters[0];

    // Empty suffix should match any string
    const anyString = bson.BSONValue{ .string = bson.BSONString{ .value = "anything" } };
    try std.testing.expect(filter.matchValue(anyString));

    const emptyString = bson.BSONValue{ .string = bson.BSONString{ .value = "" } };
    try std.testing.expect(filter.matchValue(emptyString));
}

test "Filter.parse handles exists" {
    const ally = std.testing.allocator;
    var opPairs = [_]bson.BSONKeyValuePair{
        .{ .key = "$exists", .value = .{ .boolean = .{ .value = true } } },
    };
    const opDoc = try bson.BSONDocument.fromPairs(ally, &opPairs);
    defer opDoc.deinit(ally);
    var filterPairs = [_]bson.BSONKeyValuePair{
        .{ .key = "email", .value = .{ .document = opDoc } },
    };
    const filterDoc = try bson.BSONDocument.fromPairs(ally, &filterPairs);
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
    var opPairs = [_]bson.BSONKeyValuePair{
        .{ .key = "$exists", .value = .{ .boolean = .{ .value = true } } },
    };
    const opDoc = try bson.BSONDocument.fromPairs(ally, &opPairs);
    defer opDoc.deinit(ally);
    var filterPairs = [_]bson.BSONKeyValuePair{
        .{ .key = "field", .value = .{ .document = opDoc } },
    };
    const filterDoc = try bson.BSONDocument.fromPairs(ally, &filterPairs);
    defer filterDoc.deinit(ally);
    const filters = try Filter.parse(ally, bson.BSONValue{ .document = filterDoc });
    defer {
        for (filters) |*f| f.deinit(ally);
        ally.free(filters);
    }
    const filter = &filters[0];

    // Exists filter should always return true for any value when field exists
    const stringValue = bson.BSONValue{ .string = bson.BSONString{ .value = "test" } };
    try std.testing.expect(filter.matchValue(stringValue));

    const intValue = bson.BSONValue{ .int32 = .{ .value = 42 } };
    try std.testing.expect(filter.matchValue(intValue));

    const boolValue = bson.BSONValue{ .boolean = .{ .value = false } };
    try std.testing.expect(filter.matchValue(boolValue));
}

test "Filter.match exists checks field presence in document" {
    const ally = std.testing.allocator;
    var opPairs = [_]bson.BSONKeyValuePair{
        .{ .key = "$exists", .value = .{ .boolean = .{ .value = true } } },
    };
    const opDoc = try bson.BSONDocument.fromPairs(ally, &opPairs);
    defer opDoc.deinit(ally);
    var filterPairs = [_]bson.BSONKeyValuePair{
        .{ .key = "email", .value = .{ .document = opDoc } },
    };
    const filterDoc = try bson.BSONDocument.fromPairs(ally, &filterPairs);
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
    try std.testing.expect(filter.match(&docWithField));

    // Document without the field should not match
    var docWithoutField = bson.BSONDocument.initEmpty();
    docWithoutField = try docWithoutField.set(ally, "name", bson.BSONValue{ .string = bson.BSONString{ .value = "John" } });
    defer docWithoutField.deinit(ally);
    try std.testing.expect(!filter.match(&docWithoutField));
}

test "Filter.parse handles notExists" {
    const ally = std.testing.allocator;
    var opPairs = [_]bson.BSONKeyValuePair{
        .{ .key = "$notExists", .value = .{ .boolean = .{ .value = true } } },
    };
    const opDoc = try bson.BSONDocument.fromPairs(ally, &opPairs);
    defer opDoc.deinit(ally);
    var filterPairs = [_]bson.BSONKeyValuePair{
        .{ .key = "deletedAt", .value = .{ .document = opDoc } },
    };
    const filterDoc = try bson.BSONDocument.fromPairs(ally, &filterPairs);
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
    var opPairs = [_]bson.BSONKeyValuePair{
        .{ .key = "$notExists", .value = .{ .boolean = .{ .value = true } } },
    };
    const opDoc = try bson.BSONDocument.fromPairs(ally, &opPairs);
    defer opDoc.deinit(ally);
    var filterPairs = [_]bson.BSONKeyValuePair{
        .{ .key = "field", .value = .{ .document = opDoc } },
    };
    const filterDoc = try bson.BSONDocument.fromPairs(ally, &filterPairs);
    defer filterDoc.deinit(ally);
    const filters = try Filter.parse(ally, bson.BSONValue{ .document = filterDoc });
    defer {
        for (filters) |*f| f.deinit(ally);
        ally.free(filters);
    }
    const filter = &filters[0];

    // notExists filter should always return false when a value exists
    const stringValue = bson.BSONValue{ .string = bson.BSONString{ .value = "test" } };
    try std.testing.expect(!filter.matchValue(stringValue));

    const intValue = bson.BSONValue{ .int32 = .{ .value = 42 } };
    try std.testing.expect(!filter.matchValue(intValue));

    const boolValue = bson.BSONValue{ .boolean = .{ .value = false } };
    try std.testing.expect(!filter.matchValue(boolValue));
}

test "Filter.match notExists checks field absence in document" {
    const ally = std.testing.allocator;
    var opPairs = [_]bson.BSONKeyValuePair{
        .{ .key = "$notExists", .value = .{ .boolean = .{ .value = true } } },
    };
    const opDoc = try bson.BSONDocument.fromPairs(ally, &opPairs);
    defer opDoc.deinit(ally);
    var filterPairs = [_]bson.BSONKeyValuePair{
        .{ .key = "deletedAt", .value = .{ .document = opDoc } },
    };
    const filterDoc = try bson.BSONDocument.fromPairs(ally, &filterPairs);
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
    try std.testing.expect(!filter.match(&docWithField));

    // Document without the field should match (returns true when field doesn't exist)
    var docWithoutField = bson.BSONDocument.initEmpty();
    docWithoutField = try docWithoutField.set(ally, "createdAt", bson.BSONValue{ .string = bson.BSONString{ .value = "2024-01-01" } });
    defer docWithoutField.deinit(ally);
    try std.testing.expect(filter.match(&docWithoutField));
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

    pub fn parse(doc: bson.BSONValue) SortParsingErrors!SortConfig {
        if (doc != bson.BSONValueType.document) return error.InvalidQuerySort;

        if (doc.document.keyNumber() != 1) return error.InvalidQuerySortParamLength;

        var iter = doc.document.iter();
        const pair = iter.next() orelse return error.InvalidQuerySort;

        if (pair.value != bson.BSONValueType.string) return error.InvalidQuerySortParamType;

        const path = pair.value.string.value;
        if (path.len == 0) return error.InvalidQueryPath;
        if (std.mem.eql(u8, pair.key, "asc")) {
            return SortConfig{ .asc = path };
        } else if (std.mem.eql(u8, pair.key, "desc")) {
            return SortConfig{ .desc = path };
        } else {
            return error.InvalidQuerySort;
        }
    }
};

test "Sort.parse" {
    const ally = std.testing.allocator;
    const sortDoc = try bson.BSONDocument.fromTuple(ally, .{
        .asc = bson.BSONValue{ .string = bson.BSONString{ .value = "field" } },
    });
    defer sortDoc.deinit(ally);
    const sort = try SortConfig.parse(bson.BSONValue{ .document = sortDoc });

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
    const sectorDoc = try bson.BSONDocument.fromTuple(ally, .{
        .offset = bson.BSONValue{ .int32 = .{ .value = 10 } },
        .limit = bson.BSONValue{ .int32 = .{ .value = 10 } },
    });
    defer sectorDoc.deinit(ally);
    const sector = try Sector.parse(&bson.BSONValue{ .document = sectorDoc });
    try std.testing.expectEqual(sector.offset, 10);
    try std.testing.expectEqual(sector.limit, 10);
}

pub const Query = struct {
    const defaultFilters = [0]Filter{};

    filters: []Filter,
    sortConfig: ?SortConfig,
    sector: ?Sector,
    // projection: bson.Document,

    pub fn parseRaw(ally: Allocator, rawQuery: []const u8) QueryParsingErrors!Query {
        const queryDoc = bson.BSONDocument.init(rawQuery);

        return parse(ally, queryDoc);
    }

    pub const QueryParsingErrors = (SortConfig.SortParsingErrors ||
        Filter.FilterParsingErrors ||
        Sector.SectorParsingErrors);

    pub fn parse(ally: Allocator, queryDoc: bson.BSONDocument) QueryParsingErrors!Query {
        const filterDoc = queryDoc.get("query");
        const filters = if (filterDoc) |doc| try Filter.parse(ally, doc) else @constCast(defaultFilters[0..]);
        const sortDoc = queryDoc.get("sort");
        const sortConfig = if (sortDoc) |doc| try SortConfig.parse(doc) else null;
        const sectorDoc = queryDoc.get("sector");
        const sector = if (sectorDoc) |*doc| try Sector.parse(doc) else null;

        const query = Query{
            .filters = filters,
            .sortConfig = sortConfig,
            .sector = sector,
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

    pub fn match(self: Query, doc: *const bson.BSONDocument) bool {
        for (self.filters) |filter| {
            if (!filter.match(doc)) return false;
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

    const queryDoc = try bson.BSONDocument.fromJSON(ally,
        \\ {
        \\   "query": {
        \\      "field.subfield": {"$eq": "value"}
        \\   },
        \\   "sort": {"asc": "field"},
        \\   "sector": {"offset": 0, "limit": 10}
        \\ }
    );
    // const buffer = try ally.alloc(u8, queryDoc.len);
    // queryDoc.serializeToMemory(buffer);
    var query = try Query.parse(ally, queryDoc);
    defer query.deinit(ally);
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

    try std.testing.expect(query.match(&doc));
    try std.testing.expect(!query.match(&doc2));
    defer query.deinit(ally);
}
