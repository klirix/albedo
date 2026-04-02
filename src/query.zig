/// queries will be simple
/// There are filters, sorts, sectoring and projections
/// filters: $eq, $ne, $lt, $lte, $gt, $gte, $in, $between, $startsWith, $endsWith, $exists, $notExists
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

const Path = [][]const u8; // "field.subfield".split('.')

pub const FilterType = enum { eq, ne, lt, lte, gt, gte, in, between, startsWith, endsWith, exists, notExists, @"or", @"and", nor };

/// A group of filters that are AND-ed together (used inside logical operators).
pub const FilterGroup = []Filter;

pub const PathValuePair = struct {
    path: []const u8,
    value: bson.BSONValue,

    /// Duplicate the slice data inside a BSONValue so the filter owns it.
    pub fn dupeValue(ally: Allocator, v: bson.BSONValue) Allocator.Error!bson.BSONValue {
        return switch (v) {
            .string => |s| bson.BSONValue{ .string = .{ .value = try ally.dupe(u8, s.value) } },
            .binary => |b| bson.BSONValue{ .binary = .{ .value = try ally.dupe(u8, b.value), .subtype = b.subtype } },
            .array => |a| bson.BSONValue{ .array = bson.BSONDocument{ .buffer = try ally.dupe(u8, a.buffer) } },
            .document => |d| bson.BSONValue{ .document = bson.BSONDocument{ .buffer = try ally.dupe(u8, d.buffer) } },
            else => v, // scalar types (int32, int64, double, bool, null, objectId, datetime, minKey, maxKey) are self-contained
        };
    }

    /// Free any heap-owned slice data previously created by dupeValue.
    pub fn freeValue(ally: Allocator, v: bson.BSONValue) void {
        switch (v) {
            .string => |s| ally.free(s.value),
            .binary => |b| ally.free(b.value),
            .array => |a| ally.free(a.buffer),
            .document => |d| ally.free(d.buffer),
            else => {},
        }
    }
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
    /// At least one group must match.
    @"or": []FilterGroup,
    /// All groups must match (explicit AND for grouping/composability).
    @"and": []FilterGroup,
    /// No group must match.
    nor: []FilterGroup,

    pub fn deinit(self: *Filter, ally: Allocator) void {
        switch (self.*) {
            .in => |*inFilter| {
                PathValuePair.freeValue(ally, inFilter.value);
                ally.free(inFilter.path);
            },
            .between => |*betweenFilter| {
                PathValuePair.freeValue(ally, betweenFilter.value);
                ally.free(betweenFilter.path);
            },
            .eq, .ne, .lt, .gt, .lte, .gte, .startsWith, .endsWith, .exists, .notExists => |*filter| {
                PathValuePair.freeValue(ally, filter.value);
                ally.free(filter.path);
            },
            .@"or", .@"and", .nor => |groups| {
                for (groups) |group| {
                    for (group) |*f| f.deinit(ally);
                    ally.free(group);
                }
                ally.free(groups);
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
        const logicalOpMap = comptime std.StaticStringMap(FilterType).initComptime(.{
            .{ "$or", .@"or" },
            .{ "$and", .@"and" },
            .{ "$nor", .nor },
        });
        var filters = std.ArrayListUnmanaged(Filter){};
        defer filters.deinit(ally);

        var iter = doc.iter();
        while (iter.next()) |pair| {
            const path = pair.key;
            if (path.len == 0) return FilterParsingErrors.InvalidQueryPath;

            // Handle logical operators ($or, $and, $nor) at the top level.
            if (logicalOpMap.get(path)) |logical_op| {
                // Accept both BSON array and document (tuple-style) values.
                const groups_doc: bson.BSONDocument = switch (pair.value) {
                    .array => |arr| arr,
                    .document => |d| d,
                    else => return FilterParsingErrors.InvalidQueryFilter,
                };
                var group_list = std.ArrayListUnmanaged(FilterGroup){};
                defer group_list.deinit(ally);
                var arr_iter = groups_doc.iter();
                while (arr_iter.next()) |arr_pair| {
                    if (arr_pair.value != .document) return FilterParsingErrors.InvalidQueryFilter;
                    const group = try parseDoc(ally, arr_pair.value.document);
                    errdefer {
                        const g = group;
                        for (g) |*f| f.deinit(ally);
                        ally.free(g);
                    }
                    try group_list.append(ally, group);
                }
                const groups = try group_list.toOwnedSlice(ally);
                const filter = switch (logical_op) {
                    .@"or" => Filter{ .@"or" = groups },
                    .@"and" => Filter{ .@"and" = groups },
                    .nor => Filter{ .nor = groups },
                    else => unreachable,
                };
                try filters.append(ally, filter);
                continue;
            }

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
                                const owned_value = PathValuePair.dupeValue(ally, operand) catch return FilterParsingErrors.OutOfMemory;
                                errdefer PathValuePair.freeValue(ally, owned_value);
                                const filter = @unionInit(Filter, @tagName(op), PathValuePair{ .path = path_copy, .value = owned_value });

                                try filters.append(ally, filter);
                            },
                            .between => {
                                if (operand != .array) return FilterParsingErrors.InvalidQueryOperatorParameter;
                                if (operand.array.keyNumber() != 2) return FilterParsingErrors.InvalidQueryOperatorBetweenSize;
                                const path_copy = try ally.dupe(u8, path);
                                errdefer ally.free(path_copy);
                                const owned_value = PathValuePair.dupeValue(ally, operand) catch return FilterParsingErrors.OutOfMemory;
                                errdefer PathValuePair.freeValue(ally, owned_value);
                                try filters.append(ally, .{ .between = .{ .path = path_copy, .value = owned_value } });
                            },
                            .@"or", .@"and", .nor => return FilterParsingErrors.InvalidQueryOperator,
                        }
                        matched = true;
                    }

                    if (!matched) return FilterParsingErrors.InvalidQueryOperator;
                },
                .string, .int32, .int64, .objectId, .datetime, .boolean, .null, .double => {
                    const path_copy = try ally.dupe(u8, path);
                    errdefer ally.free(path_copy);
                    const owned_value = PathValuePair.dupeValue(ally, pair.value) catch return FilterParsingErrors.OutOfMemory;
                    errdefer PathValuePair.freeValue(ally, owned_value);
                    try filters.append(ally, .{ .eq = .{ .path = path_copy, .value = owned_value } });
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
            .@"or", .@"and", .nor => unreachable,
        }
    }

    pub fn match(self: *const Filter, doc: *const bson.BSONDocument) bool {
        switch (self.*) {
            .@"or" => |groups| {
                for (groups) |group| {
                    const group_matches = blk: {
                        for (group) |*f| {
                            if (!f.match(doc)) break :blk false;
                        }
                        break :blk true;
                    };
                    if (group_matches) return true;
                }
                return false;
            },
            .@"and" => |groups| {
                for (groups) |group| {
                    for (group) |*f| {
                        if (!f.match(doc)) return false;
                    }
                }
                return true;
            },
            .nor => |groups| {
                for (groups) |group| {
                    const group_matches = blk: {
                        for (group) |*f| {
                            if (!f.match(doc)) break :blk false;
                        }
                        break :blk true;
                    };
                    if (group_matches) return false;
                }
                return true;
            },
            else => {},
        }
        const valueToMatch = switch (self.*) {
            .notExists => |e| blk: {
                // For notExists, return true if field doesn't exist, false if it exists
                break :blk doc.getPath(e.path) orelse return true;
            },
            inline .eq, .ne, .lt, .lte, .gte, .gt, .in, .between, .startsWith, .endsWith, .exists => |e| blk: {
                break :blk doc.getPath(e.path) orelse return false;
            },
            .@"or", .@"and", .nor => unreachable,
        };
        return matchValue(self, valueToMatch);
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
    try std.testing.expect(filter.matchValue(anyString));

    const emptyString = bson.BSONValue{ .string = bson.BSONString{ .value = "" } };
    try std.testing.expect(filter.matchValue(emptyString));
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
    try std.testing.expect(filter.matchValue(anyString));

    const emptyString = bson.BSONValue{ .string = bson.BSONString{ .value = "" } };
    try std.testing.expect(filter.matchValue(emptyString));
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
    try std.testing.expect(filter.matchValue(stringValue));

    const intValue = bson.BSONValue{ .int32 = .{ .value = 42 } };
    try std.testing.expect(filter.matchValue(intValue));

    const boolValue = bson.BSONValue{ .boolean = .{ .value = false } };
    try std.testing.expect(filter.matchValue(boolValue));
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
    try std.testing.expect(filter.match(&docWithField));

    // Document without the field should not match
    var docWithoutField = bson.BSONDocument.initEmpty();
    docWithoutField = try docWithoutField.set(ally, "name", bson.BSONValue{ .string = bson.BSONString{ .value = "John" } });
    defer docWithoutField.deinit(ally);
    try std.testing.expect(!filter.match(&docWithoutField));
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
    try std.testing.expect(!filter.matchValue(stringValue));

    const intValue = bson.BSONValue{ .int32 = .{ .value = 42 } };
    try std.testing.expect(!filter.matchValue(intValue));

    const boolValue = bson.BSONValue{ .boolean = .{ .value = false } };
    try std.testing.expect(!filter.matchValue(boolValue));
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

    var builder = try bson.Builder.init(ally);
    defer builder.deinit();
    var cursor = try builder.object("cursor");
    try cursor.put("version", @as(i32, 1));
    try cursor.put("mode", "full_scan");
    var anchor = try cursor.object("anchor");
    try anchor.put("docId", doc_id);
    try anchor.put("_id", "user-1");
    try anchor.put("pageId", @as(i64, 4));
    try anchor.put("offset", @as(i32, 16));
    try anchor.end();
    try cursor.end();
    const queryDoc = try builder.finish();
    defer queryDoc.deinit(ally);

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

    var builder = try bson.Builder.init(ally);
    defer builder.deinit();
    var cursor = try builder.object("cursor");
    try cursor.put("version", @as(i32, 1));
    try cursor.put("mode", "index_range");
    try cursor.put("indexPath", "age");
    var anchor = try cursor.object("anchor");
    try anchor.put("docId", doc_id);
    try anchor.put("_id", "user-2");
    try anchor.put("pageId", @as(i64, 8));
    try anchor.put("offset", @as(i32, 24));
    try anchor.end();
    try cursor.end();
    const queryDoc = try builder.finish();
    defer queryDoc.deinit(ally);

    var parsed = try Query.parse(ally, queryDoc);
    defer parsed.deinit(ally);
    try std.testing.expect(parsed.cursor != null);
    try std.testing.expectEqual(CursorMode.index_range, parsed.cursor.?.mode);
    try std.testing.expect(std.mem.eql(u8, parsed.cursor.?.index_path.?, "age"));
}

test "Query.parse cursor rejects invalid version" {
    const ally = std.testing.allocator;
    var builder = try bson.Builder.init(ally);
    defer builder.deinit();
    var cursor = try builder.object("cursor");
    try cursor.put("version", @as(i32, 2));
    try cursor.put("mode", "full_scan");
    try cursor.end();
    const queryDoc = try builder.finish();
    defer queryDoc.deinit(ally);

    try std.testing.expectError(error.InvalidCursorVersion, Query.parse(ally, queryDoc));
}

test "Query.parse cursor rejects missing index path" {
    const ally = std.testing.allocator;
    var builder = try bson.Builder.init(ally);
    defer builder.deinit();
    var cursor = try builder.object("cursor");
    try cursor.put("version", @as(i32, 1));
    try cursor.put("mode", "index_range");
    try cursor.end();
    const queryDoc = try builder.finish();
    defer queryDoc.deinit(ally);

    try std.testing.expectError(error.MissingCursorIndexPath, Query.parse(ally, queryDoc));
}

test "Query.match matches objectId correctly" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    const ally = arena.allocator();
    defer arena.deinit();

    const objId = try bson.ObjectId.init();

    var doc_builder = try bson.Builder.init(ally);
    defer doc_builder.deinit();
    try doc_builder.put("_id", objId);
    const doc = try doc_builder.finish();
    defer doc.deinit(ally);

    const objId2 = try bson.ObjectId.init();
    var doc2_builder = try bson.Builder.init(ally);
    defer doc2_builder.deinit();
    try doc2_builder.put("_id", objId2);
    const doc2 = try doc2_builder.finish();
    defer doc2.deinit(ally);

    var query_builder = try bson.Builder.init(ally);
    defer query_builder.deinit();
    try query_builder.put("query", doc);
    const queryDoc = try query_builder.finish();
    defer queryDoc.deinit(ally);
    // const buffer = try ally.alloc(u8, queryDoc.len);
    // queryDoc.serializeToMemory(buffer);
    var query = try Query.parse(ally, queryDoc);

    try std.testing.expect(query.match(&doc));
    try std.testing.expect(!query.match(&doc2));
    defer query.deinit(ally);
}

test "Filter.parse handles $or at top level" {
    const ally = std.testing.allocator;
    // { "$or": [{"role": "admin"}, {"public": true}] }
    const filterDoc = try bson.fmt.serialize(.{
        .@"$or" = .{ .{ .role = "admin" }, .{ .public = true } },
    }, ally);
    defer filterDoc.deinit(ally);
    const filters = try Filter.parse(ally, bson.BSONValue{ .document = filterDoc });
    defer {
        for (filters) |*f| f.deinit(ally);
        ally.free(filters);
    }
    try std.testing.expectEqual(@as(usize, 1), filters.len);
    switch (filters[0]) {
        .@"or" => |groups| {
            try std.testing.expectEqual(@as(usize, 2), groups.len);
            try std.testing.expectEqual(@as(usize, 1), groups[0].len);
            try std.testing.expectEqual(@as(usize, 1), groups[1].len);
        },
        else => return error.UnexpectedFilterType,
    }
}

test "Filter.match $or returns true when any group matches" {
    const ally = std.testing.allocator;
    const filterDoc = try bson.fmt.serialize(.{
        .@"$or" = .{ .{ .role = "admin" }, .{ .public = true } },
    }, ally);
    defer filterDoc.deinit(ally);
    const filters = try Filter.parse(ally, bson.BSONValue{ .document = filterDoc });
    defer {
        for (filters) |*f| f.deinit(ally);
        ally.free(filters);
    }

    const adminDoc = try bson.fmt.serialize(.{ .role = "admin", .public = false }, ally);
    defer adminDoc.deinit(ally);
    try std.testing.expect(filters[0].match(&adminDoc));

    const publicDoc = try bson.fmt.serialize(.{ .role = "user", .public = true }, ally);
    defer publicDoc.deinit(ally);
    try std.testing.expect(filters[0].match(&publicDoc));

    const neitherDoc = try bson.fmt.serialize(.{ .role = "user", .public = false }, ally);
    defer neitherDoc.deinit(ally);
    try std.testing.expect(!filters[0].match(&neitherDoc));
}

test "Filter.match $nor returns true when no group matches" {
    const ally = std.testing.allocator;
    const filterDoc = try bson.fmt.serialize(.{
        .@"$nor" = .{ .{ .spam = true }, .{ .deleted = true } },
    }, ally);
    defer filterDoc.deinit(ally);
    const filters = try Filter.parse(ally, bson.BSONValue{ .document = filterDoc });
    defer {
        for (filters) |*f| f.deinit(ally);
        ally.free(filters);
    }

    const cleanDoc = try bson.fmt.serialize(.{ .spam = false, .deleted = false }, ally);
    defer cleanDoc.deinit(ally);
    try std.testing.expect(filters[0].match(&cleanDoc));

    const spamDoc = try bson.fmt.serialize(.{ .spam = true, .deleted = false }, ally);
    defer spamDoc.deinit(ally);
    try std.testing.expect(!filters[0].match(&spamDoc));
}

test "Filter.match $and with combined $or and leaf filter" {
    const ally = std.testing.allocator;
    // { "$or": [{"role": "admin"}, {"public": true}], "deleted": false }
    const filterDoc = try bson.fmt.serialize(.{
        .@"$or" = .{ .{ .role = "admin" }, .{ .public = true } },
        .deleted = false,
    }, ally);
    defer filterDoc.deinit(ally);
    const filters = try Filter.parse(ally, bson.BSONValue{ .document = filterDoc });
    defer {
        for (filters) |*f| f.deinit(ally);
        ally.free(filters);
    }
    try std.testing.expectEqual(@as(usize, 2), filters.len);

    // Admin, not deleted: matches both filters
    const doc1 = try bson.fmt.serialize(.{ .role = "admin", .public = false, .deleted = false }, ally);
    defer doc1.deinit(ally);
    var match1 = true;
    for (filters) |*f| {
        if (!f.match(&doc1)) { match1 = false; break; }
    }
    try std.testing.expect(match1);

    // Admin but deleted: fails the leaf filter
    const doc2 = try bson.fmt.serialize(.{ .role = "admin", .public = false, .deleted = true }, ally);
    defer doc2.deinit(ally);
    var match2 = true;
    for (filters) |*f| {
        if (!f.match(&doc2)) { match2 = false; break; }
    }
    try std.testing.expect(!match2);
}
