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
const Allocator = std.mem.Allocator;

const Path = [][]const u8; // "field.subfield".split('.')

pub const FilterType = enum {
    eq,
    ne,
    lt,
    gt,
    in,
};

pub const PathValuePair = struct {
    path: Path,
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
    gt: PathValuePair,
    in: struct {
        path: Path,
        values: []bson.BSONValue,
    },

    pub fn deinit(self: *Filter, ally: Allocator) void {
        switch (self.*) {
            .in => |*inFilter| {
                ally.free(inFilter.path);
                ally.free(inFilter.values);
            },
            .eq, .ne, .lt, .gt => |*filter| {
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
        OutOfMemory,
    };

    fn parseDoc(ally: Allocator, doc: *const bson.BSONDocument) FilterParsingErrors![]Filter {
        var filters = try ally.alloc(Filter, doc.values.len);
        for (doc.values, 0..) |pair, i| {
            const path = try parsePath(ally, pair.key);
            if (path.len == 0) return FilterParsingErrors.InvalidQueryPath;

            switch (pair.value) {
                .document => |*operatorDoc| {
                    if (operatorDoc.values.len != 1) return FilterParsingErrors.InvalidQueryOperatorSize;
                    const operatorPair = operatorDoc.values[0];
                    if (std.mem.eql(u8, operatorPair.key, "$eq")) {
                        filters[i] = Filter{ .eq = .{ .path = path, .value = operatorPair.value } };
                    } else if (std.mem.eql(u8, operatorPair.key, "$ne")) {
                        filters[i] = Filter{ .ne = .{ .path = path, .value = operatorPair.value } };
                    } else if (std.mem.eql(u8, operatorPair.key, "$lt")) {
                        filters[i] = Filter{ .lt = .{ .path = path, .value = operatorPair.value } };
                    } else if (std.mem.eql(u8, operatorPair.key, "$gt")) {
                        filters[i] = Filter{ .gt = .{ .path = path, .value = operatorPair.value } };
                    } else if (std.mem.eql(u8, operatorPair.key, "$in")) {
                        const inValues = operatorPair.value;
                        if (inValues != bson.BSONValueType.array) {
                            return FilterParsingErrors.InvalidInOperatorValue;
                        }
                        var values: []bson.BSONValue = try ally.alloc(bson.BSONValue, inValues.array.values.len);
                        for (inValues.array.values, 0..) |value, j| {
                            values[j] = value.value;
                        }
                        filters[i] = Filter{
                            .in = .{ .path = path, .values = values },
                        };
                    } else {
                        return FilterParsingErrors.InvalidQueryOperator;
                    }
                },
                .string, .int32, .int64, .objectId, .datetime, .boolean, .null => {
                    filters[i] = Filter{
                        .eq = .{ .path = path, .value = pair.value },
                    };
                },

                else => return FilterParsingErrors.InvalidQueryFilter,
            }
        }
        return filters;
    }

    pub fn parse(ally: Allocator, doc: *const bson.BSONValue) FilterParsingErrors![]Filter {
        switch (doc.*) {
            .document => |*document| {
                return try parseDoc(ally, document);
            },
            else => return FilterParsingErrors.InvalidQueryRoot,
        }
    }

    pub fn match(self: *Filter, doc: *const bson.BSONDocument) bool {
        switch (self.*) {
            .eq => |eqFilter| {
                const pathResult = doc.getPath(eqFilter.path);
                if (pathResult) |value| {
                    return value.eql(eqFilter.value);
                } else {
                    return false;
                }
            },
            .ne => |neFilter| {
                const pathResult = doc.getPath(neFilter.path);
                if (pathResult) |value| {
                    return !value.eql(neFilter.value);
                } else {
                    return false;
                }
            },
            .lt => |ltFilter| {
                if (doc.getPath(ltFilter.path)) |value| {
                    return value.order(ltFilter.value) == .lt;
                } else {
                    return false;
                }
            },
            .gt => |gtFilter| {
                if (doc.getPath(gtFilter.path)) |value| {
                    return value.order(gtFilter.value) == .gt;
                } else {
                    return false;
                }
            },
            .in => |inFilter| {
                if (doc.getPath(inFilter.path)) |value| {
                    for (inFilter.values) |inValue| {
                        if (value.eql(inValue)) return true;
                    }
                    return false;
                } else {
                    return false;
                }
            },
        }
    }
};

test "Filter.parse" {
    const ally = std.testing.allocator;
    var opPairs = [_]bson.BSONKeyValuePair{
        .{ .key = "$eq", .value = .{ .string = bson.BSONString{ .value = "value" } } },
    };
    const opDoc = bson.BSONDocument.fromPairs(ally, &opPairs);
    var filterPairs = [_]bson.BSONKeyValuePair{
        .{ .key = "field.subfield", .value = .{ .document = opDoc } },
    };
    const filterDoc = bson.BSONDocument.fromPairs(ally, &filterPairs);
    const filters = try Filter.parse(ally, &bson.BSONValue{ .document = filterDoc });
    defer {
        for (filters) |*filter| filter.deinit(ally);
        ally.free(filters);
    }
    try std.testing.expectEqual(filters.len, 1);
    const filter = filters[0];
    switch (filter) {
        .eq => |*eqFilter| {
            try std.testing.expectEqualSlices(u8, "field", eqFilter.path[0]);
            try std.testing.expectEqualSlices(u8, "subfield", eqFilter.path[1]);
            try std.testing.expectEqualStrings(eqFilter.value.string.value, "value");
        },
        else => unreachable,
    }
}

const SortType = enum {
    asc,
    desc,
};

const SortConfig = union(SortType) {
    asc: Path,
    desc: Path,

    pub const SortParsingErrors = error{
        InvalidQuerySort,
        InvalidQuerySortParamLength,
        InvalidQuerySortParamType,
        InvalidQueryPath,
        OutOfMemory,
    };

    pub fn parse(ally: Allocator, doc: bson.BSONValue) SortParsingErrors!SortConfig {
        if (doc != bson.BSONValueType.document) return error.InvalidQuerySort;
        if (doc.document.values.len != 1) return error.InvalidQuerySortParamLength;
        const pair = doc.document.values[0];
        if (pair.value != bson.BSONValueType.string) return error.InvalidQuerySortParamType;
        const path = try parsePath(ally, pair.value.string.value);
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
    var sortPairs = [_]bson.BSONKeyValuePair{
        .{ .key = "asc", .value = .{ .string = bson.BSONString{ .value = "field" } } },
    };
    const sortDoc = bson.BSONDocument.fromPairs(ally, &sortPairs);
    const sort = try SortConfig.parse(ally, bson.BSONValue{ .document = sortDoc });
    defer {
        switch (sort) {
            .asc => |ascSort| ally.free(ascSort),
            .desc => |descSort| ally.free(descSort),
        }
    }
    switch (sort) {
        .asc => |ascSort| {
            try std.testing.expectEqualSlices(u8, "field", ascSort[0]);
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
            else => return SectorParsingErrors.InvalidQueryOffset,
        };
        return Sector{
            .offset = offset,
            .limit = limit,
        };
    }
};

test "Sector.parse" {
    const ally = std.testing.allocator;
    var sectorPairs = [_]bson.BSONKeyValuePair{
        .{ .key = "offset", .value = .{ .int32 = .{ .value = 10 } } },
        .{ .key = "limit", .value = .{ .int32 = .{ .value = 10 } } },
    };
    const sectorDoc = bson.BSONDocument.fromPairs(ally, &sectorPairs);
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
        var queryDoc = try bson.BSONDocument.deserializeFromMemory(ally, rawQuery);
        defer queryDoc.deinit();

        return parse(ally, queryDoc);
    }

    pub const QueryParsingErrors = (SortConfig.SortParsingErrors ||
        Filter.FilterParsingErrors ||
        Sector.SectorParsingErrors);

    pub fn parse(ally: Allocator, queryDoc: bson.BSONDocument) QueryParsingErrors!Query {
        const filterDoc = queryDoc.get("query");
        const filters = if (filterDoc) |*doc| try Filter.parse(ally, doc) else @constCast(defaultFilters[0..]);
        const sortDoc = queryDoc.get("sort");
        const sortConfig = if (sortDoc) |doc| try SortConfig.parse(ally, doc) else null;
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

    pub fn match(self: *const Query, doc: *const bson.BSONDocument) bool {
        for (self.filters) |*filter| {
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

    const objId = bson.ObjectId.init();

    var doc = bson.BSONDocument.init(ally);
    try doc.set("_id", bson.BSONValue.init(objId));

    const objId2 = bson.ObjectId.init();
    var doc2 = bson.BSONDocument.init(ally);
    try doc.set("_id", bson.BSONValue.init(objId2));

    var queryDoc = bson.BSONDocument.init(ally);
    try queryDoc.set("query", bson.BSONValue.init(doc));

    // const buffer = try ally.alloc(u8, queryDoc.len);
    // queryDoc.serializeToMemory(buffer);
    var query = try Query.parse(ally, queryDoc);

    try std.testing.expect(query.match(&doc));
    try std.testing.expect(!query.match(&doc2));
    defer query.deinit(ally);
}
