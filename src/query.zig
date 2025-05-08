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

pub const FilterType = enum {
    eq,
    ne,
    lt,
    lte,
    gt,
    gte,
    in,
    between,
};

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

    pub fn deinit(self: *Filter, ally: Allocator) void {
        switch (self.*) {
            .in => |*inFilter| {
                ally.free(inFilter.path);
            },
            .between => |*betweenFilter| {
                ally.free(betweenFilter.path);
            },
            .eq, .ne, .lt, .gt, .lte, .gte => |*filter| {
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
        var filters = try ally.alloc(Filter, doc.keyNumber());
        var iter = doc.iter();
        var i: usize = 0;
        while (iter.next()) |pair| : (i += 1) {
            const path = pair.key;
            if (path.len == 0) return FilterParsingErrors.InvalidQueryPath;

            switch (pair.value) {
                .document => |*operatorDoc| {
                    if (operatorDoc.get("$eq")) |operand| {
                        filters[i] = Filter{
                            .eq = .{ .path = path, .value = operand },
                        };
                        continue;
                    } else if (operatorDoc.get("$in")) |operand| {
                        filters[i] = Filter{
                            .in = .{ .path = path, .value = operand },
                        };
                        continue;
                    } else if (operatorDoc.get("$ne")) |operand| {
                        filters[i] = Filter{
                            .ne = .{ .path = path, .value = operand },
                        };
                        continue;
                    } else if (operatorDoc.get("$lt")) |operand| {
                        filters[i] = Filter{
                            .lt = .{ .path = path, .value = operand },
                        };
                        continue;
                    } else if (operatorDoc.get("$gt")) |operand| {
                        filters[i] = Filter{
                            .gt = .{ .path = path, .value = operand },
                        };
                        continue;
                    } else if (operatorDoc.get("$between")) |operand| {
                        if (operand != .array) return FilterParsingErrors.InvalidQueryOperatorParameter;
                        if (operand.array.keyNumber() != 2) return FilterParsingErrors.InvalidQueryOperatorBetweenSize;
                        filters[i] = Filter{
                            .between = .{ .path = path, .value = operand },
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
        }
    }

    pub fn match(self: *const Filter, doc: *const bson.BSONDocument) bool {
        const valueToMatch = switch (self.*) {
            inline .eq, .ne, .lt, .lte, .gte, .gt, .in, .between => |e| doc.getPath(e.path) orelse return false,
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

    const objId = bson.ObjectId.init();

    var doc = bson.BSONDocument.initEmpty();
    doc = try doc.set(ally, "_id", bson.BSONValue.init(objId));
    defer doc.deinit(ally);

    const objId2 = bson.ObjectId.init();
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
