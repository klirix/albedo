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

const Path = []const [:0]const u8; // "field.subfield".split('.')

const FilterType = enum {
    eq,
    ne,
    lt,
    gt,
    in,
};

const PathValuePair = struct {
    path: Path,
    value: bson.BSONValue,
};

const Filter = union(FilterType) {
    eq: PathValuePair,
    ne: PathValuePair,
    lt: PathValuePair,
    gt: PathValuePair,
    in: struct {
        path: Path,
        values: []bson.BSONValue,
    },

    pub fn parseDoc(*const bson.BSONDocument) ![]Filter {
        var filters: []Filter = &[]Filter{};
        for (doc) |pair| {
            const path = pair.key;
            const value = pair.value;
            if (value.isBSONDocument()) |doc| {
                for (doc) |filterPair| {
                    const filterType = filterPair.key;
                    const filterValue = filterPair.value;
                    switch (filterType) {
                        FilterType.eq => filters.append(Filter.eq(PathValuePair{ .path = path, .value = filterValue })),
                        FilterType.ne => filters.append(Filter.ne(PathValuePair{ .path = path, .value = filterValue })),
                        FilterType.lt => filters.append(Filter.lt(PathValuePair{ .path = path, .value = filterValue })),
                        FilterType.gt => filters.append(Filter.gt(PathValuePair{ .path = path, .value = filterValue })),
                        FilterType.in => filters.append(Filter.in(PathValuePair{ .path = path, .values = filterValue })),
                    }
                }
            }
        }
        return filters;
    }
};

const SortType = enum {
    Asc,
    Desc,
};

const Sort = union(SortType) {
    Asc: Path,
    Desc: Path,
};

const Sector = struct {
    offset: ?u64,
    limit: ?u64,
};

pub const Query = struct {
    filters: []Filter = &[]Filter{},
    sorts: ?Sort,
    sector: ?Sector,
    projection: bson.Document,

    pub fn parse(ally: Allocator, rawQuery: []const u8) !void {
        const queryDoc = try bson.BSONDocument.deserializeFromMemory(ally, rawQuery);
        defer queryDoc.deinit();

        const filterDoc = queryDoc.get("query");
        if (filterDoc) |doc| {}
        self.sorts = doc.get("sort") orelse return error.InvalidQuery;
        self.sector = doc.get("sector") orelse return error.InvalidQuery;
        self.projection = doc.get("projection") orelse return error.InvalidQuery;
    }
};
