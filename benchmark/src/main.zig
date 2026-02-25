const std = @import("std");
const albedo = @import("albedo");
const sqlite = @import("sqlite");
const bson = albedo.bson;
const Bucket = albedo.Bucket;
const Query = albedo.Query;

// ─── Configuration ───────────────────────────────────────────────────────────

const NUM_RECORDS: usize = 100000;
const SEARCH_ITERATIONS: usize = 1000;
const SEARCH_TARGET_NAME = std.fmt.comptimePrint("record_{}", .{NUM_RECORDS - 2});
const SEARCH_TARGET_AGE: i32 = 42;

// ─── Output Helper ───────────────────────────────────────────────────────────

fn write(bytes: []const u8) void {
    _ = std.posix.write(std.posix.STDOUT_FILENO, bytes) catch {};
}

fn print(comptime fmt: []const u8, args: anytype) void {
    var buf: [4096]u8 = undefined;
    const s = std.fmt.bufPrint(&buf, fmt, args) catch return;
    write(s);
}

// ─── Pretty Printing Helpers ─────────────────────────────────────────────────

const Color = struct {
    const reset = "\x1b[0m";
    const bold = "\x1b[1m";
    const dim = "\x1b[2m";
    const cyan = "\x1b[36m";
    const green = "\x1b[32m";
    const yellow = "\x1b[33m";
    const magenta = "\x1b[35m";
    const white = "\x1b[37m";
    const blue = "\x1b[34m";
    const red = "\x1b[31m";
};

fn printHeader() void {
    print("\n", .{});
    print("{s}{s}  ╔══════════════════════════════════════════════════════════╗{s}\n", .{ Color.bold, Color.cyan, Color.reset });
    print("{s}{s}  ║          ◆  ALBEDO  vs  SQLITE  BENCHMARK  ◆           ║{s}\n", .{ Color.bold, Color.cyan, Color.reset });
    print("{s}{s}  ╚══════════════════════════════════════════════════════════╝{s}\n", .{ Color.bold, Color.cyan, Color.reset });
    print("{s}  Records: {d}  |  Search iterations: {d}{s}\n\n", .{ Color.dim, NUM_RECORDS, SEARCH_ITERATIONS, Color.reset });
}

fn printSectionHeader(title: []const u8) void {
    print("  {s}{s}▐ {s}{s}\n", .{ Color.bold, Color.yellow, title, Color.reset });
    print("  {s}├──────────────────────────────────────────────────────────{s}\n", .{ Color.dim, Color.reset });
}

fn printResult(label: []const u8, nanos: u64) void {
    const ms = @as(f64, @floatFromInt(nanos)) / 1_000_000.0;
    const color = if (ms < 10.0) Color.green else if (ms < 100.0) Color.yellow else Color.red;
    print("  {s}│{s}  {s:<20} {s}{s}{d:>10.3} ms{s}\n", .{ Color.dim, Color.reset, label, Color.bold, color, ms, Color.reset });
}

fn printOpsPerSec(label: []const u8, nanos: u64, ops: usize) void {
    const secs = @as(f64, @floatFromInt(nanos)) / 1_000_000_000.0;
    const ops_sec = @as(f64, @floatFromInt(ops)) / secs;
    const avg_us = @as(f64, @floatFromInt(nanos)) / @as(f64, @floatFromInt(ops)) / 1_000.0;
    print("  {s}│{s}  {s:<20} {s}{d:>10.0} ops/s  {s}{d:>8.2} µs/op{s}\n", .{ Color.dim, Color.reset, label, Color.magenta, ops_sec, Color.dim, avg_us, Color.reset });
}

fn printWinner(albedo_ns: u64, sqlite_ns: u64) void {
    if (albedo_ns < sqlite_ns) {
        const speedup = @as(f64, @floatFromInt(sqlite_ns)) / @as(f64, @floatFromInt(albedo_ns));
        print("  {s}│{s}  {s}{s}  ★ Albedo wins — {d:.1}x faster{s}\n", .{ Color.dim, Color.reset, Color.bold, Color.green, speedup, Color.reset });
    } else if (sqlite_ns < albedo_ns) {
        const speedup = @as(f64, @floatFromInt(albedo_ns)) / @as(f64, @floatFromInt(sqlite_ns));
        print("  {s}│{s}  {s}{s}  ★ SQLite wins — {d:.1}x faster{s}\n", .{ Color.dim, Color.reset, Color.bold, Color.blue, speedup, Color.reset });
    } else {
        print("  {s}│{s}  {s}  ≈ Tie{s}\n", .{ Color.dim, Color.reset, Color.yellow, Color.reset });
    }
    print("  {s}│{s}\n", .{ Color.dim, Color.reset });
}

fn printBar(albedo_ns: u64, sqlite_ns: u64) void {
    const max_ns = @max(albedo_ns, sqlite_ns);
    const bar_width: usize = 40;

    const a_width: usize = if (max_ns > 0) @intFromFloat(@as(f64, @floatFromInt(albedo_ns)) / @as(f64, @floatFromInt(max_ns)) * @as(f64, @floatFromInt(bar_width))) else 0;
    const s_width: usize = if (max_ns > 0) @intFromFloat(@as(f64, @floatFromInt(sqlite_ns)) / @as(f64, @floatFromInt(max_ns)) * @as(f64, @floatFromInt(bar_width))) else 0;

    print("  {s}│{s}  {s}Albedo{s} ", .{ Color.dim, Color.reset, Color.cyan, Color.reset });
    for (0..a_width) |_| write("\x1b[36m█\x1b[0m");
    write("\n");

    print("  {s}│{s}  {s}SQLite{s} ", .{ Color.dim, Color.reset, Color.blue, Color.reset });
    for (0..s_width) |_| write("\x1b[34m█\x1b[0m");
    write("\n");
}

// ─── Benchmark Functions ─────────────────────────────────────────────────────

fn benchAlbedoInsert(allocator: std.mem.Allocator, bucket: *Bucket) !u64 {
    var timer = try std.time.Timer.start();

    for (0..NUM_RECORDS) |i| {
        const age: i32 = @intCast(i);
        var name_buf: [64]u8 = undefined;
        const name = std.fmt.bufPrint(&name_buf, "record_{d}", .{i}) catch unreachable;

        const doc = try bson.fmt.serialize(.{
            ._id = try bson.ObjectId.init(),
            .name = @as([]const u8, name),
            .age = age,
            .email = @as([]const u8, "user@example.com"),
            .active = true,
        }, allocator);

        _ = try bucket.insert(doc);
    }

    return timer.read();
}

fn benchSqliteInsert(db: *sqlite.Db) !u64 {
    var timer = try std.time.Timer.start();

    // try db.exec("BEGIN TRANSACTION", .{}, .{});

    var stmt = try db.prepare("INSERT INTO docs (name, age, email, active) VALUES (?, ?, ?, ?)");
    defer stmt.deinit();

    for (0..NUM_RECORDS) |i| {
        const age: i32 = @intCast(i);
        var name_buf: [64]u8 = undefined;
        const name = std.fmt.bufPrint(&name_buf, "record_{d}", .{i}) catch unreachable;

        stmt.exec(.{}, .{
            @as([]const u8, name),
            age,
            @as([]const u8, "user@example.com"),
            @as(i32, 1),
        }) catch |err| {
            std.debug.print("SQLite insert error: {}\n", .{err});
            return err;
        };

        stmt.reset();
    }

    // try db.exec("COMMIT", .{}, .{});

    return timer.read();
}

fn benchAlbedoScan(allocator: std.mem.Allocator, bucket: *Bucket) !u64 {
    var timer = try std.time.Timer.start();

    for (0..SEARCH_ITERATIONS) |_| {
        var arena = std.heap.ArenaAllocator.init(allocator);
        defer arena.deinit();

        var qdoc = try bson.fmt.serialize(.{
            .query = .{ .name = @as([]const u8, SEARCH_TARGET_NAME) },
        }, arena.allocator());
        defer qdoc.deinit(arena.allocator());

        var q = try Query.parse(arena.allocator(), qdoc);
        defer q.deinit(arena.allocator());

        var iter = try bucket.listIterate(&arena, q);
        defer iter.deinit() catch {};

        var count: usize = 0;
        while (try iter.next(iter)) |_| {
            count += 1;
        }

        if (count == 0) {
            std.debug.print("  {s}│  ⚠ Albedo scan found 0 results{s}\n", .{ Color.dim, Color.reset });
        }
    }

    return timer.read();
}

fn benchSqliteScan(db: *sqlite.Db) !u64 {
    var timer = try std.time.Timer.start();

    var stmt = try db.prepare("SELECT id, age, active FROM docs WHERE name = ?");
    defer stmt.deinit();

    for (0..SEARCH_ITERATIONS) |_| {
        const Row = struct { id: i64, age: i32, active: i32 };
        var iter = try stmt.iterator(Row, .{@as([]const u8, SEARCH_TARGET_NAME)});

        var count: usize = 0;
        while (try iter.next(.{})) |_| {
            count += 1;
        }

        if (count == 0) {
            std.debug.print("  {s}│  ⚠ SQLite scan found 0 results{s}\n", .{ Color.dim, Color.reset });
        }

        stmt.reset();
    }

    return timer.read();
}

fn benchAlbedoIndexSearch(allocator: std.mem.Allocator, bucket: *Bucket) !u64 {
    var timer = try std.time.Timer.start();

    for (0..SEARCH_ITERATIONS) |_| {
        var arena = std.heap.ArenaAllocator.init(allocator);
        defer arena.deinit();

        const qdoc = try bson.fmt.serialize(.{
            .query = .{ .age = SEARCH_TARGET_AGE },
        }, arena.allocator());

        const q = try Query.parse(arena.allocator(), qdoc);

        const iter = try bucket.listIterate(&arena, q);
        defer iter.deinit() catch {};

        var count: usize = 0;
        while (try iter.next(iter)) |_| {
            count += 1;
        }

        if (count != 1) {
            std.debug.print("  {s}│  ⚠ Albedo index search found 0 results{s}\n", .{ Color.dim, Color.reset });
        }
    }

    return timer.read();
}

fn benchSqliteIndexSearch(db: *sqlite.Db) !u64 {
    var timer = try std.time.Timer.start();

    var stmt = try db.prepare("SELECT id, age, active FROM docs WHERE age = ?");
    defer stmt.deinit();

    for (0..SEARCH_ITERATIONS) |_| {
        const Row = struct { id: i64, age: i32, active: i32 };
        var iter = try stmt.iterator(Row, .{SEARCH_TARGET_AGE});

        var count: usize = 0;
        while (try iter.next(.{})) |_| {
            count += 1;
        }

        if (count == 0) {
            std.debug.print("  {s}│  ⚠ SQLite index search found 0 results{s}\n", .{ Color.dim, Color.reset });
        }

        stmt.reset();
    }

    return timer.read();
}

fn benchAlbedoReadAll(allocator: std.mem.Allocator, bucket: *Bucket) !u64 {
    var timer = try std.time.Timer.start();

    for (0..SEARCH_ITERATIONS) |_| {
        var arena = std.heap.ArenaAllocator.init(allocator);
        defer arena.deinit();

        var qdoc = try bson.fmt.serialize(.{
            .query = .{},
            .sector = .{
                .limit = 1000, // Large limit to read all records
            },
        }, arena.allocator());
        _ = &qdoc;

        const q = try Query.parse(arena.allocator(), qdoc);

        var iter = try bucket.listIterate(&arena, q);
        defer iter.deinit() catch {};

        var count: usize = 0;
        while (try iter.next(iter)) |doc| {
            const Row = struct { name: []const u8, age: i32, email: []const u8, active: bool };
            _ = try bson.fmt.parse(Row, doc, allocator);
            count += 1;
        }

        if (count != NUM_RECORDS) {
            std.debug.print("  {s}│  ⚠ Albedo read all: expected {d}, got {d}{s}\n", .{ Color.dim, NUM_RECORDS, count, Color.reset });
        }
    }

    return timer.read();
}

fn benchSqliteReadAll(db: *sqlite.Db) !u64 {
    var timer = try std.time.Timer.start();

    var stmt = try db.prepare("SELECT id, age, active FROM docs LIMIT 1000"); // Large limit to read all records
    defer stmt.deinit();

    for (0..SEARCH_ITERATIONS) |_| {
        const Row = struct { id: i64, age: i32, active: i32 };
        var iter = try stmt.iterator(Row, .{});

        var count: usize = 0;
        while (try iter.next(.{})) |_| {
            count += 1;
        }

        if (count != NUM_RECORDS) {
            std.debug.print("  {s}│  ⚠ SQLite read all: expected {d}, got {d}{s}\n", .{ Color.dim, NUM_RECORDS, count, Color.reset });
        }

        stmt.reset();
    }

    return timer.read();
}

// ─── Main ────────────────────────────────────────────────────────────────────

pub fn main() !void {
    const allocator = std.heap.smp_allocator;

    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();

    printHeader();

    // ── Setup Albedo ─────────────────────────────────────────────────────
    var bucket = try Bucket.init(arena.allocator(), "file.bucket");
    defer bucket.deinit();

    // ── Setup SQLite ─────────────────────────────────────────────────────
    var db = try sqlite.Db.init(.{
        .mode = sqlite.Db.Mode{ .File = "file.sqlite" },
        .open_flags = .{
            .write = true,
            .create = true,
        },
        .threading_mode = .Serialized,
    });
    defer db.deinit();

    // No WAL — use rollback journal like Albedo's direct-write model.
    // synchronous=NORMAL: fsync on commit but not on journal write.
    _ = try db.pragma(void, .{}, "journal_mode", "DELETE");
    _ = try db.pragma(void, .{}, "synchronous", "NORMAL");
    try db.exec(
        "CREATE TABLE IF NOT EXISTS docs (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT, age INTEGER, email TEXT, active INTEGER)",
        .{},
        .{},
    );

    // ══════════════════════════════════════════════════════════════════════
    // 1. INSERTION BENCHMARK
    // ══════════════════════════════════════════════════════════════════════
    printSectionHeader("1 ▸ x100k Insertion Speed");

    const albedo_insert_ns = try benchAlbedoInsert(arena.allocator(), &bucket);
    printResult("Albedo", albedo_insert_ns);
    printOpsPerSec("", albedo_insert_ns, NUM_RECORDS);

    const sqlite_insert_ns = try benchSqliteInsert(&db);
    printResult("SQLite", sqlite_insert_ns);
    printOpsPerSec("", sqlite_insert_ns, NUM_RECORDS);

    printBar(albedo_insert_ns, sqlite_insert_ns);
    printWinner(albedo_insert_ns, sqlite_insert_ns);

    // ══════════════════════════════════════════════════════════════════════
    // 2. FULL SCAN (no index on name)
    // ══════════════════════════════════════════════════════════════════════
    printSectionHeader("2 ▸ x1000 Column Scan (no index)");

    const albedo_scan_ns = try benchAlbedoScan(arena.allocator(), &bucket);
    printResult("Albedo", albedo_scan_ns);
    printOpsPerSec("", albedo_scan_ns, SEARCH_ITERATIONS);

    const sqlite_scan_ns = try benchSqliteScan(&db);
    printResult("SQLite", sqlite_scan_ns);
    printOpsPerSec("", sqlite_scan_ns, SEARCH_ITERATIONS);

    printBar(albedo_scan_ns, sqlite_scan_ns);
    printWinner(albedo_scan_ns, sqlite_scan_ns);

    // ══════════════════════════════════════════════════════════════════════
    // 3. READ ALL
    // ══════════════════════════════════════════════════════════════════════
    printSectionHeader("3 ▸ Read 1000x Documents");

    const albedo_read_ns = try benchAlbedoReadAll(arena.allocator(), &bucket);
    printResult("Albedo", albedo_read_ns);
    printOpsPerSec("", albedo_read_ns, SEARCH_ITERATIONS);

    const sqlite_read_ns = try benchSqliteReadAll(&db);
    printResult("SQLite", sqlite_read_ns);
    printOpsPerSec("", sqlite_read_ns, SEARCH_ITERATIONS);

    printBar(albedo_read_ns, sqlite_read_ns);
    printWinner(albedo_read_ns, sqlite_read_ns);

    // ══════════════════════════════════════════════════════════════════════
    // 4. INDEX-BASED SEARCH
    // ══════════════════════════════════════════════════════════════════════
    printSectionHeader("4 ▸ 1000x Indexed Search");

    // std.log.debug("Data: {x}", .{bucket.pageCache.get(2).?.data[4358 + 32 .. 4362 + 32]});
    // Create indexes on "age" for both
    try bucket.ensureIndex("age", .{});

    // std.log.debug("Data: {x}", .{bucket.pageCache.get(2).?.data[4358 + 32 .. 4362 + 32]});
    try db.exec("CREATE INDEX IF NOT EXISTS idx_age ON docs(age)", .{}, .{});

    const albedo_idx_ns = try benchAlbedoIndexSearch(arena.allocator(), &bucket);
    printResult("Albedo", albedo_idx_ns);
    printOpsPerSec("", albedo_idx_ns, SEARCH_ITERATIONS);

    const sqlite_idx_ns = try benchSqliteIndexSearch(&db);
    printResult("SQLite", sqlite_idx_ns);
    printOpsPerSec("", sqlite_idx_ns, SEARCH_ITERATIONS);

    printBar(albedo_idx_ns, sqlite_idx_ns);
    printWinner(albedo_idx_ns, sqlite_idx_ns);

    // ══════════════════════════════════════════════════════════════════════
    // SUMMARY
    // ══════════════════════════════════════════════════════════════════════
    print("  {s}{s}╔══════════════════════════════════════════════════════════╗{s}\n", .{ Color.bold, Color.cyan, Color.reset });
    print("  {s}{s}║                      SUMMARY                            ║{s}\n", .{ Color.bold, Color.cyan, Color.reset });
    print("  {s}{s}╠══════════════════════════════════════════════════════════╣{s}\n", .{ Color.bold, Color.cyan, Color.reset });
    print("  {s}{s}║{s}  {s:<24} {s}{s:>10}   {s}{s:>10}    {s}{s}║{s}\n", .{
        Color.bold,
        Color.cyan,
        Color.reset,
        "Test",
        Color.cyan,
        "Albedo",
        Color.blue,
        "SQLite",
        Color.bold,
        Color.cyan,
        Color.reset,
    });
    print("  {s}{s}╠══════════════════════════════════════════════════════════╣{s}\n", .{ Color.bold, Color.cyan, Color.reset });

    const tests = [_]struct { name: []const u8, albedo_ns: u64, sqlite_ns: u64 }{
        .{ .name = "Insert", .albedo_ns = albedo_insert_ns, .sqlite_ns = sqlite_insert_ns },
        .{ .name = "Column Scan", .albedo_ns = albedo_scan_ns, .sqlite_ns = sqlite_scan_ns },
        .{ .name = "Read All", .albedo_ns = albedo_read_ns, .sqlite_ns = sqlite_read_ns },
        .{ .name = "Index Search", .albedo_ns = albedo_idx_ns, .sqlite_ns = sqlite_idx_ns },
    };

    for (tests) |t| {
        const a_ms = @as(f64, @floatFromInt(t.albedo_ns)) / 1_000_000.0;
        const s_ms = @as(f64, @floatFromInt(t.sqlite_ns)) / 1_000_000.0;
        const marker_a: []const u8 = if (t.albedo_ns <= t.sqlite_ns) " ★" else "  ";
        const marker_s: []const u8 = if (t.sqlite_ns <= t.albedo_ns) " ★" else "  ";

        print("  {s}{s}║{s}  {s:<24}{s}{d:>8.2} ms{s}{s}{d:>8.2} ms{s}  {s}{s}║{s}\n", .{
            Color.bold,
            Color.cyan,
            Color.reset,
            t.name,
            if (t.albedo_ns <= t.sqlite_ns) Color.green else Color.dim,
            a_ms,
            marker_a,
            if (t.sqlite_ns <= t.albedo_ns) Color.green else Color.dim,
            s_ms,
            marker_s,
            Color.bold,
            Color.cyan,
            Color.reset,
        });
    }

    print("  {s}{s}╚══════════════════════════════════════════════════════════╝{s}\n\n", .{ Color.bold, Color.cyan, Color.reset });
}
