const std = @import("std");
const albedo = @import("albedo");
const sqlite = @import("sqlite");
const bson = albedo.bson;
const Bucket = albedo.Bucket;
const Query = albedo.Query;
const UpdateProgram = albedo.UpdateProgram;

// ─── Configuration ───────────────────────────────────────────────────────────

const NUM_RECORDS: usize = 100_000;
const SEARCH_ITERATIONS: usize = 1000;
const SEARCH_TARGET_NAME = std.fmt.comptimePrint("record_{}", .{NUM_RECORDS - 2});
const SEARCH_TARGET_AGE: i32 = 42;
const UPDATE_TARGET_AGE: i32 = 42;
const UPDATE_TARGET_NAME = std.fmt.comptimePrint("record_{d:0>6}", .{UPDATE_TARGET_AGE});
const BATCH_UPDATE_SIZE: i32 = 100; // docs updated per iteration

// ─── Output Helper ───────────────────────────────────────────────────────────

fn write(bytes: []const u8) void {
    _ = std.posix.write(std.posix.STDOUT_FILENO, bytes) catch {};
}

fn print(comptime fmt: []const u8, args: anytype) void {
    var buf: [4096]u8 = undefined;
    const s = std.fmt.bufPrint(&buf, fmt, args) catch return;
    write(s);
}

// ─── Colors ──────────────────────────────────────────────────────────────────

const C = struct {
    const reset = "\x1b[0m";
    const bold = "\x1b[1m";
    const dim = "\x1b[2m";
    const cyan = "\x1b[36m";
    const green = "\x1b[32m";
    const yellow = "\x1b[33m";
    const magenta = "\x1b[35m";
    const blue = "\x1b[34m";
    const red = "\x1b[31m";
};

// ─── Statistics ──────────────────────────────────────────────────────────────

const Stats = struct {
    runs: usize,
    avg_ns: f64,
    stddev_ns: f64,
    min_ns: u64,
    max_ns: u64,
    p75_ns: u64,
    p99_ns: u64,
    p995_ns: u64,
};

fn computeStats(samples: []u64) Stats {
    const n = samples.len;
    std.mem.sort(u64, samples, {}, std.sort.asc(u64));

    var sum: f64 = 0;
    for (samples) |s| sum += @floatFromInt(s);
    const avg = sum / @as(f64, @floatFromInt(n));

    var var_sum: f64 = 0;
    for (samples) |s| {
        const d = @as(f64, @floatFromInt(s)) - avg;
        var_sum += d * d;
    }
    const stddev = @sqrt(var_sum / @as(f64, @floatFromInt(n)));

    return .{
        .runs = n,
        .avg_ns = avg,
        .stddev_ns = stddev,
        .min_ns = samples[0],
        .max_ns = samples[n - 1],
        .p75_ns = samples[@min(n - 1, (n * 75) / 100)],
        .p99_ns = samples[@min(n - 1, (n * 99) / 100)],
        .p995_ns = samples[@min(n - 1, (n * 995) / 1000)],
    };
}

fn fmtTime(ns: u64) [10]u8 {
    return fmtTimeF(@floatFromInt(ns));
}

fn fmtTimeF(ns: f64) [10]u8 {
    var buf: [10]u8 = .{' '} ** 10;
    if (ns < 1_000.0) {
        _ = std.fmt.bufPrint(&buf, "{d:>6.1} ns", .{ns}) catch {};
    } else if (ns < 1_000_000.0) {
        _ = std.fmt.bufPrint(&buf, "{d:>6.2} us", .{ns / 1_000.0}) catch {};
    } else if (ns < 1_000_000_000.0) {
        _ = std.fmt.bufPrint(&buf, "{d:>6.2} ms", .{ns / 1_000_000.0}) catch {};
    } else {
        _ = std.fmt.bufPrint(&buf, "{d:>7.3} s", .{ns / 1_000_000_000.0}) catch {};
    }
    return buf;
}

// ─── Table Output ────────────────────────────────────────────────────────────

fn printTableHeader() void {
    print("\n  {s}{s}{s:<22} {s:>5}  {s:>23}  {s:>23}  {s:>10} {s:>10} {s:>10}{s}\n", .{
        C.bold,             C.cyan,
        "benchmark",        "runs",
        "time (avg ± σ)", "(min … max)",
        "p75",              "p99",
        "p995",             C.reset,
    });
    write("  ");
    for (0..112) |_| write("\xe2\x94\x80"); // ─
    write("\n");
}

fn printRow(name: []const u8, s: Stats, color: []const u8) void {
    const avg = fmtTimeF(s.avg_ns);
    const dev = fmtTimeF(s.stddev_ns);
    const lo = fmtTime(s.min_ns);
    const hi = fmtTime(s.max_ns);
    const p75 = fmtTime(s.p75_ns);
    const p99 = fmtTime(s.p99_ns);
    const p995 = fmtTime(s.p995_ns);

    print("  {s}{s:<22}{s}", .{ color, name, C.reset });
    print(" {s}{d:>5}{s}", .{ C.dim, s.runs, C.reset });
    print("  {s}{s}{s} ± {s}{s}{s}", .{ C.bold, &avg, C.reset, C.dim, &dev, C.reset });
    print("  {s}({s} … {s}){s}", .{ C.dim, &lo, &hi, C.reset });
    print("  {s}", .{&p75});
    print(" {s}", .{&p99});
    print(" {s}", .{&p995});
    write("\n");
}

fn printWinner(a: Stats, b: Stats) void {
    if (a.avg_ns < b.avg_ns) {
        const speedup = b.avg_ns / a.avg_ns;
        print("  {s}{s}★ Albedo is {d:.2}x faster{s}\n\n", .{ C.bold, C.green, speedup, C.reset });
    } else if (b.avg_ns < a.avg_ns) {
        const speedup = a.avg_ns / b.avg_ns;
        print("  {s}{s}★ SQLite is {d:.2}x faster{s}\n\n", .{ C.bold, C.blue, speedup, C.reset });
    } else {
        print("  {s}≈ Tie{s}\n\n", .{ C.yellow, C.reset });
    }
}

// ─── Benchmark Functions ─────────────────────────────────────────────────────

/// Insert benchmark: single bulk run of NUM_RECORDS inserts, returns total ns.
fn benchAlbedoInsert(allocator: std.mem.Allocator, bucket: *Bucket) !u64 {
    var timer = try std.time.Timer.start();

    const doc = try bson.fmt.serialize(.{
        .name = "record_000000",
        .age = @as(i32, 0),
        ._id = @as(i32, 0),
        .email = "user@example.com",
        .active = true,
    }, allocator);

    var buffer: []u8 = @constCast(doc.buffer);

    for (0..NUM_RECORDS) |i| {
        const age: i32 = @intCast(i);
        const name_buf = (buffer)[4 + 1 + 5 + 4 ..];
        _ = std.fmt.bufPrint(name_buf, "record_{d:0>6}", .{i}) catch unreachable;
        std.mem.writeInt(i32, buffer[33 .. 33 + 4], age, .little);
        std.mem.writeInt(i32, buffer[42 .. 42 + 4], age, .little);
        _ = try bucket.insert(doc);
    }
    return timer.read();
}

fn benchSqliteInsert(db: *sqlite.Db) !u64 {
    var timer = try std.time.Timer.start();

    var stmt = try db.prepare("INSERT INTO docs (id, name, age, email, active) VALUES (?, ?, ?, ?, ?)");
    defer stmt.deinit();

    for (0..NUM_RECORDS) |i| {
        const age: i32 = @intCast(i);
        var name_buf: [64]u8 = undefined;
        const name = std.fmt.bufPrint(&name_buf, "record_{d}", .{i}) catch unreachable;

        stmt.exec(.{}, .{
            i,
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

    return timer.read();
}

/// Per-iteration benchmarks fill samples[] with per-iteration nanoseconds.
fn benchAlbedoScan(allocator: std.mem.Allocator, bucket: *Bucket, samples: []u64) !void {
    for (samples, 0..) |*sample, i| {
        if (i >= SEARCH_ITERATIONS) break;
        if (@mod(1, 10) == 0) {
            print("  {s}Albedo scan iteration {d}/{d}{s}\n", .{ C.dim, i + 1, SEARCH_ITERATIONS, C.reset });
        }
        var timer = try std.time.Timer.start();

        var arena_alloc = std.heap.ArenaAllocator.init(allocator);
        defer arena_alloc.deinit();

        var qdoc = try bson.fmt.serialize(.{
            .query = .{ .name = @as([]const u8, SEARCH_TARGET_NAME) },
        }, arena_alloc.allocator());
        defer qdoc.deinit(arena_alloc.allocator());

        var q = try Query.parse(arena_alloc.allocator(), qdoc);
        defer q.deinit(arena_alloc.allocator());

        var iter = try bucket.listIterate(&arena_alloc, q);
        defer iter.deinit() catch {};

        while ((try iter.next(iter))) |_| {}

        sample.* = timer.read();
    }
}

fn benchSqliteScan(db: *sqlite.Db, samples: []u64) !void {
    var stmt = try db.prepare("SELECT id, age, active FROM docs WHERE name = ?");
    defer stmt.deinit();

    for (samples) |*sample| {
        var timer = try std.time.Timer.start();

        const Row = struct { id: i64, age: i32, active: i32 };
        var iter = try stmt.iterator(Row, .{@as([]const u8, SEARCH_TARGET_NAME)});
        while (try iter.next(.{})) |_| {}

        sample.* = timer.read();
        stmt.reset();
    }
}

fn benchAlbedoIndexSearch(allocator: std.mem.Allocator, bucket: *Bucket, samples: []u64) !void {
    var arena_alloc = std.heap.ArenaAllocator.init(allocator);
    defer arena_alloc.deinit();
    const qdoc = try bson.fmt.serialize(.{
        .query = .{ .age = SEARCH_TARGET_AGE },
    }, arena_alloc.allocator());

    const q = try Query.parse(arena_alloc.allocator(), qdoc);
    for (samples) |*sample| {
        var timer = try std.time.Timer.start();

        const iter = try bucket.listIterate(&arena_alloc, q);
        defer iter.deinit() catch {};

        while (try iter.next(iter)) |_| {}

        sample.* = timer.read();
    }
}

fn benchSqliteIndexSearch(db: *sqlite.Db, samples: []u64) !void {
    var stmt = try db.prepare("SELECT id, age, active FROM docs WHERE age = ?");
    defer stmt.deinit();

    for (samples) |*sample| {
        var timer = try std.time.Timer.start();

        const Row = struct { id: i64, age: i32, active: i32 };
        var iter = try stmt.iterator(Row, .{SEARCH_TARGET_AGE});
        while (try iter.next(.{})) |_| {}

        sample.* = timer.read();
        stmt.reset();
    }
}

fn benchAlbedoReadAll(allocator: std.mem.Allocator, bucket: *Bucket, samples: []u64) !void {
    for (samples) |*sample| {
        var timer = try std.time.Timer.start();

        var arena_alloc = std.heap.ArenaAllocator.init(allocator);
        defer arena_alloc.deinit();

        var qdoc = try bson.fmt.serialize(.{
            .query = .{},
            .sector = .{ .limit = 1000 },
        }, arena_alloc.allocator());
        _ = &qdoc;

        const q = try Query.parse(arena_alloc.allocator(), qdoc);

        var iter = try bucket.listIterate(&arena_alloc, q);
        defer iter.deinit() catch {};

        while (try iter.next(iter)) |doc| {
            if (doc.get("active").?.boolean.value != true) {
                @branchHint(.unlikely);
                return error.UnexpectedValue;
            }
            // const Row = struct { name: []const u8, age: i32, email: []const u8, active: bool };
            // _ = try bson.fmt.parse(Row, doc, allocator);
        }

        sample.* = timer.read();
    }
}

fn benchAlbedoUpdate(allocator: std.mem.Allocator, bucket: *Bucket, samples: []u64) !void {
    // Pre-build two replacement docs with alternating email so each iteration
    // writes a different value, preventing any short-circuit optimisation.
    var doc_a = try bson.fmt.serialize(.{
        .name = @as([]const u8, UPDATE_TARGET_NAME),
        .age = UPDATE_TARGET_AGE,
        .email = @as([]const u8, "updated_a@example.com"),
        .active = true,
    }, allocator);
    defer doc_a.deinit(allocator);

    var doc_b = try bson.fmt.serialize(.{
        .name = @as([]const u8, UPDATE_TARGET_NAME),
        .age = UPDATE_TARGET_AGE,
        .email = @as([]const u8, "updated_b@example.com"),
        .active = true,
    }, allocator);
    defer doc_b.deinit(allocator);

    for (samples, 0..) |*sample, i| {
        var arena_alloc = std.heap.ArenaAllocator.init(allocator);
        defer arena_alloc.deinit();

        const qdoc = try bson.fmt.serialize(.{
            .query = .{ .age = UPDATE_TARGET_AGE },
        }, arena_alloc.allocator());

        const q = try Query.parse(arena_alloc.allocator(), qdoc);

        var timer = try std.time.Timer.start();

        var iter = try bucket.transformIterate(&arena_alloc, q);
        defer iter.close() catch {};

        const replacement = if (i % 2 == 0) &doc_a else &doc_b;
        try iter.transform(replacement);

        sample.* = timer.read();
    }
}

fn benchSqliteUpdate(db: *sqlite.Db, samples: []u64) !void {
    var stmt = try db.prepare("UPDATE docs SET email = ? WHERE age = ?");
    defer stmt.deinit();

    const emails = [2][]const u8{ "updated_a@example.com", "updated_b@example.com" };

    for (samples, 0..) |*sample, i| {
        var timer = try std.time.Timer.start();

        try stmt.exec(.{}, .{ emails[i % 2], UPDATE_TARGET_AGE });
        stmt.reset();

        sample.* = timer.read();
    }
}

fn benchAlbedoBatchUpdate(allocator: std.mem.Allocator, bucket: *Bucket, samples: []u64) !void {
    const emails = [2][]const u8{ "batch_a@example.com", "batch_b@example.com" };

    var arena_alloc = std.heap.ArenaAllocator.init(allocator);
    defer arena_alloc.deinit();
    // const tx = try bucket.beginTransaction();

    // defer tx.commit() catch {
    //     std.debug.print("Albedo batch update error: \n", .{});
    // };

    const qdoc = try bson.fmt.serialize(.{
        .query = .{},
    }, arena_alloc.allocator());
    var q = try Query.parse(arena_alloc.allocator(), qdoc);

    const email = emails[1];
    const raw_program = try bson.fmt.serialize(.{
        .@"$set" = .{
            .email = email,
        },
    }, arena_alloc.allocator());
    const program = try UpdateProgram.parse(arena_alloc.allocator(), raw_program);

    for (samples, 0..) |*sample, i| {
        var timer = try std.time.Timer.start();

        q.sector = .{ .limit = BATCH_UPDATE_SIZE, .offset = @intCast((i * @as(usize, @intCast(BATCH_UPDATE_SIZE))) % NUM_RECORDS) };

        _ = try bucket.transfigurate(q, program);

        sample.* = timer.read();
    }
}

fn benchSqliteBatchUpdate(db: *sqlite.Db, samples: []u64) !void {
    var stmt = try db.prepare("UPDATE docs SET email = ? WHERE age >= ? AND age < ?");
    defer stmt.deinit();

    const emails = [2][]const u8{ "batch_a@example.com", "batch_b@example.com" };

    for (samples, 0..) |*sample, i| {
        const batch_start: i32 = @intCast((i * @as(usize, @intCast(BATCH_UPDATE_SIZE))) % NUM_RECORDS);
        const batch_end: i32 = batch_start + BATCH_UPDATE_SIZE;

        var timer = try std.time.Timer.start();

        try stmt.exec(.{}, .{ emails[i % 2], batch_start, batch_end });
        stmt.reset();

        sample.* = timer.read();
    }
}

fn benchSqliteReadAll(db: *sqlite.Db, samples: []u64) !void {
    var stmt = try db.prepare("SELECT id, age, active FROM docs LIMIT 1000");
    defer stmt.deinit();

    for (samples) |*sample| {
        var timer = try std.time.Timer.start();

        const Row = struct { id: i64, age: i32, active: i32 };
        var iter = try stmt.iterator(Row, .{});
        while (try iter.next(.{})) |_| {}

        sample.* = timer.read();
        stmt.reset();
    }
}

// ─── Main ────────────────────────────────────────────────────────────────────

pub fn main() !void {
    const allocator = std.heap.smp_allocator;

    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();

    // ── Setup Albedo ─────────────────────────────────────────────────────
    var bucket = try Bucket.openFileWithOptions(arena.allocator(), "file.bucket", .{
        .wal = true,
        .read_durability = .process,
        .auto_vaccuum = false,
        .write_durability = .{
            .periodic = 2048,
        },
        .wal_auto_checkpoint = 10000,
    });
    defer std.fs.cwd().deleteFile("file.bucket") catch {};
    defer bucket.deinit();

    // try bucket.dropIndex("_id");

    // ── Setup SQLite ─────────────────────────────────────────────────────
    var db = try sqlite.Db.init(.{
        .mode = sqlite.Db.Mode{ .File = "file.sqlite" },
        .open_flags = .{ .write = true, .create = true },
        .threading_mode = .Serialized,
    });
    defer std.fs.cwd().deleteFile("file.sqlite") catch {};
    defer db.deinit();

    _ = try db.pragma(void, .{}, "journal_mode", "WAL");
    // _ = try db.pragma(void, .{}, "synchronous", "NORMAL");
    try db.exec(
        "CREATE TABLE IF NOT EXISTS docs (id INTEGER, name TEXT, age INTEGER, email TEXT, active INTEGER)",
        .{},
        .{},
    );
    try db.exec("CREATE UNIQUE INDEX IF NOT EXISTS idx_id ON docs(id)", .{}, .{});

    // ── Print header ─────────────────────────────────────────────────────
    print("\n  {s}{s}◆  ALBEDO vs SQLITE  ◆{s}", .{ C.bold, C.cyan, C.reset });
    print("  {s}records={d}  iters={d}{s}\n", .{ C.dim, NUM_RECORDS, SEARCH_ITERATIONS, C.reset });

    printTableHeader();

    // ══════════════════════════════════════════════════════════════════════
    // 1. INSERTION — single bulk run
    // ══════════════════════════════════════════════════════════════════════
    {
        const s_ns = try benchSqliteInsert(&db);
        const a_ns = try benchAlbedoInsert(arena.allocator(), &bucket);

        var a_buf = [_]u64{a_ns};
        var s_buf = [_]u64{s_ns};
        const a_stats = computeStats(&a_buf);
        const s_stats = computeStats(&s_buf);

        printRow("Albedo insert 100k", a_stats, C.cyan);
        printRow("SQLite insert 100k", s_stats, C.blue);
        printWinner(a_stats, s_stats);
    }

    // ══════════════════════════════════════════════════════════════════════
    // 2. COLUMN SCAN (no index)
    // ══════════════════════════════════════════════════════════════════════
    {
        var a_samples: [SEARCH_ITERATIONS]u64 = undefined;
        var s_samples: [SEARCH_ITERATIONS]u64 = undefined;
        try benchAlbedoScan(arena.allocator(), &bucket, &a_samples);
        try benchSqliteScan(&db, &s_samples);
        const a_stats = computeStats(&a_samples);
        const s_stats = computeStats(&s_samples);

        printRow("Albedo scan", a_stats, C.cyan);
        printRow("SQLite scan", s_stats, C.blue);
        printWinner(a_stats, s_stats);
    }

    // ══════════════════════════════════════════════════════════════════════
    // 3. READ 1000 DOCUMENTS
    // ══════════════════════════════════════════════════════════════════════
    {
        var a_samples: [SEARCH_ITERATIONS]u64 = undefined;
        var s_samples: [SEARCH_ITERATIONS]u64 = undefined;
        try benchAlbedoReadAll(arena.allocator(), &bucket, &a_samples);
        try benchSqliteReadAll(&db, &s_samples);
        const a_stats = computeStats(&a_samples);
        const s_stats = computeStats(&s_samples);

        printRow("Albedo read 1000", a_stats, C.cyan);
        printRow("SQLite read 1000", s_stats, C.blue);
        printWinner(a_stats, s_stats);
    }

    // ══════════════════════════════════════════════════════════════════════
    // 4. INDEX-BASED SEARCH
    // ══════════════════════════════════════════════════════════════════════
    {
        try bucket.ensureIndex("age", .{});
        try db.exec("CREATE UNIQUE INDEX IF NOT EXISTS idx_age ON docs(age)", .{}, .{});

        var a_samples: [SEARCH_ITERATIONS]u64 = undefined;
        var s_samples: [SEARCH_ITERATIONS]u64 = undefined;
        try benchAlbedoIndexSearch(arena.allocator(), &bucket, &a_samples);
        try benchSqliteIndexSearch(&db, &s_samples);
        const a_stats = computeStats(&a_samples);
        const s_stats = computeStats(&s_samples);

        printRow("Albedo index search", a_stats, C.cyan);
        printRow("SQLite index search", s_stats, C.blue);
        printWinner(a_stats, s_stats);
    }

    // ══════════════════════════════════════════════════════════════════════
    // 5. POINT UPDATE via index (age = UPDATE_TARGET_AGE)
    // ══════════════════════════════════════════════════════════════════════
    {
        // age index is already present from benchmark 4
        var a_samples: [SEARCH_ITERATIONS]u64 = undefined;
        var s_samples: [SEARCH_ITERATIONS]u64 = undefined;
        try benchAlbedoUpdate(arena.allocator(), &bucket, &a_samples);
        try benchSqliteUpdate(&db, &s_samples);
        const a_stats = computeStats(&a_samples);
        const s_stats = computeStats(&s_samples);

        printRow("Albedo update (point)", a_stats, C.cyan);
        printRow("SQLite update (point)", s_stats, C.blue);
        printWinner(a_stats, s_stats);
    }

    // ══════════════════════════════════════════════════════════════════════
    // 6. BATCH UPDATE — 100 docs per iteration via age range index
    // ══════════════════════════════════════════════════════════════════════
    {
        var a_samples: [SEARCH_ITERATIONS]u64 = undefined;
        var s_samples: [SEARCH_ITERATIONS]u64 = undefined;
        try benchAlbedoBatchUpdate(arena.allocator(), &bucket, &a_samples);
        try benchSqliteBatchUpdate(&db, &s_samples);
        const a_stats = computeStats(&a_samples);
        const s_stats = computeStats(&s_samples);

        printRow("Albedo batch upd 100", a_stats, C.cyan);
        printRow("SQLite batch upd 100", s_stats, C.blue);
        printWinner(a_stats, s_stats);
    }

    write("\n");
}
