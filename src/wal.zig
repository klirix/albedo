const std = @import("std");
const builtin = @import("builtin");

/// Must match `DEFAULT_PAGE_SIZE` in albedo.zig.
pub const DEFAULT_PAGE_SIZE: u16 = 8192;

/// Native file-descriptor / handle type, selected at compile time:
///   - POSIX (Linux / macOS): `std.posix.fd_t`  (i32)
///   - Windows:               `std.os.windows.HANDLE`
const NativeFd = std.fs.File.Handle;

/// OS handles stored by WalIndex for the mmap'd SHM file.
/// On POSIX only the fd is required; on Windows a separate mapping
/// handle created by CreateFileMappingW must also be kept alive.
const WalIndexHandles = if (builtin.os.tag == .windows) struct {
    file: NativeFd,
    mapping: std.os.windows.HANDLE,
} else NativeFd;

/// Windows-only shims for file-backed virtual memory.
/// The entire namespace is compiled away on non-Windows targets.
const win_mmap = if (builtin.os.tag == .windows) struct {
    const w = std.os.windows;

    pub const PAGE_READWRITE: w.DWORD = 0x04;
    pub const FILE_MAP_ALL_ACCESS: w.DWORD = 0x000F_001F;

    pub extern "kernel32" fn CreateFileMappingW(
        hFile: w.HANDLE,
        lpFileMappingAttributes: ?*anyopaque,
        flProtect: w.DWORD,
        dwMaximumSizeHigh: w.DWORD,
        dwMaximumSizeLow: w.DWORD,
        lpName: ?[*:0]const u16,
    ) callconv(.winapi) ?w.HANDLE;

    pub extern "kernel32" fn MapViewOfFile(
        hFileMappingObject: w.HANDLE,
        dwDesiredAccess: w.DWORD,
        dwFileOffsetHigh: w.DWORD,
        dwFileOffsetLow: w.DWORD,
        dwNumberOfBytesToMap: w.SIZE_T,
    ) callconv(.winapi) ?*anyopaque;

    pub extern "kernel32" fn UnmapViewOfFile(
        lpBaseAddress: *const anyopaque,
    ) callconv(.winapi) w.BOOL;
} else struct {};

/// Write-Ahead Log for crash recovery and MVCC snapshot reads.
///
/// Before any page is written to the main database file the full page image
/// is appended to the WAL. On recovery the WAL is replayed so that the
/// database is brought back to a consistent state.
///
/// The WAL file layout:
///   [ WALHeader (40 bytes) ]
///   [ WALFrame  (frame_header + page bytes) ] *
///
/// An accompanying shared-memory index file (`<path>-shm`) is mmap'd and
/// contains a spinlock-protected skip list that maps (page_id, tx_timestamp)
/// to WAL file offsets. This enables MVCC-style lookups via `page_data()`.
///
/// Supported on POSIX (Linux / macOS) and Windows.
pub const WAL = struct {

    // ── WAL Header (40 bytes) ─────────────────────────────────────────

    /// Persisted at the start of every WAL file.
    pub const Header = extern struct {
        magic: [4]u8 = .{ 'W', 'A', 'L', 0 },
        version: u16 = 1,
        page_size: u16 = DEFAULT_PAGE_SIZE,
        frame_count: u64 = 0,
        salt: u64 = 0,
        checksum: u64 = 0,
        /// Timestamp of the latest committed transaction.
        tx_timestamp: i64 = 0,

        pub const byte_size: usize = 40;

        comptime {
            if (@sizeOf(Header) != byte_size)
                @compileError("WAL Header must be exactly 40 bytes");
        }

        pub fn toBytes(self: *const Header) [byte_size]u8 {
            var buf: [byte_size]u8 = undefined;
            @memcpy(buf[0..4], &self.magic);
            std.mem.writeInt(u16, buf[4..6], self.version, .little);
            std.mem.writeInt(u16, buf[6..8], self.page_size, .little);
            std.mem.writeInt(u64, buf[8..16], self.frame_count, .little);
            std.mem.writeInt(u64, buf[16..24], self.salt, .little);
            std.mem.writeInt(u64, buf[24..32], self.checksum, .little);
            std.mem.writeInt(i64, buf[32..40], self.tx_timestamp, .little);
            return buf;
        }

        pub fn fromBytes(raw: *const [byte_size]u8) Header {
            return .{
                .magic = raw[0..4].*,
                .version = std.mem.readInt(u16, raw[4..6], .little),
                .page_size = std.mem.readInt(u16, raw[6..8], .little),
                .frame_count = std.mem.readInt(u64, raw[8..16], .little),
                .salt = std.mem.readInt(u64, raw[16..24], .little),
                .checksum = std.mem.readInt(u64, raw[24..32], .little),
                .tx_timestamp = std.mem.readInt(i64, raw[32..40], .little),
            };
        }
    };

    // ── Frame Header (32 bytes) ───────────────────────────────────────

    /// Each frame records exactly one page write.
    pub const FrameHeader = extern struct {
        page_id: u64,
        /// Monotonically increasing commit counter for partial-write detection.
        commit_seq: u64,
        checksum: u64,
        /// Transaction timestamp this page write belongs to.
        tx_timestamp: i64,

        pub const byte_size: usize = 32;

        comptime {
            if (@sizeOf(FrameHeader) != byte_size)
                @compileError("WAL FrameHeader must be exactly 32 bytes");
        }

        pub fn toBytes(self: *const FrameHeader) [byte_size]u8 {
            var buf: [byte_size]u8 = undefined;
            std.mem.writeInt(u64, buf[0..8], self.page_id, .little);
            std.mem.writeInt(u64, buf[8..16], self.commit_seq, .little);
            std.mem.writeInt(u64, buf[16..24], self.checksum, .little);
            std.mem.writeInt(i64, buf[24..32], self.tx_timestamp, .little);
            return buf;
        }

        pub fn fromBytes(raw: *const [byte_size]u8) FrameHeader {
            return .{
                .page_id = std.mem.readInt(u64, raw[0..8], .little),
                .commit_seq = std.mem.readInt(u64, raw[8..16], .little),
                .checksum = std.mem.readInt(u64, raw[16..24], .little),
                .tx_timestamp = std.mem.readInt(i64, raw[24..32], .little),
            };
        }
    };

    pub const frame_size: usize = FrameHeader.byte_size + DEFAULT_PAGE_SIZE;

    /// Sentinel page_id used for BucketHeader WAL frames.
    /// When a frame carries this id, its first 64 bytes hold the BucketHeader
    /// (the remainder is zero-padded) and should be written to file offset 0.
    pub const HEADER_PAGE_ID: u64 = std.math.maxInt(u64);

    // ── Skip-list constants ───────────────────────────────────────────

    pub const MAX_LEVEL: u8 = 12;
    const SENTINEL: u32 = 0xFFFF_FFFF;
    const DEFAULT_INDEX_CAPACITY: u32 = 65536;

    // ── Shared-memory layout types ────────────────────────────────────

    /// Header of the mmap'd `<path>-shm` file (128 bytes).
    pub const ShmHeader = extern struct {
        magic: [4]u8,
        version: u16,
        max_level: u8,
        current_level: u8,
        entry_count: u32,
        capacity: u32,
        /// Atomic spinlock: 0 = unlocked, 1 = write-locked.
        write_lock: u32,
        /// Number of active readers (atomic).
        reader_count: u32,
        /// Head sentinel's forward pointers (one per level).
        head_forward: [MAX_LEVEL]u32,
        /// Number of processes/connections that have this WAL open (atomic).
        active_connections: u32,
        _pad0: [4]u8,
        /// Latest committed oplog sequence number (atomic, monotonic).
        oplog_seqno: u64,
        /// Sequence number of the oldest entry still retained in the ring.
        oplog_oldest_seqno: u64,
        /// Byte offset within the oplog ring where the next entry will be written.
        oplog_write_pos: u32,
        /// Size of the oplog ring buffer region in bytes.
        /// 0 means the oplog is disabled (also used by older SHM files that pre-date
        /// this field — the reserved bytes were zero, so 0 acts as a "legacy/unset"
        /// sentinel and enables seamless migration).
        oplog_size: u32,
        _reserved: [24]u8,

        pub const byte_size: usize = 128;

        comptime {
            if (@sizeOf(ShmHeader) != byte_size)
                @compileError("ShmHeader must be exactly 128 bytes");
        }
    };

    /// A single skip-list node in the mmap'd region (72 bytes).
    pub const ShmNode = extern struct {
        page_id: u64,
        tx_timestamp: i64,
        wal_offset: u64,
        forward: [MAX_LEVEL]u32,

        pub const byte_size: usize = 72;

        comptime {
            if (@sizeOf(ShmNode) != byte_size)
                @compileError("ShmNode must be exactly 72 bytes");
        }
    };

    // ── Oplog (circular buffer in the SHM region) ─────────────────────

    /// Size of the oplog ring buffer region in the SHM file.
    pub const DEFAULT_OPLOG_REGION_SIZE: usize = 4 * 1024 * 1024; // 4 MiB

    /// Maximum payload size that is stored inline in an oplog entry.
    /// Larger payloads store a physical location reference instead.
    pub const OPLOG_INLINE_MAX: u16 = 1024;

    /// Integer type used for all atomic accesses to oplog_seqno /
    /// oplog_oldest_seqno.  The fields remain u64 in memory (preserving the
    /// ABI layout), but we @ptrCast to *seqno_type so that 32-bit targets
    /// can use a native-width atomic instead of an unavailable 64-bit one.
    /// On 64-bit platforms seqno_type == u64 and nothing is lost.
    pub const seqno_type = usize;

    /// Operation kind for oplog entries.
    pub const OpKind = enum(u8) {
        insert = 1,
        update = 2,
        delete = 3,
    };

    /// Describes how the entry payload is stored.
    pub const PayloadKind = enum(u8) {
        /// Full BSON document bytes are stored inline after the header.
        inline_doc = 0,
        /// Payload is a physical location reference (page_id + offset).
        ref_loc = 1,
        /// No payload (e.g. delete where only the doc_id matters).
        none = 2,
    };

    /// Physical location reference for large or deleted documents.
    pub const DocRef = extern struct {
        page_id: u64,
        offset: u16,
        _pad: [6]u8 = .{0} ** 6,

        pub const byte_size: usize = 16;

        comptime {
            if (@sizeOf(DocRef) != byte_size)
                @compileError("DocRef must be exactly 16 bytes");
        }
    };

    /// Fixed-size header for each oplog ring entry (32 bytes).
    ///
    /// Layout:  seqno(8) | timestamp(8) | doc_id(12) | payload_len(2) | op(1) | payload_kind(1)
    ///
    /// The entry's total on-disk size is `OplogEntryHeader.byte_size + payload_len`.
    /// Readers detect wrap/corruption by checking that `seqno` ≥ expected.
    pub const OplogEntryHeader = extern struct {
        /// Monotonically increasing sequence number.
        seqno: u64,
        /// Wall-clock timestamp of the operation.
        timestamp: i64,
        /// The _id of the affected document (ObjectId stored as 12 raw bytes).
        doc_id: [12]u8,
        /// Length of the payload that follows this header.
        payload_len: u16,
        /// The kind of mutation.
        op: OpKind,
        /// How the payload is encoded.
        payload_kind: PayloadKind,

        pub const byte_size: usize = 32;

        comptime {
            if (@sizeOf(OplogEntryHeader) != byte_size)
                @compileError("OplogEntryHeader must be exactly 32 bytes");
        }
    };

    // ── WalIndex (mmap'd shared-memory skip list) ─────────────────────

    pub const WalIndex = struct {
        fd: WalIndexHandles,
        map: [*]align(std.heap.page_size_min) u8,
        map_len: usize,
        path: []const u8,
        allocator: std.mem.Allocator,
        /// Configured oplog ring size in bytes (0 = disabled).
        oplog_size: u32,

        /// Byte offset where the oplog ring region starts (right after the header).
        pub const oplog_offset: usize = ShmHeader.byte_size;

        /// Byte offset where the skip-list nodes start, computed from the
        /// configured oplog size.
        pub inline fn nodesOffset(self: *const WalIndex) usize {
            return ShmHeader.byte_size + self.oplog_size;
        }

        pub fn init(allocator: std.mem.Allocator, path: []const u8, capacity: u32, oplog_size: u32) Error!WalIndex {
            const stored_path = allocator.dupe(u8, path) catch return Error.WalIndexFailed;
            errdefer allocator.free(stored_path);

            const desired = ShmHeader.byte_size + @as(usize, oplog_size) + @as(usize, capacity) * ShmNode.byte_size;
            const map_len = std.mem.alignForward(usize, desired, std.heap.page_size_min);

            // ── Open the SHM file and map it into memory ──────────────────────

            const fd: WalIndexHandles = if (builtin.os.tag == .windows) blk: {
                // Open (or create) the SHM file for read + write.
                const file_handle = openShmFd(path) catch return Error.WalIndexFailed;
                errdefer closeFd(file_handle);

                // Ensure the file is at least map_len bytes.
                ftruncateFile(file_handle, map_len) catch return Error.WalIndexFailed;

                // Create a named-less file-mapping object backed by the file.
                const high: std.os.windows.DWORD = @truncate(map_len >> 32);
                const low: std.os.windows.DWORD = @truncate(map_len);
                const mapping = win_mmap.CreateFileMappingW(
                    file_handle,
                    null,
                    win_mmap.PAGE_READWRITE,
                    high,
                    low,
                    null,
                ) orelse return Error.WalIndexFailed;
                errdefer std.os.windows.CloseHandle(mapping);

                break :blk .{ .file = file_handle, .mapping = mapping };
            } else blk: {
                // POSIX: open for read+write, create if absent.
                const raw_fd = openShmFd(path) catch return Error.WalIndexFailed;
                errdefer std.posix.close(raw_fd);

                // Ensure the file is at least the required size.
                std.posix.ftruncate(raw_fd, @intCast(map_len)) catch return Error.WalIndexFailed;

                break :blk raw_fd;
            };

            // ── Map the file into the process address space ────────────────────

            const map: [*]align(std.heap.page_size_min) u8 = if (builtin.os.tag == .windows) blk: {
                const ptr = win_mmap.MapViewOfFile(
                    fd.mapping,
                    win_mmap.FILE_MAP_ALL_ACCESS,
                    0,
                    0,
                    map_len,
                ) orelse return Error.WalIndexFailed;
                break :blk @ptrCast(@alignCast(ptr));
            } else blk: {
                const map_slice = std.posix.mmap(
                    null,
                    map_len,
                    std.posix.PROT.READ | std.posix.PROT.WRITE,
                    .{ .TYPE = .SHARED },
                    fd,
                    0,
                ) catch return Error.WalIndexFailed;
                break :blk map_slice.ptr;
            };

            var idx = WalIndex{
                .fd = fd,
                .map = map,
                .map_len = map_len,
                .path = stored_path,
                .allocator = allocator,
                .oplog_size = oplog_size,
            };

            // Validate or initialize the SHM header.
            const shm = idx.shmHeader();
            if (!std.mem.eql(u8, &shm.magic, &.{ 'W', 'A', 'L', 'I' })) {
                // Fresh or stale (bad magic) file — initialize from scratch.
                shm.* = .{
                    .magic = .{ 'W', 'A', 'L', 'I' },
                    .version = 1,
                    .max_level = MAX_LEVEL,
                    .current_level = 0,
                    .entry_count = 0,
                    .capacity = capacity,
                    .write_lock = 0,
                    .reader_count = 0,
                    .head_forward = [_]u32{SENTINEL} ** MAX_LEVEL,
                    .active_connections = 0,
                    ._pad0 = [_]u8{0} ** 4,
                    .oplog_seqno = 0,
                    .oplog_oldest_seqno = 0,
                    .oplog_write_pos = 0,
                    .oplog_size = oplog_size,
                    ._reserved = [_]u8{0} ** 24,
                };
            } else if (shm.oplog_size == 0) {
                // Older SHM format (oplog_size field was in _reserved and zeroed).
                // Treat as legacy: reinitialize so the new oplog_size takes effect.
                // The skip-list index will be rebuilt from WAL frames by the caller.
                shm.* = .{
                    .magic = .{ 'W', 'A', 'L', 'I' },
                    .version = 1,
                    .max_level = MAX_LEVEL,
                    .current_level = 0,
                    .entry_count = 0,
                    .capacity = capacity,
                    .write_lock = 0,
                    .reader_count = 0,
                    .head_forward = [_]u32{SENTINEL} ** MAX_LEVEL,
                    .active_connections = 0,
                    ._pad0 = [_]u8{0} ** 4,
                    .oplog_seqno = 0,
                    .oplog_oldest_seqno = 0,
                    .oplog_write_pos = 0,
                    .oplog_size = oplog_size,
                    ._reserved = [_]u8{0} ** 24,
                };
            } else if (shm.oplog_size != oplog_size) {
                // Explicit mismatch: the SHM was created with a different oplog size.
                return Error.WalOplogSizeMismatch;
            }
            // else: shm.oplog_size == oplog_size — existing SHM is compatible.

            // Register this connection.
            _ = @atomicRmw(u32, &shm.active_connections, .Add, @as(u32, 1), .release);

            return idx;
        }

        pub fn deinit(self: *WalIndex) void {
            self.disconnect();
            if (builtin.os.tag == .windows) {
                _ = win_mmap.UnmapViewOfFile(@ptrCast(self.map));
                std.os.windows.CloseHandle(self.fd.mapping);
                closeFd(self.fd.file);
            } else {
                const slice: []align(std.heap.page_size_min) const u8 = @alignCast(self.map[0..self.map_len]);
                std.posix.munmap(slice);
                std.posix.close(self.fd);
            }
            self.allocator.free(self.path);
        }

        /// Decrement the active_connections count in the SHM header.
        fn disconnect(self: *WalIndex) void {
            const shm = self.shmHeader();
            _ = @atomicRmw(u32, &shm.active_connections, .Sub, @as(u32, 1), .release);
        }

        /// Returns the number of active connections (other processes/handles).
        pub fn activeConnections(self: *WalIndex) u32 {
            return @atomicLoad(u32, &self.shmHeader().active_connections, .acquire);
        }

        /// Disconnect, unmap, close, and delete the SHM file if this was
        /// the last active connection. Returns true if the file was deleted.
        pub fn deleteFile(self: *WalIndex) bool {
            // Decrement connection count and check if we were the last one.
            self.disconnect();
            const remaining = self.activeConnections();

            // Stash the path before freeing.
            const path_copy = self.path;
            const ally = self.allocator;

            // Unmap and close the fd(s) first.
            if (builtin.os.tag == .windows) {
                _ = win_mmap.UnmapViewOfFile(@ptrCast(self.map));
                std.os.windows.CloseHandle(self.fd.mapping);
                closeFd(self.fd.file);
            } else {
                const slice: []align(std.heap.page_size_min) const u8 = @alignCast(self.map[0..self.map_len]);
                std.posix.munmap(slice);
                std.posix.close(self.fd);
            }

            if (remaining == 0) {
                std.fs.cwd().deleteFile(path_copy) catch {};
                ally.free(path_copy);
                return true;
            }
            ally.free(path_copy);
            return false;
        }

        pub fn shmHeader(self: *WalIndex) *ShmHeader {
            return @ptrCast(@alignCast(self.map));
        }

        fn nodePtr(self: *WalIndex, idx: u32) *ShmNode {
            const base: [*]u8 = @ptrCast(self.map);
            const offset = self.nodesOffset() + @as(usize, idx) * ShmNode.byte_size;
            return @ptrCast(@alignCast(base + offset));
        }

        // ── Oplog ring helpers ──

        /// Returns a pointer to the start of the oplog ring region.
        fn oplogBase(self: *WalIndex) [*]u8 {
            const base: [*]u8 = @ptrCast(self.map);
            return base + oplog_offset;
        }

        /// Append an oplog entry into the circular buffer.
        /// Must be called while holding the write lock.
        pub fn appendOplogEntry(
            self: *WalIndex,
            op: OpKind,
            doc_id: [12]u8,
            timestamp: i64,
            payload: ?[]const u8,
            payload_kind: PayloadKind,
        ) void {
            // Oplog is disabled for this bucket; silently skip.
            if (self.oplog_size == 0) return;

            const shm = self.shmHeader();
            const ring = self.oplogBase();
            const region = @as(u32, self.oplog_size);

            // Write lock is held — plain read/write is safe and preserves full u64 precision.
            const seqno: u64 = shm.oplog_seqno + 1;
            const plen: u16 = if (payload) |p| @intCast(p.len) else 0;
            const total: u32 = @intCast(OplogEntryHeader.byte_size + @as(u32, plen));

            var write_pos = @atomicLoad(u32, &shm.oplog_write_pos, .monotonic);

            // Compute the new oldest_seqno (deferred publish — see below).
            const new_oldest = self.evictOverwritten(shm, ring, write_pos, total);

            // Write header.
            const hdr = OplogEntryHeader{
                .seqno = seqno,
                .doc_id = doc_id,
                .timestamp = timestamp,
                .payload_len = plen,
                .op = op,
                .payload_kind = payload_kind,
            };
            const hdr_bytes: [OplogEntryHeader.byte_size]u8 = @bitCast(hdr);
            self.ringWrite(ring, write_pos, &hdr_bytes);
            write_pos = (write_pos + @as(u32, @intCast(OplogEntryHeader.byte_size))) % region;

            // Write payload.
            if (payload) |p| {
                self.ringWrite(ring, write_pos, p);
                write_pos = (write_pos + @as(u32, plen)) % region;
            }

            // Publish: write_pos → seqno → oldest_seqno, all with release
            // ordering so readers' acquire-loads see a consistent state.
            // Crucially, seqno is stored BEFORE oldest_seqno so that a
            // reader never observes oldest > head.
            @atomicStore(u32, &shm.oplog_write_pos, write_pos, .release);
            @atomicStore(seqno_type, @as(*seqno_type, @ptrCast(&shm.oplog_seqno)), @truncate(seqno), .release);
            if (new_oldest != shm.oplog_oldest_seqno) {
                @atomicStore(seqno_type, @as(*seqno_type, @ptrCast(&shm.oplog_oldest_seqno)), @truncate(new_oldest), .release);
            }
        }

        /// Read a single oplog entry starting at `ring_pos`.
        /// Returns the header, the payload slice (copied into `buf`), and
        /// the ring position immediately after the entry.
        /// Returns `null` if the entry at that position has been overwritten.
        pub fn readOplogEntry(
            self: *WalIndex,
            ring_pos: u32,
            buf: []u8,
        ) ?struct { header: OplogEntryHeader, payload: []const u8, next_pos: u32 } {
            if (self.oplog_size == 0) return null;
            const ring = self.oplogBase();
            const region = @as(u32, self.oplog_size);

            var hdr_bytes: [OplogEntryHeader.byte_size]u8 = undefined;
            self.ringRead(ring, ring_pos, &hdr_bytes);
            const hdr: OplogEntryHeader = @bitCast(hdr_bytes);

            // Sanity: seqno 0 means empty/uninitialized.
            if (hdr.seqno == 0) return null;

            var pos = (ring_pos + @as(u32, @intCast(OplogEntryHeader.byte_size))) % region;
            const plen: u32 = hdr.payload_len;
            if (plen > 0 and plen <= buf.len) {
                self.ringRead(ring, pos, buf[0..plen]);
            }
            pos = (pos + plen) % region;

            return .{
                .header = hdr,
                .payload = if (plen > 0 and plen <= buf.len) buf[0..plen] else buf[0..0],
                .next_pos = pos,
            };
        }

        /// Copy `data` into the ring at `pos`, wrapping around oplog_size.
        fn ringWrite(self: *WalIndex, ring: [*]u8, pos: u32, data: []const u8) void {
            const region = self.oplog_size;
            const first = @min(data.len, region - pos);
            @memcpy(ring[pos .. pos + first], data[0..first]);
            if (first < data.len) {
                @memcpy(ring[0 .. data.len - first], data[first..]);
            }
        }

        /// Read `data.len` bytes from the ring at `pos`, wrapping around.
        fn ringRead(self: *WalIndex, ring: [*]u8, pos: u32, data: []u8) void {
            const region = self.oplog_size;
            const first = @min(data.len, region - pos);
            @memcpy(data[0..first], ring[pos .. pos + first]);
            if (first < data.len) {
                @memcpy(data[first..], ring[0 .. data.len - first]);
            }
        }

        /// Count entries that would be overwritten by a new entry of
        /// `total` bytes at `write_pos` and return the new oldest seqno.
        /// Does NOT publish the value — the caller must store it after
        /// publishing the new `oplog_seqno` so readers never observe
        /// `oldest > head`.
        fn evictOverwritten(
            self: *WalIndex,
            shm: *ShmHeader,
            ring: [*]u8,
            write_pos: u32,
            total: u32,
        ) u64 {
            // Write lock is held — plain reads give full u64 values.
            const oldest: u64 = shm.oplog_oldest_seqno;
            const current: u64 = shm.oplog_seqno;
            if (oldest == 0 and current == 0) {
                // Ring is empty, nothing to evict.
                return 0;
            }
            // Walk from the logical oldest entry forward, evicting any that
            // overlap with the region [write_pos .. write_pos + total).
            // Because we only ever append sequentially, we just need to
            // check whether the distance from write_pos to the next read
            // position is less than total.
            var scan_pos = write_pos;
            var evicted: u64 = 0;
            var remaining = total;
            const region = @as(u32, self.oplog_size);
            while (remaining > 0) {
                var hdr_bytes: [OplogEntryHeader.byte_size]u8 = undefined;
                // read header at scan_pos (may wrap)
                const first = @min(OplogEntryHeader.byte_size, region - scan_pos);
                @memcpy(hdr_bytes[0..first], ring[scan_pos .. scan_pos + first]);
                if (first < OplogEntryHeader.byte_size) {
                    @memcpy(hdr_bytes[first..], ring[0 .. OplogEntryHeader.byte_size - first]);
                }
                const hdr: OplogEntryHeader = @bitCast(hdr_bytes);
                if (hdr.seqno == 0) break; // uninitialized region
                if (hdr.seqno < oldest) break; // already evicted
                const entry_total: u32 = @as(u32, @intCast(OplogEntryHeader.byte_size)) + hdr.payload_len;
                evicted += 1;
                scan_pos = (scan_pos + entry_total) % region;
                if (remaining <= entry_total) break;
                remaining -= entry_total;
            }
            return oldest + evicted;
        }

        // ── Locking ──

        pub fn acquireWrite(self: *WalIndex) void {
            const lock_ptr: *u32 = &self.shmHeader().write_lock;
            while (@cmpxchgWeak(u32, lock_ptr, @as(u32, 0), @as(u32, 1), .acquire, .monotonic) != null) {
                std.atomic.spinLoopHint();
            }
        }

        pub fn releaseWrite(self: *WalIndex) void {
            @atomicStore(u32, &self.shmHeader().write_lock, @as(u32, 0), .release);
        }

        pub fn acquireRead(self: *WalIndex) void {
            _ = @atomicRmw(u32, &self.shmHeader().reader_count, .Add, @as(u32, 1), .acquire);
        }

        pub fn releaseRead(self: *WalIndex) void {
            _ = @atomicRmw(u32, &self.shmHeader().reader_count, .Sub, @as(u32, 1), .release);
        }

        // ── Skip-list helpers ──

        fn keyLessThan(a_page: u64, a_tx: i64, b_page: u64, b_tx: i64) bool {
            if (a_page != b_page) return a_page < b_page;
            return a_tx < b_tx;
        }

        fn keyLessOrEqual(a_page: u64, a_tx: i64, b_page: u64, b_tx: i64) bool {
            if (a_page != b_page) return a_page < b_page;
            return a_tx <= b_tx;
        }

        fn randomLevel() u8 {
            var level: u8 = 1;
            var buf: [2]u8 = undefined;
            std.crypto.random.bytes(&buf);
            var bits: u16 = std.mem.readInt(u16, &buf, .little);
            while (level < MAX_LEVEL) {
                if ((bits & 1) == 0) break;
                level += 1;
                bits >>= 1;
            }
            return level;
        }

        /// Insert a (page_id, tx_timestamp) → wal_offset mapping.
        /// Caller MUST hold the write lock.
        pub fn insert(self: *WalIndex, page_id: u64, tx_timestamp: i64, wal_offset: u64) Error!void {
            const shm = self.shmHeader();
            if (shm.entry_count >= shm.capacity) return Error.WalIndexFailed;

            var update: [MAX_LEVEL]u32 = [_]u32{SENTINEL} ** MAX_LEVEL;
            var current: u32 = SENTINEL; // SENTINEL = head sentinel
            var lvl: u8 = shm.current_level;
            while (lvl > 0) {
                lvl -= 1;
                var fwd = if (current == SENTINEL)
                    shm.head_forward[lvl]
                else
                    self.nodePtr(current).forward[lvl];
                while (fwd != SENTINEL) {
                    const n = self.nodePtr(fwd);
                    if (keyLessOrEqual(n.page_id, n.tx_timestamp, page_id, tx_timestamp)) {
                        current = fwd;
                        fwd = n.forward[lvl];
                    } else break;
                }
                update[lvl] = current;
            }

            const new_lvl = randomLevel();
            if (new_lvl > shm.current_level) {
                var i: u8 = shm.current_level;
                while (i < new_lvl) : (i += 1) {
                    update[i] = SENTINEL;
                }
                shm.current_level = new_lvl;
            }

            const new_idx = shm.entry_count;
            shm.entry_count += 1;

            const node = self.nodePtr(new_idx);
            node.page_id = page_id;
            node.tx_timestamp = tx_timestamp;
            node.wal_offset = wal_offset;
            node.forward = [_]u32{SENTINEL} ** MAX_LEVEL;

            var i: u8 = 0;
            while (i < new_lvl) : (i += 1) {
                if (update[i] == SENTINEL) {
                    node.forward[i] = shm.head_forward[i];
                    shm.head_forward[i] = new_idx;
                } else {
                    const pred = self.nodePtr(update[i]);
                    node.forward[i] = pred.forward[i];
                    pred.forward[i] = new_idx;
                }
            }
        }

        /// Look up the WAL offset for the latest version of `page_id` with
        /// tx_timestamp ≤ max_tx. Caller MUST hold the read or write lock.
        /// Returns null when no matching entry exists.
        pub fn lookup(self: *WalIndex, page_id: u64, max_tx: i64) ?u64 {
            const shm = self.shmHeader();
            if (shm.entry_count == 0 or shm.current_level == 0) return null;

            var current: u32 = SENTINEL;
            var lvl: u8 = shm.current_level;
            while (lvl > 0) {
                lvl -= 1;
                var fwd = if (current == SENTINEL)
                    shm.head_forward[lvl]
                else
                    self.nodePtr(current).forward[lvl];

                while (fwd != SENTINEL) {
                    const n = self.nodePtr(fwd);
                    if (keyLessOrEqual(n.page_id, n.tx_timestamp, page_id, max_tx)) {
                        current = fwd;
                        fwd = n.forward[lvl];
                    } else break;
                }
            }

            if (current == SENTINEL) return null;
            const found = self.nodePtr(current);
            if (found.page_id == page_id and found.tx_timestamp <= max_tx) {
                return found.wal_offset;
            }
            return null;
        }

        /// Clear all entries. Caller MUST hold the write lock.
        pub fn clear(self: *WalIndex) void {
            const shm = self.shmHeader();
            shm.entry_count = 0;
            shm.current_level = 0;
            shm.head_forward = [_]u32{SENTINEL} ** MAX_LEVEL;
        }
    };

    // ── Errors ────────────────────────────────────────────────────────

    pub const Error = error{
        WalOpenFailed,
        WalWriteFailed,
        WalReadFailed,
        WalSyncFailed,
        WalCorrupted,
        WalRecoveryFailed,
        WalIndexFailed,
        WalPageNotFound,
        /// The oplog_size stored in the SHM file does not match the value
        /// requested when opening this WAL.  Either re-open with the
        /// matching size or delete the stale SHM file and retry.
        WalOplogSizeMismatch,
    };

    // ── WAL state ─────────────────────────────────────────────────────

    write_fd: NativeFd,
    read_fd: NativeFd,
    header: Header,
    /// Next sequence number to assign.
    next_seq: u64,
    path: []const u8,
    allocator: std.mem.Allocator,
    /// Timestamp of the latest committed (sync'd) transaction.
    latest_committed_tx: i64,
    /// Maximum tx_timestamp seen since the last sync.
    pending_max_tx: i64,
    /// mmap'd shared-memory page index.
    index: WalIndex,

    // ── Lifecycle ─────────────────────────────────────────────────────

    /// Open (or create) a WAL file at `path`.
    /// An accompanying `<path>-shm` file is created for the mmap'd page index.
    /// `oplog_size` controls the circular oplog ring buffer: pass 0 to disable
    /// the oplog, or any non-zero value for a custom ring size (use
    /// WAL.DEFAULT_OPLOG_REGION_SIZE` for the default 4 MiB).  If an existing SHM file
    /// was created with a different non-zero size the call returns
    /// `Error.WalOplogSizeMismatch`.
    /// Supported on POSIX (Linux / macOS) and Windows.
    pub fn init(allocator: std.mem.Allocator, path: []const u8, oplog_size: u32) Error!WAL {
        const stored_path = allocator.dupe(u8, path) catch return Error.WalOpenFailed;
        errdefer allocator.free(stored_path);

        // Open the write fd for append-only writes; create if absent.
        const write_fd = openWriteFd(path) catch return Error.WalOpenFailed;
        errdefer closeFd(write_fd);

        // Open a separate read-only fd.
        const read_fd = openReadFd(path) catch return Error.WalOpenFailed;
        errdefer closeFd(read_fd);

        // Try to read an existing header; if the file is empty, write a fresh one.
        var wal_header: Header = .{};
        var next_seq: u64 = 1;

        const file_size_val = fileSize(read_fd) catch return Error.WalReadFailed;
        if (file_size_val >= Header.byte_size) {
            var hdr_buf: [Header.byte_size]u8 = undefined;
            preadAll(read_fd, &hdr_buf, 0) catch return Error.WalReadFailed;
            wal_header = Header.fromBytes(&hdr_buf);

            if (!std.mem.eql(u8, &wal_header.magic, &.{ 'W', 'A', 'L', 0 })) {
                return Error.WalCorrupted;
            }

            next_seq = wal_header.frame_count + 1;
        } else {
            // Seed the salt from OS randomness.
            var salt_bytes: [8]u8 = undefined;
            std.crypto.random.bytes(&salt_bytes);
            wal_header.salt = std.mem.readInt(u64, &salt_bytes, .little);

            // Write the initial header. Because the write fd is append-only and
            // the file is empty, this lands at offset 0.
            const hdr_bytes = wal_header.toBytes();
            writeAll(write_fd, &hdr_bytes) catch return Error.WalWriteFailed;
            fsync(write_fd) catch return Error.WalSyncFailed;
        }

        // Build the SHM index path: "<wal_path>-shm"
        const shm_path = allocator.alloc(u8, path.len + 4) catch return Error.WalOpenFailed;
        defer allocator.free(shm_path);
        @memcpy(shm_path[0..path.len], path);
        @memcpy(shm_path[path.len..], "-shm");

        var wal_index = try WalIndex.init(allocator, shm_path, DEFAULT_INDEX_CAPACITY, oplog_size);
        errdefer wal_index.deinit();

        var wal = WAL{
            .write_fd = write_fd,
            .read_fd = read_fd,
            .header = wal_header,
            .next_seq = next_seq,
            .path = stored_path,
            .allocator = allocator,
            .latest_committed_tx = wal_header.tx_timestamp,
            .pending_max_tx = wal_header.tx_timestamp,
            .index = wal_index,
        };

        // Rebuild the SHM index from WAL frames.
        try wal.rebuildIndex();

        return wal;
    }

    pub fn deinit(self: *WAL) void {
        self.index.deinit();
        closeFd(self.write_fd);
        closeFd(self.read_fd);
        self.allocator.free(self.path);
    }

    /// Sync, checkpoint, close file descriptors, and delete the WAL and
    /// SHM files. Call this on graceful database close when all WAL frames
    /// have already been applied to the main database file.
    /// Files are only removed when this is the last active connection.
    /// Sync, close file descriptors, and — if this is the last active
    /// connection — checkpoint (truncate) the WAL and delete both the
    /// WAL and SHM files.  When other connections are still alive the
    /// WAL content is preserved so they can keep reading from it.
    pub fn consumeAndClose(self: *WAL) void {
        // 1. Sync any pending writes.
        self.sync() catch {};

        // 2. Only truncate the WAL when we are the last user.  Other
        //    processes may still be serving reads from WAL frames.
        const is_last = self.index.activeConnections() <= 1;
        if (is_last) {
            self.checkpoint() catch {};
        }

        // 3. Disconnect from SHM and delete the file if we were the last user.
        const last_connection = self.index.deleteFile();

        // 4. Close WAL file descriptors.
        closeFd(self.write_fd);
        closeFd(self.read_fd);

        // 5. Only delete the WAL file if no other connections remain.
        if (last_connection) {
            std.fs.cwd().deleteFile(self.path) catch {};
        }

        // 6. Free the path allocation.
        self.allocator.free(self.path);
    }

    // ── Writing ───────────────────────────────────────────────────────

    /// Append a full page image to the WAL. `page_data_buf` must be exactly
    /// `DEFAULT_PAGE_SIZE` bytes. `tx_timestamp` tags the frame for MVCC.
    pub fn appendPage(self: *WAL, page_id: u64, page_data_buf: []const u8, tx_timestamp: i64) Error!void {
        std.debug.assert(page_data_buf.len == DEFAULT_PAGE_SIZE);

        const wal_offset = Header.byte_size + self.header.frame_count * frame_size;
        const checksum = std.hash.XxHash3.hash(self.header.salt, page_data_buf);

        const frame_hdr = FrameHeader{
            .page_id = page_id,
            .commit_seq = self.next_seq,
            .checksum = checksum,
            .tx_timestamp = tx_timestamp,
        };

        // Build one contiguous buffer so we do a single write(2) syscall.
        var buf: [frame_size]u8 = undefined;
        const hdr_bytes = frame_hdr.toBytes();
        @memcpy(buf[0..FrameHeader.byte_size], &hdr_bytes);
        @memcpy(buf[FrameHeader.byte_size..], page_data_buf);

        writeAll(self.write_fd, &buf) catch return Error.WalWriteFailed;

        self.next_seq += 1;
        self.header.frame_count += 1;

        if (tx_timestamp > self.pending_max_tx) {
            self.pending_max_tx = tx_timestamp;
        }

        // Update SHM index.
        self.index.acquireWrite();
        defer self.index.releaseWrite();
        try self.index.insert(page_id, tx_timestamp, wal_offset);
    }

    /// Persist all buffered WAL writes to stable storage, update the header
    /// on disk, and advance `latest_committed_tx`.
    pub fn sync(self: *WAL) Error!void {
        fsync(self.write_fd) catch return Error.WalSyncFailed;
        self.latest_committed_tx = self.pending_max_tx;
        self.header.tx_timestamp = self.latest_committed_tx;
        try self.writeHeader();
    }

    // ── Reading / recovery ────────────────────────────────────────────

    pub const FrameIterator = struct {
        wal: *WAL,
        offset: u64,
        remaining: u64,
        buf: [frame_size]u8 = undefined,

        pub fn next(self: *FrameIterator) Error!?struct { header: FrameHeader, data: []const u8 } {
            if (self.remaining == 0) return null;

            preadAll(self.wal.read_fd, &self.buf, self.offset) catch return Error.WalReadFailed;

            const fh = FrameHeader.fromBytes(self.buf[0..FrameHeader.byte_size]);
            const data = self.buf[FrameHeader.byte_size..];

            // Validate checksum.
            const expected = std.hash.XxHash3.hash(self.wal.header.salt, data);
            if (fh.checksum != expected) return Error.WalCorrupted;

            self.offset += frame_size;
            self.remaining -= 1;
            return .{ .header = fh, .data = data };
        }
    };

    /// Return an iterator over all committed frames in order.
    pub fn frames(self: *WAL) FrameIterator {
        return .{
            .wal = self,
            .offset = Header.byte_size,
            .remaining = self.header.frame_count,
        };
    }

    // ── MVCC page read ────────────────────────────────────────────────

    /// Fetch the page data for `page_id` at the latest version whose
    /// tx_timestamp ≤ `max_tx`. Returns `WalPageNotFound` if no matching
    /// frame exists in the WAL.
    pub fn page_data(self: *WAL, page_id: u64, max_tx: i64) Error![DEFAULT_PAGE_SIZE]u8 {
        self.index.acquireRead();
        const maybe_offset = self.index.lookup(page_id, max_tx);
        self.index.releaseRead();

        const wal_offset = maybe_offset orelse return Error.WalPageNotFound;

        var buf: [frame_size]u8 = undefined;
        preadAll(self.read_fd, &buf, wal_offset) catch return Error.WalReadFailed;

        const fh = FrameHeader.fromBytes(buf[0..FrameHeader.byte_size]);
        const data = buf[FrameHeader.byte_size..];

        // Verify the frame actually belongs to the requested page.
        if (fh.page_id != page_id) return Error.WalCorrupted;

        const expected = std.hash.XxHash3.hash(self.header.salt, data);
        if (fh.checksum != expected) return Error.WalCorrupted;

        var result: [DEFAULT_PAGE_SIZE]u8 = undefined;
        @memcpy(&result, data);
        return result;
    }

    // ── Checkpoint / truncate ─────────────────────────────────────────

    /// Truncate the WAL back to just the header, resetting the frame count.
    /// Call this after all WAL frames have been successfully applied to the
    /// main database file.
    pub fn checkpoint(self: *WAL) Error!void {
        // Truncate the file to header-only.
        ftruncateFile(self.write_fd, Header.byte_size) catch return Error.WalWriteFailed;

        self.header.frame_count = 0;
        self.next_seq = 1;

        try self.writeHeader();
        fsync(self.write_fd) catch return Error.WalSyncFailed;

        // Clear the SHM index.
        self.index.acquireWrite();
        defer self.index.releaseWrite();
        self.index.clear();
    }

    // ── Private helpers ───────────────────────────────────────────────

    /// Rebuild the SHM index by scanning all committed WAL frames.
    fn rebuildIndex(self: *WAL) Error!void {
        self.index.acquireWrite();
        defer self.index.releaseWrite();
        self.index.clear();

        if (self.header.frame_count == 0) return;

        var i: u64 = 0;
        while (i < self.header.frame_count) : (i += 1) {
            const offset = Header.byte_size + i * frame_size;
            var hdr_buf: [FrameHeader.byte_size]u8 = undefined;
            preadAll(self.read_fd, &hdr_buf, offset) catch return Error.WalReadFailed;
            const fh = FrameHeader.fromBytes(&hdr_buf);
            try self.index.insert(fh.page_id, fh.tx_timestamp, offset);
        }
    }

    fn writeHeader(self: *WAL) Error!void {
        // The write fd is append-only, so we open a transient fd without
        // append semantics to pwrite the header at offset 0.
        const fd = openRwFd(self.path) catch return Error.WalWriteFailed;
        defer closeFd(fd);

        const hdr_bytes = self.header.toBytes();
        pwriteAll(fd, &hdr_bytes, 0) catch return Error.WalWriteFailed;
        fsync(fd) catch return Error.WalSyncFailed;
    }

    // ── Cross-platform file helpers ───────────────────────────────────

    /// Open or create a file for append-only sequential writes.
    /// On POSIX the O_APPEND flag delegates positioning to the kernel;
    /// on Windows the file is opened for writing and each `writeAll` call
    /// seeks to the end before writing (single-writer WAL, so this is safe).
    fn openWriteFd(path: []const u8) !NativeFd {
        if (builtin.os.tag == .windows) {
            // createFile with truncate=false: opens existing or creates new,
            // positioned at the beginning. writeAll will seek to end each time.
            const f = std.fs.cwd().createFile(path, .{ .truncate = false }) catch return error.OpenFailed;
            return f.handle;
        } else {
            const path_z = std.posix.toPosixPath(path) catch return error.NameTooLong;
            return std.posix.openZ(&path_z, .{
                .ACCMODE = .WRONLY,
                .CREAT = true,
                .APPEND = true,
            }, 0o644) catch return error.OpenFailed;
        }
    }

    /// Open an existing file read-only.
    fn openReadFd(path: []const u8) !NativeFd {
        const f = std.fs.cwd().openFile(path, .{}) catch return error.OpenFailed;
        return f.handle;
    }

    /// Open an existing file for random reads and writes (no append, no create).
    /// Used to pwrite the WAL header at offset 0.
    fn openRwFd(path: []const u8) !NativeFd {
        const f = std.fs.cwd().openFile(path, .{ .mode = .read_write }) catch return error.OpenFailed;
        return f.handle;
    }

    /// Open (or create) a file for read + write without truncation.
    /// Used for the SHM index file.
    fn openShmFd(path: []const u8) !NativeFd {
        const f = std.fs.cwd().createFile(path, .{
            .read = true,
            .truncate = false,
        }) catch return error.OpenFailed;
        return f.handle;
    }

    fn closeFd(fd: NativeFd) void {
        (std.fs.File{ .handle = fd }).close();
    }

    fn preadAll(fd: NativeFd, buf: []u8, offset: u64) !void {
        const n = (std.fs.File{ .handle = fd }).preadAll(buf, offset) catch return error.ReadFailed;
        if (n != buf.len) return error.ReadFailed; // unexpected EOF
    }

    fn pwriteAll(fd: NativeFd, buf: []const u8, offset: u64) !void {
        (std.fs.File{ .handle = fd }).pwriteAll(buf, offset) catch return error.WriteFailed;
    }

    /// Append `buf` to the file.  On POSIX the fd is O_APPEND so the
    /// kernel handles positioning atomically.  On Windows we seek to the
    /// end first (the WAL is single-writer so no race exists).
    fn writeAll(fd: NativeFd, buf: []const u8) !void {
        const file = std.fs.File{ .handle = fd };
        if (builtin.os.tag == .windows) {
            file.seekFromEnd(0) catch return error.WriteFailed;
        }
        file.writeAll(buf) catch return error.WriteFailed;
    }

    fn fsync(fd: NativeFd) !void {
        (std.fs.File{ .handle = fd }).sync() catch return error.SyncFailed;
    }

    fn fileSize(fd: NativeFd) !u64 {
        return (std.fs.File{ .handle = fd }).getEndPos() catch return error.StatFailed;
    }

    fn ftruncateFile(fd: NativeFd, size: u64) !void {
        (std.fs.File{ .handle = fd }).setEndPos(size) catch return error.WriteFailed;
    }
};

// ═══════════════════════════════════════════════════════════════════════
// Tests
// ═══════════════════════════════════════════════════════════════════════

fn cleanupTestFiles(path: []const u8, shm_path: []const u8) void {
    std.fs.cwd().deleteFile(path) catch {};
    std.fs.cwd().deleteFile(shm_path) catch {};
}

// ── Header serialization tests ────────────────────────────────────────

test "WAL header round-trip" {
    var hdr = WAL.Header{};
    hdr.frame_count = 42;
    hdr.salt = 0xDEADBEEF;
    hdr.tx_timestamp = 1700000000;

    const bytes = hdr.toBytes();
    const restored = WAL.Header.fromBytes(&bytes);

    try std.testing.expectEqual(restored.frame_count, 42);
    try std.testing.expectEqual(restored.salt, 0xDEADBEEF);
    try std.testing.expectEqual(restored.version, 1);
    try std.testing.expectEqual(restored.tx_timestamp, 1700000000);
}

test "WAL header default values" {
    const hdr = WAL.Header{};
    try std.testing.expect(std.mem.eql(u8, &hdr.magic, &.{ 'W', 'A', 'L', 0 }));
    try std.testing.expectEqual(hdr.version, 1);
    try std.testing.expectEqual(hdr.page_size, DEFAULT_PAGE_SIZE);
    try std.testing.expectEqual(hdr.frame_count, 0);
    try std.testing.expectEqual(hdr.salt, 0);
    try std.testing.expectEqual(hdr.checksum, 0);
    try std.testing.expectEqual(hdr.tx_timestamp, 0);
}

test "WAL header serializes magic bytes correctly" {
    const hdr = WAL.Header{};
    const bytes = hdr.toBytes();
    try std.testing.expectEqual(bytes[0], 'W');
    try std.testing.expectEqual(bytes[1], 'A');
    try std.testing.expectEqual(bytes[2], 'L');
    try std.testing.expectEqual(bytes[3], 0);
}

test "WAL header preserves all fields through round-trip" {
    const hdr = WAL.Header{
        .magic = .{ 'W', 'A', 'L', 0 },
        .version = 1,
        .page_size = 4096,
        .frame_count = std.math.maxInt(u64),
        .salt = 0x0123456789ABCDEF,
        .checksum = 0xFEDCBA9876543210,
        .tx_timestamp = -1234567890,
    };
    const bytes = hdr.toBytes();
    const restored = WAL.Header.fromBytes(&bytes);

    try std.testing.expectEqual(restored.version, 1);
    try std.testing.expectEqual(restored.page_size, 4096);
    try std.testing.expectEqual(restored.frame_count, std.math.maxInt(u64));
    try std.testing.expectEqual(restored.salt, 0x0123456789ABCDEF);
    try std.testing.expectEqual(restored.checksum, 0xFEDCBA9876543210);
    try std.testing.expectEqual(restored.tx_timestamp, -1234567890);
}

test "WAL header tx_timestamp survives negative values" {
    const hdr = WAL.Header{ .tx_timestamp = -9999 };
    const bytes = hdr.toBytes();
    const restored = WAL.Header.fromBytes(&bytes);
    try std.testing.expectEqual(restored.tx_timestamp, -9999);
}

// ── Frame header serialization tests ──────────────────────────────────

test "WAL frame header round-trip" {
    const fh = WAL.FrameHeader{
        .page_id = 7,
        .commit_seq = 100,
        .checksum = 0xCAFEBABE,
        .tx_timestamp = 1700000001,
    };

    const bytes = fh.toBytes();
    const restored = WAL.FrameHeader.fromBytes(&bytes);

    try std.testing.expectEqual(restored.page_id, 7);
    try std.testing.expectEqual(restored.commit_seq, 100);
    try std.testing.expectEqual(restored.checksum, 0xCAFEBABE);
    try std.testing.expectEqual(restored.tx_timestamp, 1700000001);
}

test "WAL frame header preserves max u64 values" {
    const fh = WAL.FrameHeader{
        .page_id = std.math.maxInt(u64),
        .commit_seq = std.math.maxInt(u64),
        .checksum = std.math.maxInt(u64),
        .tx_timestamp = std.math.maxInt(i64),
    };
    const bytes = fh.toBytes();
    const restored = WAL.FrameHeader.fromBytes(&bytes);

    try std.testing.expectEqual(restored.page_id, std.math.maxInt(u64));
    try std.testing.expectEqual(restored.commit_seq, std.math.maxInt(u64));
    try std.testing.expectEqual(restored.checksum, std.math.maxInt(u64));
    try std.testing.expectEqual(restored.tx_timestamp, std.math.maxInt(i64));
}

test "WAL frame header zero values" {
    const fh = WAL.FrameHeader{
        .page_id = 0,
        .commit_seq = 0,
        .checksum = 0,
        .tx_timestamp = 0,
    };
    const bytes = fh.toBytes();
    for (bytes) |b| {
        try std.testing.expectEqual(b, 0);
    }
}

// ── Struct size tests ─────────────────────────────────────────────────

test "WAL struct sizes" {
    try std.testing.expectEqual(WAL.Header.byte_size, 40);
    try std.testing.expectEqual(WAL.FrameHeader.byte_size, 32);
    try std.testing.expectEqual(@sizeOf(WAL.Header), 40);
    try std.testing.expectEqual(@sizeOf(WAL.FrameHeader), 32);
}

test "WAL frame_size equals FrameHeader + page size" {
    try std.testing.expectEqual(WAL.frame_size, WAL.FrameHeader.byte_size + DEFAULT_PAGE_SIZE);
}

test "WAL SHM struct sizes" {
    try std.testing.expectEqual(@sizeOf(WAL.ShmHeader), 128);
    try std.testing.expectEqual(WAL.ShmHeader.byte_size, 128);
    try std.testing.expectEqual(@sizeOf(WAL.ShmNode), 72);
    try std.testing.expectEqual(WAL.ShmNode.byte_size, 72);
}

// ── WAL file I/O tests ───────────────────────────────────────────────

test "WAL open, append, iterate, checkpoint" {
    const allocator = std.testing.allocator;
    const path = "albedo_test_wal.wal";
    const shm_path = "albedo_test_wal.wal-shm";
    cleanupTestFiles(path, shm_path);
    defer cleanupTestFiles(path, shm_path);

    // -- create & write two frames --
    {
        var wal = try WAL.init(allocator, path, WAL.DEFAULT_OPLOG_REGION_SIZE);
        defer wal.deinit();

        var page_buf: [DEFAULT_PAGE_SIZE]u8 = undefined;
        @memset(&page_buf, 0xAA);
        try wal.appendPage(0, &page_buf, 100);

        @memset(&page_buf, 0xBB);
        try wal.appendPage(1, &page_buf, 200);

        try wal.sync();

        try std.testing.expectEqual(wal.header.frame_count, 2);
    }

    // -- reopen and verify frames survive --
    {
        var wal = try WAL.init(allocator, path, WAL.DEFAULT_OPLOG_REGION_SIZE);
        defer wal.deinit();

        try std.testing.expectEqual(wal.header.frame_count, 2);

        var it = wal.frames();
        const f0 = (try it.next()) orelse return error.ExpectedFrame;
        try std.testing.expectEqual(f0.header.page_id, 0);
        try std.testing.expect(f0.data[0] == 0xAA);
        try std.testing.expectEqual(f0.header.tx_timestamp, 100);

        const f1 = (try it.next()) orelse return error.ExpectedFrame;
        try std.testing.expectEqual(f1.header.page_id, 1);
        try std.testing.expect(f1.data[0] == 0xBB);
        try std.testing.expectEqual(f1.header.tx_timestamp, 200);

        try std.testing.expect(try it.next() == null);

        // -- checkpoint (truncate) --
        try wal.checkpoint();
        try std.testing.expectEqual(wal.header.frame_count, 0);
    }

    // -- after checkpoint the WAL is empty --
    {
        var wal = try WAL.init(allocator, path, WAL.DEFAULT_OPLOG_REGION_SIZE);
        defer wal.deinit();
        try std.testing.expectEqual(wal.header.frame_count, 0);
    }
}

test "WAL corrupted magic is rejected on open" {
    const allocator = std.testing.allocator;
    const path = "albedo_test_wal_corrupt_magic.wal";
    const shm_path = "albedo_test_wal_corrupt_magic.wal-shm";
    cleanupTestFiles(path, shm_path);
    defer cleanupTestFiles(path, shm_path);

    {
        const file = try std.fs.cwd().createFile(path, .{});
        defer file.close();
        var hdr = WAL.Header{};
        hdr.magic = .{ 'B', 'A', 'D', '!' };
        const bytes = hdr.toBytes();
        try file.writeAll(&bytes);
    }

    const result = WAL.init(allocator, path, WAL.DEFAULT_OPLOG_REGION_SIZE);
    try std.testing.expectError(error.WalCorrupted, result);
}

test "WAL sequence numbers increment correctly" {
    const allocator = std.testing.allocator;
    const path = "albedo_test_wal_seq.wal";
    const shm_path = "albedo_test_wal_seq.wal-shm";
    cleanupTestFiles(path, shm_path);
    defer cleanupTestFiles(path, shm_path);

    var wal = try WAL.init(allocator, path, WAL.DEFAULT_OPLOG_REGION_SIZE);
    defer wal.deinit();

    try std.testing.expectEqual(wal.next_seq, 1);

    var page_buf: [DEFAULT_PAGE_SIZE]u8 = undefined;
    @memset(&page_buf, 0x11);
    try wal.appendPage(0, &page_buf, 10);
    try std.testing.expectEqual(wal.next_seq, 2);
    try std.testing.expectEqual(wal.header.frame_count, 1);

    @memset(&page_buf, 0x22);
    try wal.appendPage(1, &page_buf, 20);
    try std.testing.expectEqual(wal.next_seq, 3);
    try std.testing.expectEqual(wal.header.frame_count, 2);

    @memset(&page_buf, 0x33);
    try wal.appendPage(2, &page_buf, 30);
    try std.testing.expectEqual(wal.next_seq, 4);
    try std.testing.expectEqual(wal.header.frame_count, 3);
}

test "WAL same page can be written multiple times" {
    const allocator = std.testing.allocator;
    const path = "albedo_test_wal_overwrite.wal";
    const shm_path = "albedo_test_wal_overwrite.wal-shm";
    cleanupTestFiles(path, shm_path);
    defer cleanupTestFiles(path, shm_path);

    {
        var wal = try WAL.init(allocator, path, WAL.DEFAULT_OPLOG_REGION_SIZE);
        defer wal.deinit();

        var page_buf: [DEFAULT_PAGE_SIZE]u8 = undefined;

        @memset(&page_buf, 0xAA);
        try wal.appendPage(0, &page_buf, 10);

        @memset(&page_buf, 0xBB);
        try wal.appendPage(0, &page_buf, 20);

        @memset(&page_buf, 0xCC);
        try wal.appendPage(0, &page_buf, 30);

        try wal.sync();
        try std.testing.expectEqual(wal.header.frame_count, 3);
    }

    {
        var wal = try WAL.init(allocator, path, WAL.DEFAULT_OPLOG_REGION_SIZE);
        defer wal.deinit();

        var it = wal.frames();

        const f0 = (try it.next()) orelse return error.ExpectedFrame;
        try std.testing.expectEqual(f0.header.page_id, 0);
        try std.testing.expectEqual(f0.data[0], 0xAA);

        const f1 = (try it.next()) orelse return error.ExpectedFrame;
        try std.testing.expectEqual(f1.header.page_id, 0);
        try std.testing.expectEqual(f1.data[0], 0xBB);

        const f2 = (try it.next()) orelse return error.ExpectedFrame;
        try std.testing.expectEqual(f2.header.page_id, 0);
        try std.testing.expectEqual(f2.data[0], 0xCC);

        try std.testing.expect(try it.next() == null);
    }
}

test "WAL checkpoint then append starts fresh sequence" {
    const allocator = std.testing.allocator;
    const path = "albedo_test_wal_checkpoint_seq.wal";
    const shm_path = "albedo_test_wal_checkpoint_seq.wal-shm";
    cleanupTestFiles(path, shm_path);
    defer cleanupTestFiles(path, shm_path);

    var wal = try WAL.init(allocator, path, WAL.DEFAULT_OPLOG_REGION_SIZE);
    defer wal.deinit();

    var page_buf: [DEFAULT_PAGE_SIZE]u8 = undefined;
    @memset(&page_buf, 0x01);
    try wal.appendPage(5, &page_buf, 100);
    try wal.sync();

    try wal.checkpoint();
    try std.testing.expectEqual(wal.next_seq, 1);
    try std.testing.expectEqual(wal.header.frame_count, 0);

    var it = wal.frames();
    try std.testing.expect(try it.next() == null);

    @memset(&page_buf, 0x02);
    try wal.appendPage(10, &page_buf, 200);
    try wal.sync();

    try std.testing.expectEqual(wal.header.frame_count, 1);

    var it2 = wal.frames();
    const f = (try it2.next()) orelse return error.ExpectedFrame;
    try std.testing.expectEqual(f.header.page_id, 10);
    try std.testing.expectEqual(f.data[0], 0x02);
    try std.testing.expect(try it2.next() == null);
}

test "WAL checksum validates page data integrity" {
    const allocator = std.testing.allocator;
    const path = "albedo_test_wal_checksum.wal";
    const shm_path = "albedo_test_wal_checksum.wal-shm";
    cleanupTestFiles(path, shm_path);
    defer cleanupTestFiles(path, shm_path);

    {
        var wal = try WAL.init(allocator, path, WAL.DEFAULT_OPLOG_REGION_SIZE);
        defer wal.deinit();

        var page_buf: [DEFAULT_PAGE_SIZE]u8 = undefined;
        @memset(&page_buf, 0xFF);
        try wal.appendPage(0, &page_buf, 100);
        try wal.sync();
    }

    // Corrupt a byte in the page data portion.
    {
        const file = try std.fs.cwd().openFile(path, .{ .mode = .read_write });
        defer file.close();
        const corrupt_offset = WAL.Header.byte_size + WAL.FrameHeader.byte_size + 100;
        try file.pwriteAll(&[_]u8{0x00}, corrupt_offset);
    }

    {
        var wal = try WAL.init(allocator, path, WAL.DEFAULT_OPLOG_REGION_SIZE);
        defer wal.deinit();

        var it = wal.frames();
        const result = it.next();
        try std.testing.expectError(error.WalCorrupted, result);
    }
}

test "WAL many frames stress test" {
    const allocator = std.testing.allocator;
    const path = "albedo_test_wal_stress.wal";
    const shm_path = "albedo_test_wal_stress.wal-shm";
    cleanupTestFiles(path, shm_path);
    defer cleanupTestFiles(path, shm_path);

    const num_frames: u64 = 64;

    {
        var wal = try WAL.init(allocator, path, WAL.DEFAULT_OPLOG_REGION_SIZE);
        defer wal.deinit();

        var page_buf: [DEFAULT_PAGE_SIZE]u8 = undefined;
        for (0..num_frames) |i| {
            @memset(&page_buf, @truncate(i));
            try wal.appendPage(i, &page_buf, @as(i64, @intCast(i)) * 10);
        }
        try wal.sync();
        try std.testing.expectEqual(wal.header.frame_count, num_frames);
    }

    {
        var wal = try WAL.init(allocator, path, WAL.DEFAULT_OPLOG_REGION_SIZE);
        defer wal.deinit();

        try std.testing.expectEqual(wal.header.frame_count, num_frames);

        var it = wal.frames();
        for (0..num_frames) |i| {
            const frame = (try it.next()) orelse return error.ExpectedFrame;
            try std.testing.expectEqual(frame.header.page_id, i);
            const expected_byte: u8 = @truncate(i);
            try std.testing.expectEqual(frame.data[0], expected_byte);
            try std.testing.expectEqual(frame.data[DEFAULT_PAGE_SIZE - 1], expected_byte);
        }
        try std.testing.expect(try it.next() == null);
    }
}

test "WAL empty iterator returns null immediately" {
    const allocator = std.testing.allocator;
    const path = "albedo_test_wal_empty_iter.wal";
    const shm_path = "albedo_test_wal_empty_iter.wal-shm";
    cleanupTestFiles(path, shm_path);
    defer cleanupTestFiles(path, shm_path);

    var wal = try WAL.init(allocator, path, WAL.DEFAULT_OPLOG_REGION_SIZE);
    defer wal.deinit();

    var it = wal.frames();
    try std.testing.expect(try it.next() == null);
}

test "WAL file size matches expected layout" {
    const allocator = std.testing.allocator;
    const path = "albedo_test_wal_filesize.wal";
    const shm_path = "albedo_test_wal_filesize.wal-shm";
    cleanupTestFiles(path, shm_path);
    defer cleanupTestFiles(path, shm_path);

    {
        var wal = try WAL.init(allocator, path, WAL.DEFAULT_OPLOG_REGION_SIZE);
        defer wal.deinit();

        var page_buf: [DEFAULT_PAGE_SIZE]u8 = undefined;
        @memset(&page_buf, 0xAB);
        try wal.appendPage(0, &page_buf, 100);

        @memset(&page_buf, 0xCD);
        try wal.appendPage(1, &page_buf, 200);

        try wal.sync();
    }

    const file = try std.fs.cwd().openFile(path, .{});
    defer file.close();
    const stat = try file.stat();
    const expected_size = WAL.Header.byte_size + 2 * WAL.frame_size;
    try std.testing.expectEqual(stat.size, expected_size);
}

test "WAL page data is preserved byte-for-byte" {
    const allocator = std.testing.allocator;
    const path = "albedo_test_wal_data_fidelity.wal";
    const shm_path = "albedo_test_wal_data_fidelity.wal-shm";
    cleanupTestFiles(path, shm_path);
    defer cleanupTestFiles(path, shm_path);

    var original: [DEFAULT_PAGE_SIZE]u8 = undefined;
    for (&original, 0..) |*byte, i| {
        byte.* = @truncate(i *% 37 +% 13);
    }

    {
        var wal = try WAL.init(allocator, path, WAL.DEFAULT_OPLOG_REGION_SIZE);
        defer wal.deinit();
        try wal.appendPage(42, &original, 1000);
        try wal.sync();
    }

    {
        var wal = try WAL.init(allocator, path, WAL.DEFAULT_OPLOG_REGION_SIZE);
        defer wal.deinit();

        var it = wal.frames();
        const frame = (try it.next()) orelse return error.ExpectedFrame;
        try std.testing.expectEqual(frame.header.page_id, 42);
        try std.testing.expect(std.mem.eql(u8, frame.data, &original));
    }
}

// ── latest_committed_tx tests ─────────────────────────────────────────

test "WAL latest_committed_tx updates on sync" {
    const allocator = std.testing.allocator;
    const path = "albedo_test_wal_commit_tx.wal";
    const shm_path = "albedo_test_wal_commit_tx.wal-shm";
    cleanupTestFiles(path, shm_path);
    defer cleanupTestFiles(path, shm_path);

    var wal = try WAL.init(allocator, path, WAL.DEFAULT_OPLOG_REGION_SIZE);
    defer wal.deinit();

    try std.testing.expectEqual(wal.latest_committed_tx, 0);

    var page_buf: [DEFAULT_PAGE_SIZE]u8 = undefined;
    @memset(&page_buf, 0x11);
    try wal.appendPage(0, &page_buf, 100);

    // Not yet sync'd: latest_committed_tx should still be 0.
    try std.testing.expectEqual(wal.latest_committed_tx, 0);

    try wal.sync();
    try std.testing.expectEqual(wal.latest_committed_tx, 100);

    @memset(&page_buf, 0x22);
    try wal.appendPage(1, &page_buf, 200);
    try wal.appendPage(2, &page_buf, 150); // out of order, pending_max stays 200

    try wal.sync();
    try std.testing.expectEqual(wal.latest_committed_tx, 200);
}

test "WAL latest_committed_tx persists across reopen" {
    const allocator = std.testing.allocator;
    const path = "albedo_test_wal_commit_persist.wal";
    const shm_path = "albedo_test_wal_commit_persist.wal-shm";
    cleanupTestFiles(path, shm_path);
    defer cleanupTestFiles(path, shm_path);

    {
        var wal = try WAL.init(allocator, path, WAL.DEFAULT_OPLOG_REGION_SIZE);
        defer wal.deinit();

        var page_buf: [DEFAULT_PAGE_SIZE]u8 = undefined;
        @memset(&page_buf, 0xAA);
        try wal.appendPage(0, &page_buf, 500);
        try wal.sync();
    }

    {
        var wal = try WAL.init(allocator, path, WAL.DEFAULT_OPLOG_REGION_SIZE);
        defer wal.deinit();
        try std.testing.expectEqual(wal.latest_committed_tx, 500);
    }
}

// ── page_data tests ───────────────────────────────────────────────────

test "WAL page_data returns correct data" {
    const allocator = std.testing.allocator;
    const path = "albedo_test_wal_page_data.wal";
    const shm_path = "albedo_test_wal_page_data.wal-shm";
    cleanupTestFiles(path, shm_path);
    defer cleanupTestFiles(path, shm_path);

    var wal = try WAL.init(allocator, path, WAL.DEFAULT_OPLOG_REGION_SIZE);
    defer wal.deinit();

    var page_buf: [DEFAULT_PAGE_SIZE]u8 = undefined;
    @memset(&page_buf, 0xAA);
    try wal.appendPage(5, &page_buf, 100);
    try wal.sync();

    const data = try wal.page_data(5, 100);
    try std.testing.expectEqual(data[0], 0xAA);
    try std.testing.expectEqual(data[DEFAULT_PAGE_SIZE - 1], 0xAA);
}

test "WAL page_data returns WalPageNotFound for missing page" {
    const allocator = std.testing.allocator;
    const path = "albedo_test_wal_page_missing.wal";
    const shm_path = "albedo_test_wal_page_missing.wal-shm";
    cleanupTestFiles(path, shm_path);
    defer cleanupTestFiles(path, shm_path);

    var wal = try WAL.init(allocator, path, WAL.DEFAULT_OPLOG_REGION_SIZE);
    defer wal.deinit();

    const result = wal.page_data(99, 1000);
    try std.testing.expectError(error.WalPageNotFound, result);
}

test "WAL page_data MVCC returns latest version <= max_tx" {
    const allocator = std.testing.allocator;
    const path = "albedo_test_wal_mvcc.wal";
    const shm_path = "albedo_test_wal_mvcc.wal-shm";
    cleanupTestFiles(path, shm_path);
    defer cleanupTestFiles(path, shm_path);

    var wal = try WAL.init(allocator, path, WAL.DEFAULT_OPLOG_REGION_SIZE);
    defer wal.deinit();

    var page_buf: [DEFAULT_PAGE_SIZE]u8 = undefined;

    // Write page 0 at tx=100 with data 0xAA.
    @memset(&page_buf, 0xAA);
    try wal.appendPage(0, &page_buf, 100);

    // Write page 0 at tx=200 with data 0xBB.
    @memset(&page_buf, 0xBB);
    try wal.appendPage(0, &page_buf, 200);

    // Write page 0 at tx=300 with data 0xCC.
    @memset(&page_buf, 0xCC);
    try wal.appendPage(0, &page_buf, 300);

    try wal.sync();

    // max_tx=100 → should get the 0xAA version.
    const d100 = try wal.page_data(0, 100);
    try std.testing.expectEqual(d100[0], 0xAA);

    // max_tx=250 → should get the 0xBB version (200 ≤ 250, 300 > 250).
    const d250 = try wal.page_data(0, 250);
    try std.testing.expectEqual(d250[0], 0xBB);

    // max_tx=300 → should get the 0xCC version.
    const d300 = try wal.page_data(0, 300);
    try std.testing.expectEqual(d300[0], 0xCC);

    // max_tx=999 → should get the 0xCC version (latest).
    const d999 = try wal.page_data(0, 999);
    try std.testing.expectEqual(d999[0], 0xCC);

    // max_tx=50 → no version exists yet → not found.
    const result = wal.page_data(0, 50);
    try std.testing.expectError(error.WalPageNotFound, result);
}

test "WAL page_data with multiple pages and MVCC" {
    const allocator = std.testing.allocator;
    const path = "albedo_test_wal_mvcc_multi.wal";
    const shm_path = "albedo_test_wal_mvcc_multi.wal-shm";
    cleanupTestFiles(path, shm_path);
    defer cleanupTestFiles(path, shm_path);

    var wal = try WAL.init(allocator, path, WAL.DEFAULT_OPLOG_REGION_SIZE);
    defer wal.deinit();

    var page_buf: [DEFAULT_PAGE_SIZE]u8 = undefined;

    @memset(&page_buf, 0x11);
    try wal.appendPage(1, &page_buf, 100);

    @memset(&page_buf, 0x22);
    try wal.appendPage(2, &page_buf, 100);

    @memset(&page_buf, 0x33);
    try wal.appendPage(1, &page_buf, 200);

    try wal.sync();

    // Page 1 at tx=100 → 0x11.
    const d1_100 = try wal.page_data(1, 100);
    try std.testing.expectEqual(d1_100[0], 0x11);

    // Page 1 at tx=200 → 0x33.
    const d1_200 = try wal.page_data(1, 200);
    try std.testing.expectEqual(d1_200[0], 0x33);

    // Page 2 at tx=100 → 0x22.
    const d2_100 = try wal.page_data(2, 100);
    try std.testing.expectEqual(d2_100[0], 0x22);

    // Page 2 at tx=200 → still 0x22 (no newer write).
    const d2_200 = try wal.page_data(2, 200);
    try std.testing.expectEqual(d2_200[0], 0x22);

    // Page 3 doesn't exist.
    const result = wal.page_data(3, 200);
    try std.testing.expectError(error.WalPageNotFound, result);
}

test "WAL page_data after checkpoint returns not found" {
    const allocator = std.testing.allocator;
    const path = "albedo_test_wal_page_after_ckpt.wal";
    const shm_path = "albedo_test_wal_page_after_ckpt.wal-shm";
    cleanupTestFiles(path, shm_path);
    defer cleanupTestFiles(path, shm_path);

    var wal = try WAL.init(allocator, path, WAL.DEFAULT_OPLOG_REGION_SIZE);
    defer wal.deinit();

    var page_buf: [DEFAULT_PAGE_SIZE]u8 = undefined;
    @memset(&page_buf, 0xAA);
    try wal.appendPage(0, &page_buf, 100);
    try wal.sync();

    // Before checkpoint, page exists.
    const data = try wal.page_data(0, 100);
    try std.testing.expectEqual(data[0], 0xAA);

    try wal.checkpoint();

    // After checkpoint, page is gone from WAL.
    const result = wal.page_data(0, 100);
    try std.testing.expectError(error.WalPageNotFound, result);
}

// ── WalIndex skip-list tests ──────────────────────────────────────────

test "WalIndex insert and lookup" {
    const allocator = std.testing.allocator;
    const path = "albedo_test_walindex.shm";
    std.fs.cwd().deleteFile(path) catch {};
    defer std.fs.cwd().deleteFile(path) catch {};

    var idx = try WAL.WalIndex.init(allocator, path, 256, WAL.DEFAULT_OPLOG_REGION_SIZE);
    defer idx.deinit();

    idx.acquireWrite();
    try idx.insert(10, 100, 1000);
    try idx.insert(10, 200, 2000);
    try idx.insert(20, 100, 3000);
    idx.releaseWrite();

    idx.acquireRead();
    defer idx.releaseRead();

    // Page 10, tx ≤ 100 → offset 1000.
    try std.testing.expectEqual(idx.lookup(10, 100).?, 1000);
    // Page 10, tx ≤ 200 → offset 2000.
    try std.testing.expectEqual(idx.lookup(10, 200).?, 2000);
    // Page 10, tx ≤ 150 → offset 1000 (latest ≤ 150).
    try std.testing.expectEqual(idx.lookup(10, 150).?, 1000);
    // Page 20, tx ≤ 100 → offset 3000.
    try std.testing.expectEqual(idx.lookup(20, 100).?, 3000);
    // Page 99 → not found.
    try std.testing.expect(idx.lookup(99, 999) == null);
    // Page 10, tx < 100 → not found.
    try std.testing.expect(idx.lookup(10, 50) == null);
}

test "WalIndex clear resets state" {
    const allocator = std.testing.allocator;
    const path = "albedo_test_walindex_clear.shm";
    std.fs.cwd().deleteFile(path) catch {};
    defer std.fs.cwd().deleteFile(path) catch {};

    var idx = try WAL.WalIndex.init(allocator, path, 256, WAL.DEFAULT_OPLOG_REGION_SIZE);
    defer idx.deinit();

    idx.acquireWrite();
    try idx.insert(10, 100, 1000);
    idx.releaseWrite();

    idx.acquireRead();
    try std.testing.expect(idx.lookup(10, 100) != null);
    idx.releaseRead();

    idx.acquireWrite();
    idx.clear();
    idx.releaseWrite();

    idx.acquireRead();
    try std.testing.expect(idx.lookup(10, 100) == null);
    idx.releaseRead();
}

test "WalIndex many entries" {
    const allocator = std.testing.allocator;
    const path = "albedo_test_walindex_many.shm";
    std.fs.cwd().deleteFile(path) catch {};
    defer std.fs.cwd().deleteFile(path) catch {};

    var idx = try WAL.WalIndex.init(allocator, path, 4096, WAL.DEFAULT_OPLOG_REGION_SIZE);
    defer idx.deinit();

    idx.acquireWrite();
    for (0..256) |i| {
        try idx.insert(@intCast(i), @intCast(i * 10), @intCast(i * 100));
    }
    idx.releaseWrite();

    idx.acquireRead();
    defer idx.releaseRead();

    // Verify each entry is findable.
    for (0..256) |i| {
        const offset = idx.lookup(@intCast(i), @intCast(i * 10));
        try std.testing.expect(offset != null);
        try std.testing.expectEqual(offset.?, @as(u64, @intCast(i * 100)));
    }
}

// ── consumeAndClose / deleteFile tests ────────────────────────────────

test "consumeAndClose deletes WAL and SHM files" {
    const allocator = std.testing.allocator;
    const wal_path = "albedo_test_consume_close.wal";
    const shm_path = "albedo_test_consume_close.wal-shm";
    cleanupTestFiles(wal_path, shm_path);

    // Create a WAL, write a frame, then consume and close.
    {
        var w = try WAL.init(allocator, wal_path, WAL.DEFAULT_OPLOG_REGION_SIZE);
        var page_buf: [DEFAULT_PAGE_SIZE]u8 = undefined;
        @memset(&page_buf, 0xAB);
        try w.appendPage(1, &page_buf, 100);
        try w.sync();

        // Both files must exist before consume.
        _ = std.fs.cwd().statFile(wal_path) catch unreachable;
        _ = std.fs.cwd().statFile(shm_path) catch unreachable;

        w.consumeAndClose();
    }

    // After consume, both files should be gone.
    if (std.fs.cwd().statFile(wal_path)) |_| {
        return error.TestUnexpectedResult; // WAL file should have been deleted
    } else |_| {}

    if (std.fs.cwd().statFile(shm_path)) |_| {
        return error.TestUnexpectedResult; // SHM file should have been deleted
    } else |_| {}
}

test "WalIndex.deleteFile removes file when no readers" {
    const allocator = std.testing.allocator;
    const path = "albedo_test_walindex_delete.shm";
    std.fs.cwd().deleteFile(path) catch {};

    var idx = try WAL.WalIndex.init(allocator, path, 256, WAL.DEFAULT_OPLOG_REGION_SIZE);

    // File must exist.
    _ = std.fs.cwd().statFile(path) catch unreachable;

    const deleted = idx.deleteFile();
    try std.testing.expect(deleted);

    // File should be gone.
    if (std.fs.cwd().statFile(path)) |_| {
        return error.TestUnexpectedResult;
    } else |_| {}
}

test "deinit does not delete WAL files" {
    const allocator = std.testing.allocator;
    const wal_path = "albedo_test_deinit_no_delete.wal";
    const shm_path = "albedo_test_deinit_no_delete.wal-shm";
    cleanupTestFiles(wal_path, shm_path);
    defer cleanupTestFiles(wal_path, shm_path);

    {
        var w = try WAL.init(allocator, wal_path, WAL.DEFAULT_OPLOG_REGION_SIZE);
        var page_buf: [DEFAULT_PAGE_SIZE]u8 = undefined;
        @memset(&page_buf, 0xCD);
        try w.appendPage(1, &page_buf, 200);
        try w.sync();
        w.deinit();
    }

    // After plain deinit, files should still exist.
    _ = std.fs.cwd().statFile(wal_path) catch unreachable;
    _ = std.fs.cwd().statFile(shm_path) catch unreachable;
}

test "consumeAndClose preserves files when another connection is active" {
    const allocator = std.testing.allocator;
    const wal_path = "albedo_test_multi_conn.wal";
    const shm_path = "albedo_test_multi_conn.wal-shm";
    cleanupTestFiles(wal_path, shm_path);
    defer cleanupTestFiles(wal_path, shm_path);

    // Open first WAL and write a frame.
    var w1 = try WAL.init(allocator, wal_path, WAL.DEFAULT_OPLOG_REGION_SIZE);
    var page_buf: [DEFAULT_PAGE_SIZE]u8 = undefined;
    @memset(&page_buf, 0xAB);
    try w1.appendPage(1, &page_buf, 100);
    try w1.sync();

    // Open a second connection to the same WAL.
    var w2 = try WAL.init(allocator, wal_path, WAL.DEFAULT_OPLOG_REGION_SIZE);

    // Two connections should be registered.
    try std.testing.expectEqual(@as(u32, 2), w1.index.activeConnections());

    // First connection consumes and closes — files must survive.
    w1.consumeAndClose();

    // Both files should still exist because w2 is still alive.
    _ = std.fs.cwd().statFile(wal_path) catch {
        return error.TestUnexpectedResult; // WAL file should still exist
    };
    _ = std.fs.cwd().statFile(shm_path) catch {
        return error.TestUnexpectedResult; // SHM file should still exist
    };

    // Second connection's count should now be 1.
    try std.testing.expectEqual(@as(u32, 1), w2.index.activeConnections());

    // Closing the last connection should remove the files.
    w2.consumeAndClose();

    if (std.fs.cwd().statFile(wal_path)) |_| {
        return error.TestUnexpectedResult; // WAL file should be gone
    } else |_| {}
    if (std.fs.cwd().statFile(shm_path)) |_| {
        return error.TestUnexpectedResult; // SHM file should be gone
    } else |_| {}
}
