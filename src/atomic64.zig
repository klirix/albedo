const std = @import("std");
const builtin = @import("builtin");

// ponytail: 32-bit arches (armv7, i386) lack native 64-bit atomics and
// make `@atomicLoad(u64, ...)` a compile error. Fall back to a global
// 1-byte spinlock — works anywhere a u8 atomic does. Per-field locks
// (or lock-free seqlock) if SHM contention ever shows up in profiles.
const needs_lock = @sizeOf(usize) < 8;

var fallback_lock: std.atomic.Mutex = .unlocked;

inline fn acquire() void {
    while (!fallback_lock.tryLock()) std.atomic.spinLoopHint();
}

inline fn release() void {
    fallback_lock.unlock();
}

pub fn load(ptr: *const u64) u64 {
    if (builtin.single_threaded) return ptr.*;
    if (comptime needs_lock) {
        acquire();
        defer release();
        return ptr.*;
    }
    return @atomicLoad(u64, ptr, .acquire);
}

pub fn store(ptr: *u64, value: u64) void {
    if (builtin.single_threaded) {
        ptr.* = value;
        return;
    }
    if (comptime needs_lock) {
        acquire();
        defer release();
        ptr.* = value;
        return;
    }
    @atomicStore(u64, ptr, value, .release);
}

pub fn fetchAdd(ptr: *u64, value: u64) u64 {
    if (builtin.single_threaded) {
        const old = ptr.*;
        ptr.* += value;
        return old;
    }
    if (comptime needs_lock) {
        acquire();
        defer release();
        const old = ptr.*;
        ptr.* += value;
        return old;
    }
    return @atomicRmw(u64, ptr, .Add, value, .release);
}
