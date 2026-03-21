const std = @import("std");

const vec_size = 16;

pub fn cosine_similarity(a: []const f32, b: []const f32) !f32 {
    @setFloatMode(.optimized);
    const len = a.len;
    if (len == 0) return error.InvalidInput;
    if (len != b.len) return error.InvalidInput;

    const simd_len = (len / vec_size) * vec_size;
    const unroll_step = vec_size * 2;

    var dot_vec0: @Vector(vec_size, f32) = @splat(0.0);
    var dot_vec1: @Vector(vec_size, f32) = @splat(0.0);
    var sum_a_vec0: @Vector(vec_size, f32) = @splat(0.0);
    var sum_a_vec1: @Vector(vec_size, f32) = @splat(0.0);
    var sum_b_vec0: @Vector(vec_size, f32) = @splat(0.0);
    var sum_b_vec1: @Vector(vec_size, f32) = @splat(0.0);

    var i: usize = 0;
    while (i + unroll_step <= simd_len) : (i += unroll_step) {
        const vec1_0: @Vector(vec_size, f32) = a[i..][0..vec_size].*;
        const vec2_0: @Vector(vec_size, f32) = b[i..][0..vec_size].*;
        const vec1_1: @Vector(vec_size, f32) = a[i + vec_size ..][0..vec_size].*;
        const vec2_1: @Vector(vec_size, f32) = b[i + vec_size ..][0..vec_size].*;
        dot_vec0 = @mulAdd(@Vector(vec_size, f32), vec1_0, vec2_0, dot_vec0);
        dot_vec1 = @mulAdd(@Vector(vec_size, f32), vec1_1, vec2_1, dot_vec1);
        sum_a_vec0 = @mulAdd(@Vector(vec_size, f32), vec1_0, vec1_0, sum_a_vec0);
        sum_a_vec1 = @mulAdd(@Vector(vec_size, f32), vec1_1, vec1_1, sum_a_vec1);
        sum_b_vec0 = @mulAdd(@Vector(vec_size, f32), vec2_0, vec2_0, sum_b_vec0);
        sum_b_vec1 = @mulAdd(@Vector(vec_size, f32), vec2_1, vec2_1, sum_b_vec1);
    }

    var dot_vec = dot_vec0 + dot_vec1;
    var sum_a_vec = sum_a_vec0 + sum_a_vec1;
    var sum_b_vec = sum_b_vec0 + sum_b_vec1;

    while (i < simd_len) : (i += vec_size) {
        const vec1: @Vector(vec_size, f32) = a[i..][0..vec_size].*;
        const vec2: @Vector(vec_size, f32) = b[i..][0..vec_size].*;
        dot_vec = @mulAdd(@Vector(vec_size, f32), vec1, vec2, dot_vec);
        sum_a_vec = @mulAdd(@Vector(vec_size, f32), vec1, vec1, sum_a_vec);
        sum_b_vec = @mulAdd(@Vector(vec_size, f32), vec2, vec2, sum_b_vec);
    }

    var numerator: f32 = @reduce(.Add, dot_vec);
    var sum_a: f32 = @reduce(.Add, sum_a_vec);
    var sum_b: f32 = @reduce(.Add, sum_b_vec);

    while (i < len) : (i += 1) {
        const a_val = a[i];
        const b_val = b[i];
        numerator = @mulAdd(f32, a_val, b_val, numerator);
        sum_a = @mulAdd(f32, a_val, a_val, sum_a);
        sum_b = @mulAdd(f32, b_val, b_val, sum_b);
    }

    const denominator = @sqrt(sum_a) * @sqrt(sum_b);

    if (denominator == 0.0) {
        @branchHint(.unlikely);
        return error.ZeroMagnitude;
    }

    return numerator / denominator;
}

pub fn dot_product(a: []const f32, b: []const f32) !f32 {
    @setFloatMode(.optimized);
    const len = a.len;
    if (len == 0) return error.InvalidInput;
    if (len != b.len) return error.InvalidInput;

    const simd_len = (len / vec_size) * vec_size;
    const unroll_step = vec_size * 2;

    var sum_vec0: @Vector(vec_size, f32) = @splat(0.0);
    var sum_vec1: @Vector(vec_size, f32) = @splat(0.0);
    var i: usize = 0;

    while (i + unroll_step <= simd_len) : (i += unroll_step) {
        const vec1_0: @Vector(vec_size, f32) = a[i..][0..vec_size].*;
        const vec2_0: @Vector(vec_size, f32) = b[i..][0..vec_size].*;
        const vec1_1: @Vector(vec_size, f32) = a[i + vec_size ..][0..vec_size].*;
        const vec2_1: @Vector(vec_size, f32) = b[i + vec_size ..][0..vec_size].*;
        sum_vec0 = @mulAdd(@Vector(vec_size, f32), vec1_0, vec2_0, sum_vec0);
        sum_vec1 = @mulAdd(@Vector(vec_size, f32), vec1_1, vec2_1, sum_vec1);
    }

    var sum_vec = sum_vec0 + sum_vec1;
    while (i < simd_len) : (i += vec_size) {
        const vec1: @Vector(vec_size, f32) = a[i..][0..vec_size].*;
        const vec2: @Vector(vec_size, f32) = b[i..][0..vec_size].*;
        sum_vec = @mulAdd(@Vector(vec_size, f32), vec1, vec2, sum_vec);
    }

    var sum: f32 = @reduce(.Add, sum_vec);
    while (i < len) : (i += 1) {
        sum = @mulAdd(f32, a[i], b[i], sum);
    }

    return sum;
}

test cosine_similarity {
    const a = [_]f32{ 1.0, 2.0, 3.0 };
    const b = [_]f32{ 1.0, 2.0, 3.0 };
    const similarity = cosine_similarity(a[0..], b[0..]) catch unreachable;

    try std.testing.expectEqual(similarity, 0.99999994);
}

test "calculate_cosine_similarity with different vectors" {
    const a = [_]f32{ 1.0, 2.0, 3.0 };
    const b = [_]f32{ 6.0, 3.0, 1.0 };
    const similarity = cosine_similarity(a[0..], b[0..]) catch unreachable;

    try std.testing.expectEqual(similarity, 0.5910828);
}

test dot_product {
    const a = [_]f32{ 1.0, 2.0, 3.0 };
    const b = [_]f32{ 6.0, 3.0, 1.0 };
    const product = dot_product(a[0..], b[0..]) catch unreachable;

    try std.testing.expectEqual(product, 15.0);
}

test "dot_product with exact one SIMD batch" {
    const a = [_]f32{
        1.0,  2.0,  3.0,  4.0,
        5.0,  6.0,  7.0,  8.0,
        9.0,  10.0, 11.0, 12.0,
        13.0, 14.0, 15.0, 16.0,
    };
    const b = [_]f32{
        16.0, 15.0, 14.0, 13.0,
        12.0, 11.0, 10.0, 9.0,
        8.0,  7.0,  6.0,  5.0,
        4.0,  3.0,  2.0,  1.0,
    };
    const product = dot_product(a[0..], b[0..]) catch unreachable;

    try std.testing.expectEqual(product, 816.0);
}

pub fn sq_euclidean_distance(a: []const f32, b: []const f32) !f32 {
    @setFloatMode(.optimized);
    const len = a.len;
    if (len == 0) return error.InvalidInput;
    if (len != b.len) return error.InvalidInput;

    const simd_len = (len / vec_size) * vec_size;
    const unroll_step = vec_size * 2;

    var sum_vec0: @Vector(vec_size, f32) = @splat(0.0);
    var sum_vec1: @Vector(vec_size, f32) = @splat(0.0);
    var i: usize = 0;

    while (i + unroll_step <= simd_len) : (i += unroll_step) {
        const vec1_0: @Vector(vec_size, f32) = a[i..][0..vec_size].*;
        const vec2_0: @Vector(vec_size, f32) = b[i..][0..vec_size].*;
        const vec1_1: @Vector(vec_size, f32) = a[i + vec_size ..][0..vec_size].*;
        const vec2_1: @Vector(vec_size, f32) = b[i + vec_size ..][0..vec_size].*;
        const delta0 = vec1_0 - vec2_0;
        const delta1 = vec1_1 - vec2_1;
        sum_vec0 = @mulAdd(@Vector(vec_size, f32), delta0, delta0, sum_vec0);
        sum_vec1 = @mulAdd(@Vector(vec_size, f32), delta1, delta1, sum_vec1);
    }

    var sum_vec = sum_vec0 + sum_vec1;
    while (i < simd_len) : (i += vec_size) {
        const vec1: @Vector(vec_size, f32) = a[i..][0..vec_size].*;
        const vec2: @Vector(vec_size, f32) = b[i..][0..vec_size].*;
        const delta = vec1 - vec2;
        sum_vec = @mulAdd(@Vector(vec_size, f32), delta, delta, sum_vec);
    }

    var sum: f32 = @reduce(.Add, sum_vec);
    while (i < len) : (i += 1) {
        const delta = a[i] - b[i];
        sum = @mulAdd(f32, delta, delta, sum);
    }

    return sum;
}

pub inline fn euclidean_distance(a: []const f32, b: []const f32) !f32 {
    const sq_dist = try sq_euclidean_distance(a, b);
    return @sqrt(sq_dist);
}

test sq_euclidean_distance {
    const a = [_]f32{ 3.0, 5.0, 7.0 };
    const b = [_]f32{ 1.0, 2.0, 3.0 };
    const distance = sq_euclidean_distance(a[0..], b[0..]) catch unreachable;

    try std.testing.expectEqual(distance, 29.0);
}

test euclidean_distance {
    const a = [_]f32{ 3.0, 5.0, 7.0 };
    const b = [_]f32{ 1.0, 2.0, 3.0 };
    const distance = euclidean_distance(a[0..], b[0..]) catch unreachable;

    try std.testing.expectEqual(distance, 5.3851647);
}
