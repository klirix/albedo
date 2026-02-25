const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});

    const optimize = b.standardOptimizeOption(.{});

    const exe = b.addExecutable(.{
        .name = "benchmark",
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/main.zig"),

            .target = target,
            .optimize = optimize,
        }),
    });

    const sqlite = b.dependency("sqlite", .{
        .target = target,
        .optimize = optimize,
    });
    exe.root_module.addImport("sqlite", sqlite.module("sqlite"));

    const albedo_dep = b.dependency("Albedo", .{
        .target = target,
        // .optimize = optimize,
    });

    const opts = .{ .target = target, .optimize = optimize };
    const zbench_module = b.dependency("zbench", opts);
    exe.root_module.addImport("zbench", zbench_module.module("zbench"));
    exe.root_module.addImport("albedo", albedo_dep.module("albedo"));

    b.installArtifact(exe);

    const run_step = b.step("run", "Run the benchmark");

    const run_cmd = b.addRunArtifact(exe);
    run_step.dependOn(&run_cmd.step);

    run_cmd.step.dependOn(b.getInstallStep());

    if (b.args) |args| {
        run_cmd.addArgs(args);
    }
}
