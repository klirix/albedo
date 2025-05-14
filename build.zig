const std = @import("std");
const napigen = @import("napigen");

// Although this function looks imperative, note that its job is to
// declaratively construct a build graph that will be executed by an external
// runner.

const ndkBase = "/Users/askhat/Library/Android/sdk/ndk/27.0.12077973/toolchains/llvm/prebuilt/darwin-x86_64/sysroot/usr";
const include_dir = ndkBase ++ "/include";
const lib_dir = ndkBase ++ "/lib";

fn createLibCFile(b: *std.Build, arch: []const u8) ![]const u8 {
    const fname = b.fmt("android-{s}.conf", .{arch});

    var contents = std.ArrayList(u8).init(b.allocator);
    errdefer contents.deinit();

    var writer = contents.writer();

    //  The directory that contains `stdlib.h`.
    //  On POSIX-like systems, include directories be found with: `cc -E -Wp,-v -xc /dev/null
    try writer.print("include_dir={s}\n", .{include_dir});

    // The system-specific include directory. May be the same as `include_dir`.
    // On Windows it's the directory that includes `vcruntime.h`.
    // On POSIX it's the directory that includes `sys/errno.h`.
    try writer.print("sys_include_dir={s}/{s}\n", .{ include_dir, arch });

    try writer.print("crt_dir={s}/{s}\n", .{ lib_dir, arch });
    try writer.writeAll("msvc_lib_dir=\n");
    try writer.writeAll("kernel32_lib_dir=\n");
    try writer.writeAll("gcc_dir=\n");

    const step = b.addWriteFile(fname, contents.items);
    b.getInstallStep().dependOn(&step.step);
    return step.files.items[0].sub_path;
}

pub fn build(b: *std.Build) void {
    // Standard target options allows the person running `zig build` to choose
    // what target to build for. Here we do not override the defaults, which
    // means any target is allowed, and the default is native. Other options
    // for restricting supported target set are available.
    const target = b.standardTargetOptions(.{});

    // Standard optimization options allow the person running `zig build` to select
    // between Debug, ReleaseSafe, ReleaseFast, and ReleaseSmall. Here we do not
    // set a preferred release mode, allowing the user to decide how to optimize.
    const optimize = b.standardOptimizeOption(.{
        .preferred_optimize_mode = .ReleaseFast,
    });

    const buildStatic = b.option(bool, "static", "Build static library");
    const buildNode = b.option(bool, "node", "Build node extension");
    // if (b.)

    const libModule = b.createModule(.{
        .root_source_file = b.path("src/lib.zig"),
        .target = target,
        .optimize = optimize,
    });

    const isAndroid = b.option(bool, "android", "Build with android libc");

    if (buildStatic != true and buildNode != true) {
        // Build a shared library by default

        const dynamic = b.addLibrary(.{
            .name = "albedo",
            .linkage = .dynamic,
            .root_module = libModule,
        });

        const arch = switch (target.result.cpu.arch) {
            .x86_64 => "x86_64-linux-android",
            .aarch64 => "aarch64-linux-android",
            .arm => "arm-linux-androideabi",
            else => @panic("Unsupported architecture"),
        };

        if (isAndroid == true) {
            dynamic.linkLibC();
            dynamic.link_emit_relocs = true;
            dynamic.link_eh_frame_hdr = true;
            dynamic.link_function_sections = true;
            dynamic.bundle_compiler_rt = true;
            // dynamic.strip = (mode == .ReleaseSmall);
            const libs = [_][]const u8{ "GLESv2", "EGL", "android", "log", "aaudio" };
            for (libs) |lib| {
                dynamic.linkSystemLibrary2(lib, .{ .weak = true });
            }

            dynamic.addIncludePath(.{ .cwd_relative = b.fmt("{s}", .{include_dir}) });
            dynamic.addLibraryPath(.{
                .cwd_relative = b.fmt("{s}/{s}/35", .{ lib_dir, arch }),
            });
            dynamic.export_table = true;
            dynamic.addLibraryPath(.{ .cwd_relative = b.fmt("{s}/{s}", .{ lib_dir, arch }) });
            dynamic.setLibCFile(.{ .cwd_relative = b.fmt("android-confs/{s}.conf", .{arch}) });

            dynamic.libc_file.?.addStepDependencies(&dynamic.step);

            if (target.result.cpu.arch == .x86) {
                dynamic.link_z_notext = true;
            }
        }

        b.installArtifact(dynamic);
    }

    const nodeModule = b.createModule(.{
        .root_source_file = b.path("src/napi.zig"),
        .target = target,
        .optimize = optimize,
    });

    const simple_module = b.dependency("napigen", .{
        .target = target,
        .optimize = optimize,
    });
    libModule.addImport("napigen", simple_module.module("napigen"));

    if (buildNode == true) {
        const node_lib = b.addSharedLibrary(.{
            .name = "albedo_node",
            // .linkage = .dynamic,
            .root_module = nodeModule,
        });

        napigen.setup(node_lib);

        b.installArtifact(node_lib);

        const copy_node_step = b.addInstallLibFile(node_lib.getEmittedBin(), "libalbedo.node");
        b.getInstallStep().dependOn(&copy_node_step.step);
    }

    const d = b.option([]const u8, "output", "Output file name");

    if (buildStatic == true) {
        const static = b.addLibrary(.{
            .name = d orelse "albedo",
            .linkage = .static,

            .root_module = libModule,
        });

        static.bundle_compiler_rt = true;

        b.installArtifact(static);
    }

    const lib_unit_tests = b.addTest(.{
        .root_source_file = b.path("src/lib.zig"),
        .target = target,
        .optimize = optimize,
    });

    const run_lib_unit_tests = b.addRunArtifact(lib_unit_tests);

    const exe_unit_tests = b.addTest(.{
        .root_source_file = b.path("src/main.zig"),
        .target = target,
        .optimize = optimize,
    });

    const run_exe_unit_tests = b.addRunArtifact(exe_unit_tests);

    // Similar to creating the run step earlier, this exposes a `test` step to
    // the `zig build --help` menu, providing a way for the user to request
    // running the unit tests.
    const test_step = b.step("test", "Run unit tests");
    test_step.dependOn(&run_lib_unit_tests.step);
    test_step.dependOn(&run_exe_unit_tests.step);
}
