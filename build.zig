const std = @import("std");
const napigen = @import("napigen");
const lib_mod = @import("./src/lib.zig");

// Although this function looks imperative, note that its job is to
// declaratively construct a build graph that will be executed by an external
// runner.

// ponytail: API level pinned to 35 (Android 15). Add a -D option if a consumer needs to lower it.
const ANDROID_API_LEVEL = "35";

const AndroidPaths = struct {
    include_dir: []const u8,
    lib_dir: []const u8,
};

fn androidFail(comptime fmt: []const u8, args: anytype) noreturn {
    std.debug.print("albedo build: " ++ fmt ++ "\n", args);
    std.process.exit(1);
}

// Resolves NDK sysroot paths from -DandroidSDK=<path>. Picks the newest
// installed NDK under $sdk/ndk/* (lexicographic max == highest version).
fn resolveAndroidPaths(b: *std.Build, raw_sdk: []const u8) AndroidPaths {
    // ponytail: expand leading ~ to $HOME — zsh/bash don't do this after `=`,
    // and `-DandroidSDK=~/...` is what users naturally type.
    const sdk = if (std.mem.startsWith(u8, raw_sdk, "~/")) blk: {
        const home = b.graph.environ_map.get("HOME") orelse
            androidFail("could not read $HOME to expand {s}", .{raw_sdk});
        break :blk b.fmt("{s}/{s}", .{ home, raw_sdk[2..] });
    } else raw_sdk;

    const host_tag = switch (b.graph.host.result.os.tag) {
        .macos => "darwin-x86_64", // works on arm64 via Rosetta; every NDK ships this tag
        .linux => "linux-x86_64",
        .windows => "windows-x86_64",
        else => androidFail("unsupported host OS for Android build: {s}", .{@tagName(b.graph.host.result.os.tag)}),
    };

    const ndk_dir_path = b.fmt("{s}/ndk", .{sdk});
    const io = b.graph.io;
    var ndk_dir = std.Io.Dir.cwd().openDir(io, ndk_dir_path, .{ .iterate = true }) catch
        androidFail("cannot open {s} — is -DandroidSDK pointing at a valid SDK root?", .{ndk_dir_path});
    defer ndk_dir.close(io);

    var best: []const u8 = "";
    var it = ndk_dir.iterate();
    while (it.next(io) catch null) |entry| {
        if (entry.kind != .directory) continue;
        if (std.mem.order(u8, best, entry.name) == .lt) {
            best = b.allocator.dupe(u8, entry.name) catch unreachable;
        }
    }
    if (best.len == 0) androidFail("no NDK found under {s}", .{ndk_dir_path});

    const sysroot = b.fmt("{s}/{s}/toolchains/llvm/prebuilt/{s}/sysroot/usr", .{ ndk_dir_path, best, host_tag });
    return .{
        .include_dir = b.fmt("{s}/include", .{sysroot}),
        .lib_dir = b.fmt("{s}/lib", .{sysroot}),
    };
}

fn createLibModule(
    b: *std.Build,
    target: std.Build.ResolvedTarget,
    optimize: std.builtin.OptimizeMode,
    isAndroid: bool,
    isWasm: bool,
) *std.Build.Module {
    const libModule = b.createModule(.{
        .root_source_file = b.path("src/lib.zig"),
        .target = target,
        .optimize = optimize,
    });

    const buildOptions = b.addOptions();
    buildOptions.addOption(bool, "isAndroid", isAndroid);
    buildOptions.addOption(bool, "isWasm", isWasm);
    libModule.addOptions("build_options", buildOptions);

    return libModule;
}

fn buildWasmTarget(b: *std.Build, libModule: *std.Build.Module) void {
    libModule.single_threaded = true;
    libModule.link_libc = false;
    libModule.export_symbol_names = &[_][]const u8{
        "albedo_open",
        "albedo_close",
        "albedo_insert",
        "albedo_transaction_begin",
        "albedo_transaction_insert",
        "albedo_transaction_delete",
        "albedo_transaction_transform",
        "albedo_transaction_commit",
        "albedo_transaction_rollback",
        "albedo_transaction_close",
        "albedo_ensure_index",
        "albedo_drop_index",
        "albedo_delete",
        "albedo_list",
        "albedo_data",
        "albedo_close_iterator",
        "albedo_vacuum",
        "albedo_version",
        "albedo_malloc",
        "albedo_free",
        "albedo_transform",
        "albedo_transform_close",
        "albedo_transform_data",
        "albedo_transform_apply",
        "albedo_bitsize",
    };

    const wasm_module = b.addExecutable(.{
        .name = "albedo",
        .linkage = .static,
        .root_module = libModule,
    });

    wasm_module.entry = .disabled;
    wasm_module.export_table = true;

    b.installArtifact(wasm_module);
}

fn configureAndroidDynamic(b: *std.Build, libModule: *std.Build.Module, dynamic: *std.Build.Step.Compile, arch: []const u8, paths: AndroidPaths) void {
    dynamic.root_module.link_libc = true;
    dynamic.link_z_max_page_size = 16 << 10;
    dynamic.link_z_common_page_size = 16 << 10;
    dynamic.link_emit_relocs = true;
    dynamic.link_eh_frame_hdr = true;
    dynamic.link_function_sections = true;
    dynamic.bundle_compiler_rt = true;

    const libs = [_][]const u8{ "GLESv2", "EGL", "android", "log", "aaudio" };
    for (libs) |lib| {
        libModule.linkSystemLibrary(lib, .{ .weak = true });
    }

    libModule.addIncludePath(.{ .cwd_relative = paths.include_dir });
    libModule.addLibraryPath(.{ .cwd_relative = b.fmt("{s}/{s}/{s}", .{ paths.lib_dir, arch, ANDROID_API_LEVEL }) });
    libModule.addLibraryPath(.{ .cwd_relative = b.fmt("{s}/{s}", .{ paths.lib_dir, arch }) });
    dynamic.export_table = true;

    dynamic.setLibCFile(createLibCFile(b, arch, paths));

    if (dynamic.root_module.resolved_target.?.result.cpu.arch == .x86) {
        dynamic.link_z_notext = true;
    }
}

fn buildSharedLibrary(b: *std.Build, libModule: *std.Build.Module, target: std.Build.ResolvedTarget, isAndroid: bool, android_paths: ?AndroidPaths) void {
    const dynamic = b.addLibrary(.{
        .name = "albedo",
        .linkage = .dynamic,
        .root_module = libModule,
    });

    dynamic.headerpad_max_install_names = true;

    const arch = switch (target.result.cpu.arch) {
        .x86_64 => "x86_64-linux-android",
        .aarch64 => "aarch64-linux-android",
        .arm => "arm-linux-androideabi",
        else => @panic("Unsupported architecture"),
    };

    if (isAndroid) {
        configureAndroidDynamic(b, libModule, dynamic, arch, android_paths.?);
    }

    b.installArtifact(dynamic);
}

fn buildStaticLibrary(b: *std.Build, libModule: *std.Build.Module, outputName: ?[]const u8) void {
    const static = b.addLibrary(.{
        .name = outputName orelse "albedo",
        .linkage = .static,
        .root_module = libModule,
    });

    libModule.strip = true;
    libModule.unwind_tables = .none;
    libModule.omit_frame_pointer = true;

    b.installArtifact(static);
}

fn buildClientTarget(
    b: *std.Build,
    libModule: *std.Build.Module,
    target: std.Build.ResolvedTarget,
    optimize: std.builtin.OptimizeMode,
) void {
    const clientModule = b.createModule(.{
        .root_source_file = b.path("src/client.zig"),
        .target = target,
        .optimize = optimize,
    });

    clientModule.addImport("albedo", libModule);

    const client_exe = b.addExecutable(.{
        .name = "albedo-cli",
        .root_module = clientModule,
    });

    b.installArtifact(client_exe);

    const run_cmd = b.addRunArtifact(client_exe);
    run_cmd.step.dependOn(b.getInstallStep());

    if (b.args) |args| {
        run_cmd.addArgs(args);
    }

    const run_step = b.step("run", "Run the CLI client");
    run_step.dependOn(&run_cmd.step);
}

fn addTests(b: *std.Build, libModule: *std.Build.Module) void {
    const lib_unit_tests = b.addTest(.{ .root_module = libModule });
    const run_lib_unit_tests = b.addRunArtifact(lib_unit_tests);

    const test_step = b.step("test", "Run unit tests");
    test_step.dependOn(&run_lib_unit_tests.step);
}

// Generates a libc conf file into Zig's build cache (no committed per-machine paths).
fn createLibCFile(b: *std.Build, arch: []const u8, paths: AndroidPaths) std.Build.LazyPath {
    // include_dir: directory holding `stdlib.h`.
    // sys_include_dir: directory holding `sys/errno.h` (arch-specific on Android).
    const content = b.fmt(
        "include_dir={s}\n" ++
        "sys_include_dir={s}/{s}\n" ++
        "crt_dir={s}/{s}/{s}\n" ++
        "msvc_lib_dir=\n" ++
        "kernel32_lib_dir=\n" ++
        "gcc_dir=\n",
        .{ paths.include_dir, paths.include_dir, arch, paths.lib_dir, arch, ANDROID_API_LEVEL },
    );

    return b.addWriteFiles().add(b.fmt("{s}.conf", .{arch}), content);
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

    _ = b.addModule("albedo", .{
        .root_source_file = b.path("src/albedo.zig"),
        .target = target,
        .optimize = optimize,
    });

    const buildStatic = b.option(bool, "static", "Build static library") orelse false;
    // const buildNode = b.option(bool, "node", "Build node extension") orelse false;
    // const buildClient = b.option(bool, "client", "Build CLI client") orelse true;
    const outputName = b.option([]const u8, "output", "Output file name");

    const isAndroid = target.result.abi == .android;
    const isWasm = target.result.cpu.arch == .wasm32 or target.result.cpu.arch == .wasm64;

    const android_sdk = b.option([]const u8, "androidSDK", "Path to Android SDK root (required for -Dtarget=*-android)") orelse "";
    if (isAndroid and android_sdk.len == 0) {
        androidFail("-DandroidSDK=<path> is required when target is Android (e.g. -DandroidSDK=~/Library/Android/sdk)", .{});
    }
    const android_paths: ?AndroidPaths = if (isAndroid) resolveAndroidPaths(b, android_sdk) else null;

    const libModule = createLibModule(b, target, optimize, isAndroid, isWasm);

    if (isWasm) {
        buildWasmTarget(b, libModule);
    } else {
        if (!buildStatic) {
            buildSharedLibrary(b, libModule, target, isAndroid, android_paths);
        } else {
            buildStaticLibrary(b, libModule, outputName);
        }
    }

    addTests(b, libModule);
}
