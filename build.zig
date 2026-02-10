const std = @import("std");
const napigen = @import("napigen");
const lib_mod = @import("./src/lib.zig");

// Although this function looks imperative, note that its job is to
// declaratively construct a build graph that will be executed by an external
// runner.

const ndkBase = "/Users/askhat/Library/Android/sdk/ndk/27.0.12077973/toolchains/llvm/prebuilt/darwin-x86_64/sysroot/usr";
const include_dir = ndkBase ++ "/include";
const lib_dir = ndkBase ++ "/lib";

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
        "albedo_ensure_index",
        "albedo_drop_index",
        "albedo_delete",
        "albedo_list",
        "albedo_data",
        "albedo_next",
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

fn configureAndroidDynamic(b: *std.Build, dynamic: *std.Build.Step.Compile, arch: []const u8) void {
    dynamic.root_module.link_libc = true;
    dynamic.link_z_max_page_size = 16 << 10;
    dynamic.link_z_common_page_size = 16 << 10;
    dynamic.link_emit_relocs = true;
    dynamic.link_eh_frame_hdr = true;
    dynamic.link_function_sections = true;
    dynamic.bundle_compiler_rt = true;

    const libs = [_][]const u8{ "GLESv2", "EGL", "android", "log", "aaudio" };
    for (libs) |lib| {
        dynamic.linkSystemLibrary2(lib, .{ .weak = true });
    }

    dynamic.addIncludePath(.{ .cwd_relative = b.fmt("{s}", .{include_dir}) });
    dynamic.addLibraryPath(.{ .cwd_relative = b.fmt("{s}/{s}/35", .{ lib_dir, arch }) });
    dynamic.export_table = true;
    dynamic.addLibraryPath(.{ .cwd_relative = b.fmt("{s}/{s}", .{ lib_dir, arch }) });
    dynamic.setLibCFile(.{ .cwd_relative = b.fmt("android-confs/{s}.conf", .{arch}) });

    dynamic.libc_file.?.addStepDependencies(&dynamic.step);

    if (dynamic.root_module.resolved_target.?.result.cpu.arch == .x86) {
        dynamic.link_z_notext = true;
    }
}

fn buildSharedLibrary(b: *std.Build, libModule: *std.Build.Module, target: std.Build.ResolvedTarget, isAndroid: bool) void {
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
        configureAndroidDynamic(b, dynamic, arch);
    }

    b.installArtifact(dynamic);
}

fn buildStaticLibrary(b: *std.Build, libModule: *std.Build.Module, outputName: ?[]const u8) void {
    const static = b.addLibrary(.{
        .name = outputName orelse "albedo",
        .linkage = .static,
        .root_module = libModule,
    });

    static.bundle_compiler_rt = true;

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

fn createLibCFile(b: *std.Build, arch: []const u8) ![]const u8 {
    const fname = b.fmt("android-{s}.conf", .{arch});

    var contents = std.ArrayList(u8);
    errdefer contents.deinit();

    var writer = contents.writer(b.allocator);

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

    const buildStatic = b.option(bool, "static", "Build static library") orelse false;
    // const buildNode = b.option(bool, "node", "Build node extension") orelse false;
    // const buildClient = b.option(bool, "client", "Build CLI client") orelse true;
    const outputName = b.option([]const u8, "output", "Output file name");

    const isAndroid = target.result.abi == .android;
    const isWasm = target.result.cpu.arch == .wasm32 or target.result.cpu.arch == .wasm64;

    const libModule = createLibModule(b, target, optimize, isAndroid, isWasm);

    if (isWasm) {
        buildWasmTarget(b, libModule);
    } else {
        if (!buildStatic) {
            buildSharedLibrary(b, libModule, target, isAndroid);
        } else {
            buildStaticLibrary(b, libModule, outputName);
        }
    }

    addTests(b, libModule);
}
