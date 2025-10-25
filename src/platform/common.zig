pub const PlatformError = error{
    FileNotFound,
    PermissionDenied,
    AlreadyExists,
    Unexpected,
};

pub const FileOptions = struct {
    read: bool = false,
    write: bool = false,
    create: bool = false,
    truncate: bool = false,
};

pub const PlatformFileReadError = error{ClosedHandle} || PlatformError;
pub const PlatformFileWriteError = error{ClosedHandle} || PlatformError;
pub const PlatformFileSyncError = error{ClosedHandle} || PlatformError;
pub const PlatformOpenError = PlatformError || error{OutOfMemory};
