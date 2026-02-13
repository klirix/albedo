const std = @import("std");

pub const Argon2 = std.crypto.pwhash.argon2;
pub const Aes256Gcm = std.crypto.aead.aes_gcm.Aes256Gcm;
const Sha256 = std.crypto.hash.sha2.Sha256;

pub const FLAG_ENCRYPTED: u8 = 1 << 0;
pub const FLAG_KDF_ARGON2ID_V1: u8 = 1 << 1;
pub const FLAG_CIPHER_AES256_GCM_V1: u8 = 1 << 2;

pub const KEY_BYTES = Aes256Gcm.key_length;
pub const SALT_BYTES = 16;
pub const KEY_CHECK_BYTES = 12;

pub const PAGE_COUNTER_MAX: u32 = 0x000F_FFFF;
pub const PAGE_META_VERSION: u8 = 1;
pub const PAGE_COUNTER_DEFAULT: u32 = 1;
pub const PAGE_COUNTER_BYTES = 3;
pub const PAGE_TAG_BYTES = Aes256Gcm.tag_length;
pub const PAGE_NONCE_BYTES = Aes256Gcm.nonce_length;
pub const PAGE_HEADER_ENCRYPTION_BYTES = 19;
pub const PAGE_OFFSET_COUNTER = 0;
pub const PAGE_OFFSET_TAG = PAGE_COUNTER_BYTES;

pub const BUCKET_RESERVED_OFFSET_SALT = 0;
pub const BUCKET_RESERVED_OFFSET_KEY_CHECK = BUCKET_RESERVED_OFFSET_SALT + SALT_BYTES;

pub const Context = struct {
    key: ?[KEY_BYTES]u8 = null,
    salt: [SALT_BYTES]u8 = [_]u8{0} ** SALT_BYTES,

    pub fn clear(self: *Context) void {
        if (self.key) |*key| {
            std.crypto.secureZero(u8, key);
        }
        self.key = null;
        @memset(self.salt[0..], 0);
    }
};

pub const PageMeta = struct {
    counter: u32,
    tag: [Aes256Gcm.tag_length]u8,
};

pub fn derivePageNonce(salt: [SALT_BYTES]u8, page_id: u64, counter: u32) [Aes256Gcm.nonce_length]u8 {
    var material: [SALT_BYTES + @sizeOf(u64) + @sizeOf(u32)]u8 = undefined;
    @memcpy(material[0..SALT_BYTES], &salt);
    std.mem.writeInt(u64, material[SALT_BYTES .. SALT_BYTES + @sizeOf(u64)], page_id, .little);
    std.mem.writeInt(u32, material[SALT_BYTES + @sizeOf(u64) ..], counter, .little);

    var digest: [Sha256.digest_length]u8 = undefined;
    Sha256.hash(&material, &digest, .{});

    var nonce: [Aes256Gcm.nonce_length]u8 = undefined;
    @memcpy(nonce[0..], digest[0..Aes256Gcm.nonce_length]);
    return nonce;
}

pub fn computeBucketKeyCheck(key: [KEY_BYTES]u8) [KEY_CHECK_BYTES]u8 {
    const marker = "albedo-key-check-v1";
    var material: [KEY_BYTES + marker.len]u8 = undefined;
    @memcpy(material[0..KEY_BYTES], &key);
    @memcpy(material[KEY_BYTES..], marker);

    var digest: [Sha256.digest_length]u8 = undefined;
    Sha256.hash(&material, &digest, .{});

    var out: [KEY_CHECK_BYTES]u8 = undefined;
    @memcpy(out[0..], digest[0..KEY_CHECK_BYTES]);
    return out;
}

pub fn deriveBucketKey(allocator: std.mem.Allocator, password: []const u8, salt: [SALT_BYTES]u8) ![KEY_BYTES]u8 {
    var key: [KEY_BYTES]u8 = undefined;
    const params = Argon2.Params{ .t = 3, .m = 4096, .p = 1 };
    try Argon2.kdf(allocator, &key, password, &salt, params, .argon2id);
    return key;
}

pub fn encodePageMeta(header_encryption: *[PAGE_HEADER_ENCRYPTION_BYTES]u8, counter: u32, tag: [Aes256Gcm.tag_length]u8) void {
    var normalized_counter = counter;
    if (normalized_counter == 0) normalized_counter = PAGE_COUNTER_DEFAULT;
    if (normalized_counter > PAGE_COUNTER_MAX) {
        normalized_counter = PAGE_COUNTER_DEFAULT;
    }

    @memset(header_encryption[0..], 0);
    header_encryption[PAGE_OFFSET_COUNTER] = (@as(u8, PAGE_META_VERSION) << 4) | @as(u8, @truncate((normalized_counter >> 16) & 0x0F));
    header_encryption[PAGE_OFFSET_COUNTER + 1] = @truncate((normalized_counter >> 8) & 0xFF);
    header_encryption[PAGE_OFFSET_COUNTER + 2] = @truncate(normalized_counter & 0xFF);
    @memcpy(
        header_encryption[PAGE_OFFSET_TAG .. PAGE_OFFSET_TAG + PAGE_TAG_BYTES],
        &tag,
    );
}

pub fn decodePageMeta(header_encryption: *const [PAGE_HEADER_ENCRYPTION_BYTES]u8) !PageMeta {
    const version = header_encryption[0] >> 4;
    if (version != PAGE_META_VERSION) {
        return error.InvalidPageEncryptionMetadata;
    }

    const high_nibble: u32 = @intCast(header_encryption[0] & 0x0F);
    const mid: u32 = @intCast(header_encryption[1]);
    const low: u32 = @intCast(header_encryption[2]);
    const counter = (high_nibble << 16) | (mid << 8) | low;
    if (counter == 0) {
        return error.InvalidPageEncryptionMetadata;
    }

    var tag: [Aes256Gcm.tag_length]u8 = undefined;
    @memcpy(
        tag[0..],
        header_encryption[PAGE_OFFSET_TAG .. PAGE_OFFSET_TAG + PAGE_TAG_BYTES],
    );

    return .{ .counter = counter, .tag = tag };
}
