const std = @import("std");
const builtin = @import("builtin");
const mem = std.mem;

pub const CipherType = enum(u3) {
    none = 0,
    chacha20_poly1305 = 1,
    aes_256_gcm = 2,
};

pub const EncryptionFlags = packed struct {
    encrypted: u1 = 0,
    cipher_type: u3 = 0,
    reserved: u4 = 0,

    pub fn isEncrypted(self: EncryptionFlags) bool {
        return self.encrypted == 1;
    }

    pub fn getCipherType(self: EncryptionFlags) CipherType {
        return @enumFromInt(self.cipher_type);
    }

    pub fn setCipher(self: *EncryptionFlags, cipher: CipherType) void {
        self.encrypted = 1;
        self.cipher_type = @intFromEnum(cipher);
    }

    pub fn toByte(self: EncryptionFlags) u8 {
        return @bitCast(self);
    }

    pub fn fromByte(byte: u8) EncryptionFlags {
        return @bitCast(byte);
    }
};

pub const AUTH_TAG_SIZE = 16;
pub const NONCE_SIZE = 12;
pub const KEY_SIZE = 32;
pub const SALT_SIZE = 16;

/// Encrypts page data using ChaCha20-Poly1305 AEAD cipher
/// Returns the size of encrypted data including auth tag
pub fn encryptPage(
    plaintext: []const u8,
    output: []u8,
    key: [KEY_SIZE]u8,
    nonce: [NONCE_SIZE]u8,
    page_id: u64,
) !usize {
    if (output.len < plaintext.len + AUTH_TAG_SIZE) {
        return error.OutputBufferTooSmall;
    }

    const cipher = std.crypto.aead.chacha_poly.ChaCha20Poly1305;

    // Use page_id as additional authenticated data to prevent page swapping
    var aad: [8]u8 = undefined;
    std.mem.writeInt(u64, &aad, page_id, .little);

    const ciphertext = output[0..plaintext.len];
    const tag = output[plaintext.len .. plaintext.len + AUTH_TAG_SIZE];

    cipher.encrypt(
        ciphertext,
        @ptrCast(tag),
        plaintext,
        &aad,
        nonce,
        key,
    );

    return plaintext.len + AUTH_TAG_SIZE;
}

/// Decrypts page data using ChaCha20-Poly1305 AEAD cipher
/// Returns the size of decrypted data (excludes auth tag)
pub fn decryptPage(
    ciphertext_with_tag: []const u8,
    output: []u8,
    key: [KEY_SIZE]u8,
    nonce: [NONCE_SIZE]u8,
    page_id: u64,
) !usize {
    if (ciphertext_with_tag.len < AUTH_TAG_SIZE) {
        return error.InvalidCiphertext;
    }

    const plaintext_len = ciphertext_with_tag.len - AUTH_TAG_SIZE;
    if (output.len < plaintext_len) {
        return error.OutputBufferTooSmall;
    }

    const cipher = std.crypto.aead.chacha_poly.ChaCha20Poly1305;

    var aad: [8]u8 = undefined;
    std.mem.writeInt(u64, &aad, page_id, .little);

    const ciphertext = ciphertext_with_tag[0..plaintext_len];
    const tag: *const [AUTH_TAG_SIZE]u8 = @ptrCast(ciphertext_with_tag[plaintext_len..].ptr);

    cipher.decrypt(
        output[0..plaintext_len],
        ciphertext,
        tag.*,
        &aad,
        nonce,
        key,
    ) catch return error.AuthenticationFailed;

    return plaintext_len;
}

/// Derives encryption key from passphrase using Argon2id
pub fn deriveKey(
    allocator: mem.Allocator,
    passphrase: []const u8,
    salt: [SALT_SIZE]u8,
) !([KEY_SIZE]u8) {
    var key: [KEY_SIZE]u8 = undefined;

    // Argon2 is intentionally expensive. For unit tests we reduce the cost to
    // keep `zig test` reasonably fast; production builds keep the stronger defaults.
    const params: std.crypto.pwhash.argon2.Params = if (builtin.is_test)
        .{ .t = 1, .m = 8192, .p = 1 } // 1 iteration, ~8MiB, 1 thread
    else
        .{ .t = 3, .m = 65536, .p = 1 }; // 3 iterations, ~64MiB, 1 thread

    try std.crypto.pwhash.argon2.kdf(
        allocator,
        &key,
        passphrase,
        &salt,
        params,
        .argon2id,
    );

    return key;
}

/// Generates a cryptographically secure random nonce
pub fn generateNonce() [NONCE_SIZE]u8 {
    var nonce: [NONCE_SIZE]u8 = undefined;
    std.crypto.random.bytes(&nonce);
    return nonce;
}

/// Generates a cryptographically secure random salt
pub fn generateSalt() [SALT_SIZE]u8 {
    var salt: [SALT_SIZE]u8 = undefined;
    std.crypto.random.bytes(&salt);
    return salt;
}

/// Securely zero out sensitive key material
pub fn zeroKey(key: *[KEY_SIZE]u8) void {
    // Securely zero out memory
    @memset(key, 0);
    // Use volatile access to prevent compiler optimization
    const volatile_ptr: *volatile [KEY_SIZE]u8 = @ptrCast(key);
    _ = volatile_ptr[0];
}

test "encrypt and decrypt page data" {
    const plaintext = "Hello, World! This is a test page.";
    var ciphertext: [plaintext.len + AUTH_TAG_SIZE]u8 = undefined;
    var decrypted: [plaintext.len]u8 = undefined;

    const key: [KEY_SIZE]u8 = [_]u8{0x42} ** KEY_SIZE;
    const nonce = generateNonce();
    const page_id: u64 = 123;

    // Encrypt
    const encrypted_size = try encryptPage(plaintext, &ciphertext, key, nonce, page_id);
    try std.testing.expectEqual(plaintext.len + AUTH_TAG_SIZE, encrypted_size);

    // Decrypt
    const decrypted_size = try decryptPage(ciphertext[0..encrypted_size], &decrypted, key, nonce, page_id);
    try std.testing.expectEqual(plaintext.len, decrypted_size);
    try std.testing.expectEqualSlices(u8, plaintext, decrypted[0..decrypted_size]);
}

test "decryption fails with wrong key" {
    const plaintext = "Secret message";
    var ciphertext: [plaintext.len + AUTH_TAG_SIZE]u8 = undefined;
    var decrypted: [plaintext.len]u8 = undefined;

    const correct_key: [KEY_SIZE]u8 = [_]u8{0x42} ** KEY_SIZE;
    const wrong_key: [KEY_SIZE]u8 = [_]u8{0x43} ** KEY_SIZE;
    const nonce = generateNonce();
    const page_id: u64 = 456;

    const encrypted_size = try encryptPage(plaintext, &ciphertext, correct_key, nonce, page_id);

    // Should fail with wrong key
    try std.testing.expectError(
        error.AuthenticationFailed,
        decryptPage(ciphertext[0..encrypted_size], &decrypted, wrong_key, nonce, page_id),
    );
}

test "decryption fails with tampered ciphertext" {
    const plaintext = "Secret message";
    var ciphertext: [plaintext.len + AUTH_TAG_SIZE]u8 = undefined;
    var decrypted: [plaintext.len]u8 = undefined;

    const key: [KEY_SIZE]u8 = [_]u8{0x42} ** KEY_SIZE;
    const nonce = generateNonce();
    const page_id: u64 = 789;

    const encrypted_size = try encryptPage(plaintext, &ciphertext, key, nonce, page_id);

    // Tamper with ciphertext
    ciphertext[5] ^= 0xFF;

    // Should fail authentication
    try std.testing.expectError(
        error.AuthenticationFailed,
        decryptPage(ciphertext[0..encrypted_size], &decrypted, key, nonce, page_id),
    );
}

test "decryption fails with wrong page_id (AAD)" {
    const plaintext = "Secret message";
    var ciphertext: [plaintext.len + AUTH_TAG_SIZE]u8 = undefined;
    var decrypted: [plaintext.len]u8 = undefined;

    const key: [KEY_SIZE]u8 = [_]u8{0x42} ** KEY_SIZE;
    const nonce = generateNonce();
    const correct_page_id: u64 = 100;
    const wrong_page_id: u64 = 200;

    const encrypted_size = try encryptPage(plaintext, &ciphertext, key, nonce, correct_page_id);

    // Should fail with different page_id (prevents page swapping)
    try std.testing.expectError(
        error.AuthenticationFailed,
        decryptPage(ciphertext[0..encrypted_size], &decrypted, key, nonce, wrong_page_id),
    );
}

test "derive key from passphrase" {
    const allocator = std.testing.allocator;
    const passphrase = "my-secret-passphrase";
    const salt = generateSalt();

    const key1 = try deriveKey(allocator, passphrase, salt);
    const key2 = try deriveKey(allocator, passphrase, salt);

    // Same passphrase and salt should produce same key
    try std.testing.expectEqualSlices(u8, &key1, &key2);
}

test "derive key with different salts produces different keys" {
    const allocator = std.testing.allocator;
    const passphrase = "my-secret-passphrase";
    const salt1 = generateSalt();
    const salt2 = generateSalt();

    const key1 = try deriveKey(allocator, passphrase, salt1);
    const key2 = try deriveKey(allocator, passphrase, salt2);

    // Different salts should produce different keys
    try std.testing.expect(!std.mem.eql(u8, &key1, &key2));
}

test "EncryptionFlags bit manipulation" {
    var flags = EncryptionFlags{};
    try std.testing.expect(!flags.isEncrypted());
    try std.testing.expectEqual(CipherType.none, flags.getCipherType());

    flags.setCipher(.chacha20_poly1305);
    try std.testing.expect(flags.isEncrypted());
    try std.testing.expectEqual(CipherType.chacha20_poly1305, flags.getCipherType());

    // Test byte conversion
    const byte = flags.toByte();
    const restored = EncryptionFlags.fromByte(byte);
    try std.testing.expect(restored.isEncrypted());
    try std.testing.expectEqual(CipherType.chacha20_poly1305, restored.getCipherType());
}
