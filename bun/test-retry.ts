import { Bucket, ReplicationError } from "./albedo";
import * as fs from "fs";

// Clean up test files if they exist
const primaryPath = "test-retry-primary.bucket";
const replicaPath = "test-retry-replica.bucket";
if (fs.existsSync(primaryPath)) fs.unlinkSync(primaryPath);
if (fs.existsSync(replicaPath)) fs.unlinkSync(replicaPath);

// Create primary and replica buckets
const primaryBucket = Bucket.open(primaryPath);
const replicaBucket = Bucket.open(replicaPath);

console.log("=== Replication Retry Test ===\n");

let attemptCount = 0;
const maxFailures = 2; // Fail first 2 attempts, succeed on 3rd

// Set up replication callback that fails initially then succeeds
primaryBucket.setReplicationCallback((data, pageCount) => {
  attemptCount++;
  console.log(`📡 Replication attempt #${attemptCount}: ${pageCount} page(s)`);

  if (attemptCount <= maxFailures) {
    console.log(
      `   ❌ Simulating failure (attempt ${attemptCount}/${maxFailures})`
    );
    return ReplicationError.NetworkError; // Simulate network error
  }

  // After maxFailures attempts, succeed
  console.log(`   ✅ Attempting replication (attempt ${attemptCount})`);
  try {
    replicaBucket.applyReplicatedBatch(data, pageCount);
    console.log(`   ✅ Success!\n`);
    return ReplicationError.OK;
  } catch (err) {
    console.error(`   ❌ Unexpected error:`, err);
    return ReplicationError.UnknownError;
  }
});

console.log("✓ Replication callback set up (will fail first 2 attempts)\n");

// Insert some documents
console.log("Inserting 3 documents...");
for (let i = 0; i < 3; i++) {
  primaryBucket.insert({ _id: i, value: `test${i}` });
}
console.log("✓ Inserted\n");

// First flush - should fail
console.log("Flush #1 (should fail):");
primaryBucket.flush();

// Second flush - should retry and fail again
console.log("\nFlush #2 (should retry previous batch and fail again):");
primaryBucket.flush();

// Third flush - should retry and succeed
console.log("\nFlush #3 (should retry previous batch and succeed):");
primaryBucket.flush();

// Verify replication
console.log("\n=== Verification ===");
const primaryDocs = primaryBucket.all({}, {});
const replicaDocs = replicaBucket.all({}, {});

console.log(`Primary documents: ${primaryDocs.length}`);
console.log(`Replica documents: ${replicaDocs.length}`);

if (primaryDocs.length === replicaDocs.length && replicaDocs.length === 3) {
  console.log("\n✅ SUCCESS: Retry mechanism worked!");
  console.log(`   Total attempts: ${attemptCount}`);
  console.log(
    `   Expected: ${
      maxFailures + 1
    } (failed ${maxFailures} times, succeeded once)`
  );
} else {
  console.log("\n❌ FAIL: Document counts don't match");
}

primaryBucket.close();
replicaBucket.close();
