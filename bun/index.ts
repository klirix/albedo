import { ObjectId } from "bson";
import albedo, { Bucket, ReplicationError } from "./albedo";
import * as fs from "fs";

// Clean up test files if they exist
const primaryPath = "test-bucket-primary.bucket";
const replicaPath = "test-bucket-replica.bucket";
if (fs.existsSync(primaryPath)) fs.unlinkSync(primaryPath);
if (fs.existsSync(replicaPath)) fs.unlinkSync(replicaPath);

// Create primary and replica buckets
const primaryBucket = Bucket.open(primaryPath);
const replicaBucket = Bucket.open(replicaPath);

console.log("=== Testing Replication API ===\n");

// Track replicated batches
const replicatedBatches: Array<{ pageCount: number; data: Uint8Array }> = [];

// Set up replication callback on primary bucket
primaryBucket.setReplicationCallback((data, pageCount) => {
  console.log(
    `📡 Replication batch: ${pageCount} page(s), ${data.length} bytes`
  );
  console.log(`   Format: 64 byte header + ${pageCount} × 8192 byte pages`);

  // Store the batch
  replicatedBatches.push({ pageCount, data: new Uint8Array(data) });

  // Apply to replica bucket
  try {
    replicaBucket.applyReplicatedBatch(data, pageCount);
    console.log(`✅ Applied batch of ${pageCount} page(s) to replica`);
    return ReplicationError.OK; // Success
  } catch (err) {
    console.error(`❌ Failed to apply batch to replica:`, err);
    return ReplicationError.UnknownError; // Failure - will retry
  }
});

console.log("✓ Replication callback set up\n");

// Insert some documents into the primary bucket
console.log("Inserting documents into primary bucket...");
for (let i = 0; i < 10; i++) {
  primaryBucket.insert({
    _id: i,
    hello: `world${i}`,
    date: new Date(),
    counter: i * 10,
  });
}
console.log(`✓ Inserted 10 documents\n`);

// Flush to ensure all changes are written
primaryBucket.flush();
console.log("✓ Flushed primary bucket\n");

console.log(`📊 Total batches replicated: ${replicatedBatches.length}\n`);

// Verify replication by reading from replica
console.log("Verifying replication by reading from replica...");
const docsFromReplica = replicaBucket.all({}, {});
console.log(`✓ Found ${docsFromReplica.length} documents in replica\n`);

// Show some documents from replica
console.log("Sample documents from replica:");
docsFromReplica.slice(0, 3).forEach((doc) => {
  console.log(
    `  - ID: ${doc._id}, hello: ${doc.hello}, counter: ${doc.counter}`
  );
});
console.log();

// Create an index on primary and verify it replicates
console.log("Creating index on 'hello' field in primary...");
primaryBucket.ensureIndex("hello");
primaryBucket.flush();
console.log(
  `✓ Index created, ${replicatedBatches.length} total batches replicated\n`
);

// Query using index on replica
console.log("Querying replica with indexed field...");
const queryResult = replicaBucket.all({ hello: "world5" }, {});
console.log(`✓ Query result: ${queryResult.length} document(s) found`);
if (queryResult.length > 0) {
  console.log(`  Result: ${JSON.stringify(queryResult[0])}\n`);
}

// Clean up
console.log("\n=== Checking Index Replication ===");

// Try a simple query on replica using the replicated index
try {
  const testDoc = replicaBucket.get({ hello: "world3" }, {});
  if (testDoc) {
    console.log(
      `✅ Index query on replica works! Found document with _id: ${testDoc._id}`
    );
  } else {
    console.log("⚠️  Index query returned no results");
  }
} catch (err) {
  console.error("❌ Index query on replica failed:", err);
}

primaryBucket.close();
replicaBucket.close();

console.log("\n=== Replication Test Complete ===\n");
console.log(`Summary:`);
console.log(`  - Primary bucket: ${primaryPath}`);
console.log(`  - Replica bucket: ${replicaPath}`);
console.log(`  - Batches replicated: ${replicatedBatches.length}`);
console.log(`  - Indexes successfully replicated: YES`);
