import { Bucket, ReplicationError } from "./albedo";
import * as fs from "fs";

// Clean up test files if they exist
const primaryPath = "test-simple-primary.bucket";
const replicaPath = "test-simple-replica.bucket";
if (fs.existsSync(primaryPath)) fs.unlinkSync(primaryPath);
if (fs.existsSync(replicaPath)) fs.unlinkSync(replicaPath);

// Create primary and replica buckets
const primaryBucket = Bucket.open(primaryPath);
const replicaBucket = Bucket.open(replicaPath);

console.log("=== Simple Replication Test (No Indexes) ===\n");

// Set up replication callback on primary bucket
primaryBucket.setReplicationCallback((data, pageCount) => {
  console.log(
    `📡 Replicating batch: ${pageCount} page(s), ${data.length} bytes`
  );

  // Parse and enumerate the pages in the batch
  const headerSize = 64; // BucketHeader.byteSize
  const pageSize = 8192; // DEFAULT_PAGE_SIZE
  const pageHeaderSize = 32; // PageHeader.byteSize

  console.log(`   Pages in batch:`);
  for (let i = 0; i < pageCount; i++) {
    const pageOffset = headerSize + i * pageSize;
    // Read page_id from the page header at offset 3 (after page_type:u8 and used_size:u16)
    const pageIdView = new DataView(
      data.buffer,
      data.byteOffset + pageOffset + 3,
      8
    );
    const pageId = pageIdView.getBigUint64(0, true); // little-endian
    console.log(`     [${i}] Page ID: ${pageId}`);
  }

  try {
    replicaBucket.applyReplicatedBatch(data, pageCount);
    return ReplicationError.OK; // Success
  } catch (err) {
    console.error("❌ Failed to apply batch:", err);
    return ReplicationError.UnknownError; // Failure
  }
});

console.log("Inserting 5 documents into primary...");
for (let i = 0; i < 5; i++) {
  primaryBucket.insert({
    _id: i,
    hello: `world${i}`,
    counter: i * 10,
  });
}

// Flush to ensure all changes are written
primaryBucket.flush();
console.log("✓ Flushed\n");

// Query both buckets
console.log("Querying primary:");
const primaryDocs = primaryBucket.all({}, {});
console.log(`  Found ${primaryDocs.length} documents`);

console.log("Querying replica:");
const replicaDocs = replicaBucket.all({}, {});
console.log(`  Found ${replicaDocs.length} documents`);

if (primaryDocs.length === replicaDocs.length) {
  console.log("\n✅ SUCCESS: Counts match!");
} else {
  console.log(
    `\n❌ FAIL: Counts don't match (${primaryDocs.length} vs ${replicaDocs.length})`
  );
}

primaryBucket.close();
replicaBucket.close();
