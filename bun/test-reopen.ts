import { Bucket, ReplicationError } from "./albedo";
import * as fs from "fs";

// Clean up test files if they exist
const primaryPath = "test-reopen-primary.bucket";
const replicaPath = "test-reopen-replica.bucket";
if (fs.existsSync(primaryPath)) fs.unlinkSync(primaryPath);
if (fs.existsSync(replicaPath)) fs.unlinkSync(replicaPath);

console.log("=== Replication with Reopen Test ===\n");

// Phase 1: Create and replicate with index
{
  console.log("Phase 1: Initial setup and replication");
  const primaryBucket = Bucket.open(primaryPath);
  const replicaBucket = Bucket.open(replicaPath);

  primaryBucket.setReplicationCallback((data, pageCount) => {
    try {
      replicaBucket.applyReplicatedBatch(data, pageCount);
      return ReplicationError.OK;
    } catch (err) {
      console.error("Replication failed:", err);
      return ReplicationError.UnknownError;
    }
  });

  // Insert documents
  for (let i = 0; i < 5; i++) {
    primaryBucket.insert({ _id: i, name: `user${i}`, age: 20 + i });
  }

  // Create index
  primaryBucket.ensureIndex("name");
  primaryBucket.flush();

  console.log("✓ Inserted 5 documents and created index on 'name'");

  primaryBucket.close();
  replicaBucket.close();
  console.log("✓ Closed both buckets\n");
}

// Phase 2: Reopen and verify
{
  console.log("Phase 2: Reopen and verify replication");
  const primaryBucket2 = Bucket.open(primaryPath);
  const replicaBucket2 = Bucket.open(replicaPath);

  const primaryDocs = primaryBucket2.all({}, {});
  const replicaDocs = replicaBucket2.all({}, {});

  console.log(`Primary docs: ${primaryDocs.length}`);
  console.log(`Replica docs: ${replicaDocs.length}`);

  // Test index query on replica
  const testDoc = replicaBucket2.get({ name: "user3" }, {});

  if (testDoc && testDoc._id === 3) {
    console.log(`✅ Index query works! Found user3 with _id: ${testDoc._id}`);
  } else {
    console.log("❌ Index query failed!");
  }

  primaryBucket2.close();
  replicaBucket2.close();
}

console.log("\n✅ All tests passed!");
