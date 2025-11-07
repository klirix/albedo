# albedo-wasm

WebAssembly bindings for the [Albedo](../README.md) embedded document store. The package provides a small TypeScript-first API that works in Bun/Node.js and browsers while sharing the exact same storage format.

## Requirements

- Node.js ≥ 18, Bun ≥ 1.0, or any browser
- Optionally tooling that can serve/copy `.wasm` assets referenced as `new URL("./albedo.wasm", import.meta.url)` (Vite, Bun, Webpack, esbuild, and most bundlers already handle this).

## Installation

```bash
npm install albedo-wasm        # or pnpm add / yarn add
bun add albedo-wasm
```

### Choosing a distribution

| Variant                   | How to import                                  | When to use                                                                                                                                |
| ------------------------- | ---------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------ |
| `dist/index.js` (default) | `import { Bucket } from "albedo-wasm";`        | Prefer this for almost everything. WASM sits next to the JS bundle allowing browsers to stream-compile it and CDNs to cache it separately. |
| Inline bundle             | `import { Bucket } from "albedo-wasm/inline";` | Only when you absolutely need a single-file payload (e.g. embedding into sandboxed runtimes that cannot fetch sibling files).              |

When targeting browsers, make sure your bundler copies `albedo.wasm` to your public assets folder. Example for Vite:

```ts
// vite.config.ts
export default defineConfig({
  assetsInclude: ["**/*.wasm"],
});
```

## Quick start (Node.js / Bun)

```ts
import { Bucket } from "albedo-wasm";

// Buckets map to files. Relative paths resolve from process.cwd().
const bucket = Bucket.open("./data/tasks.bucket");

bucket.ensureIndex("meta.created_at", { sparse: true });

bucket.insert({
  _id: crypto.randomUUID(),
  title: "Ship WASM bindings",
  status: "open",
  meta: { created_at: new Date().toISOString(), priority: 1 },
});

const openTasks = bucket.all(
  { status: "open" },
  {
    sort: { asc: "meta.priority" },
    // projection: { pick: ["title", "status", "meta.priority"] },
  }
);

bucket.transform({ status: "open" }, (doc) => ({
  ...doc,
  status: doc.meta.priority > 1 ? "blocked" : "in-progress",
}));

bucket.delete({ status: "done" });
bucket.vacuum();
bucket.close();
```

> Bun users do not need any special configuration—the loader automatically uses Bun's fast file APIs.

## Quick start (browser / Localstorage)

In the browser the bucket file lives inside Localstorage.

```ts
import { Bucket, compileWasmModule } from "albedo-wasm";
import wasm from "albedo-wasm/albedo.wasm?url";

async function initBucket() {
  await compileWasmModule(wasm); // can skip with inline

  const bucket = Bucket.open("notes/app.bucket");

  // Seed data on first run
  if (!bucket.get({ "meta.slug": "welcome" })) {
    bucket.insert({
      title: "Welcome",
      content: "Stored entirely inside Localstorage!",
      meta: { slug: "welcome", created_at: Date.now() },
    });
  }

  const latest = Array.from(
    bucket.list(
      {},
      {
        sector: { limit: 50 },
        sort: { desc: "meta.created_at" },
      }
    )
  );

  return { bucket, latest };
}

const { bucket, latest } = await initBucket();
console.log("Rendered notes", latest);
```

The module uses top-level `await`, so make sure your bundler targets environments that support it (all evergreen browsers and ESM-only Node.js/Bun entry points do).

## Query & index basics

Albedo reuses familiar document-store concepts:

```ts
type Query = {
  query?: Record<string, Filter>;
  sort?: { asc: string } | { desc: string };
  sector?: { offset?: number; limit?: number };
  // Projection is not working atm
  // projection?: { pick?: string[] } | { omit?: string[] };
};

type Filter =
  | Scalar
  | { $eq: Scalar }
  | { $gt: Scalar }
  | { $gte: Scalar }
  | { $lt: Scalar }
  | { $lte: Scalar }
  | { $ne: Scalar }
  | { $in: Scalar[] }
  | { $between: [Scalar, Scalar] }
  | { $startsWith: string }
  | { $endsWith: string }
  | { $exists: any }
  | { $notExists: any };
```

Use dotted paths (`"meta.created_at"`) for nested fields and call `bucket.ensureIndex(path, options)` before running heavy queries. Index options:

```ts
bucket.ensureIndex("meta.slug", {
  unique: true,
  sparse: false,
  reverse: false,
});
```

## API highlights

- `Bucket.open(path)` → opens (or creates) a bucket file. Call `bucket.close()` when shutting down.
- `bucket.insert(doc)` → inserts any BSON-serializable document.
- `bucket.ensureIndex(path, options)` / `bucket.dropIndex(path)` → manage indexes.
- `bucket.list(query?, options?)` → generator that yields matching documents; integrate with `for...of`, `Array.from`, or break early by returning `true` from the iterator.
- `bucket.all(query?, options?)` → convenience wrapper that collects `list`.
- `bucket.get(query, options?)` → returns the first matching document or `null`.
- `bucket.delete(query)` → removes matching documents.
- `bucket.transform(query, mutator)` → streaming mutate/replace/delete. Return a new document to replace, `null` to delete, or `undefined` to leave the doc untouched.
- `bucket.vacuum()` → compacts the file.
- `Bucket.defaultIndexOptions` and `Bucket.version()` are also exported for advanced tooling.

TypeScript declarations live in `dist/index.d.ts`, so editors receive full completions automatically.

## Local development

Bun is required for dev

```bash
# Install JS dependencies
bun install

# Build the standard JS + WASM pair
bun run build

# Build single-file inline distribution
bun run build:inline

# Run the Bun-based unit tests
bun test
```

---

Need help or have questions? Open an issue in the main repository and mention the `albedo-wasm` package. Happy hacking!
