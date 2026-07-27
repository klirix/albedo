# Query-driven update expressions

[Documentation index](README.md) ·
[Query language](query-language.md) ·
[Configuration](configuration.md)

The Zig API can apply an `UpdateProgram` to every document selected by a
`Query`.

```zig
const albedo = @import("albedo");

var query_doc = try albedo.bson.fmt.serialize(.{
    .query = .{ .status = "active" },
}, allocator);
defer query_doc.deinit(allocator);
var q = try albedo.Query.parse(allocator, query_doc);
defer q.deinit(allocator);

var program_doc = try albedo.bson.fmt.serialize(.{
    .@"$set" = .{
        .visits = .{ .@"$plus" = .{ "$.visits", 1 } },
        .display_name = .{
            .@"$concat" = .{ "$.first_name", " ", "$.last_name" },
        },
        .updated_at = "$$now",
    },
    .@"$unset" = "legacy_status",
}, allocator);
defer program_doc.deinit(allocator);
var program = try albedo.UpdateProgram.parse(allocator, program_doc);
defer program.deinit(allocator);

const updated_count = try bucket.transfigurate(q, program);
```

`Bucket.transfigurate` updates every match in one transaction, commits once,
and returns the match count. An expression failure aborts the still-active
transaction.

Lower-level APIs are also available:

- `Transaction.transfigurate(query, program)` stages updates in an existing
  transaction.
- `TransformIterator.transfigurate(program)` updates the current target.
- `TransformIterator.transfigurateAll(program)` updates every remaining
  target and returns the count.

These APIs currently belong to the Zig core API. The C ABI's
`albedo_transform` flow continues to accept complete replacement documents
from the caller.

## Update stages

An update program is either one stage or an ordered pipeline.

| Form | Example | Effect |
|------|---------|--------|
| `$set` | `{ "$set": { "profile.state": "active" } }` | Set one or more paths |
| `$unset` | `{ "$unset": "legacy" }` | Remove one path |
| `$unset` array | `{ "$unset": ["legacy", "debug"] }` | Remove several paths |
| Direct assignment | `{ "profile.state": "active" }` | Shorthand for `$set` |

Paths may be dotted, but cannot be empty or contain leading, trailing, or
consecutive dots.

## Pipelines and evaluation order

Pipelines use an array-like BSON root with sequential numeric keys. With
`bson.fmt.serialize`, write those keys explicitly:

```zig
var program_doc = try albedo.bson.fmt.serialize(.{
    .@"0" = .{
        .@"$set" = .{
            .score = .{ .@"$plus" = .{ "$.score", 10 } },
        },
    },
    .@"1" = .{
        .status = "recalculated",
        .@"$unset" = .{ "temporary_score", "old_rank" },
    },
}, allocator);
```

Every expression in a stage reads the document as it existed at the beginning
of that stage. A later stage sees the output of the previous stage. Use
separate stages when one computed field depends on another field set by the
same program.

## Expression reference

Expressions can be nested in `$set` values, direct assignments, literal
documents, and literal arrays.

| Expression | Example | Result |
|------------|---------|--------|
| Literal | `"active"`, `42`, `{ "a": 1 }` | Literal value; nested containers evaluate recursively |
| Field reference | `"$.profile.score"` | Value at the dotted path, or BSON `null` when missing |
| Current time | `"$$now"` | BSON datetime captured when evaluation begins for the current document |
| `$plus` | `{ "$plus": ["$.score", 10] }` | Sum of two or more numeric arguments |
| `$minus` | `{ "$minus": ["$.balance", 5, 2] }` | Left-to-right subtraction |
| `$concat` | `{ "$concat": ["$.first", " ", "$.last"] }` | Concatenate one or more strings |
| `$isoDateTime` | `{ "$isoDateTime": "2024-04-22T10:20:30.456Z" }` | Parse or preserve a BSON datetime |

### Numeric behavior

`$plus` and `$minus` accept BSON `int32`, `int64`, and `double` values.
Integer operations preserve `int32` when the result fits, promote to `int64`
when needed, and report overflow beyond `int64`. Any `double` argument
produces a `double`.

### Strings and datetime

`$concat` requires all evaluated arguments to be strings.

`$isoDateTime` accepts an existing BSON datetime or a string in:

```text
YYYY-MM-DDTHH:MM:SS[.fraction]Z
YYYY-MM-DDTHH:MM:SS[.fraction]±HH:MM
```

Any string beginning with `$.` is a field reference and must include a
non-empty path. To produce a literal string with that prefix, construct it:

```bson
{ "$concat": ["$", ".not_a_reference"] }
```

`$$now` is reserved for the current timestamp. An unknown single-key
expression operator beginning with `$` is rejected during program parsing.

## Query selection

The full [query language](query-language.md) selects update targets. Filters
and indexes determine the matched documents, while `sector` can constrain the
target slice. Update expressions always evaluate against complete stored
documents, not their projected result.
