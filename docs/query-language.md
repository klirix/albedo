# Query language

[Documentation index](README.md) ·
[Getting started](getting-started.md) ·
[Update expressions](update-expressions.md) ·
[Streaming](streaming.md)

Albedo queries are BSON documents with up to five optional sections:

1. `query` — filter expressions
2. `sort` — ordering by one field
3. `sector` — offset and limit
4. `projection` — returned fields
5. `cursor` — exported streaming state

An empty document `{}` returns all documents. See
[streaming and cursors](streaming.md) for cursor details.

## Query structure

```bson
{
  "query": {
    <filter expressions>
  },
  "sort": {
    "asc": "field.path" | "desc": "field.path"
  },
  "sector": {
    "offset": <int>,
    "limit": <int>
  },
  "projection": {
    "pick": ["field"] | "omit": ["field"]
  },
  "cursor": {
    <exported cursor state>
  }
}
```

## Field filters

Write a filter as `"field.path": { "$operator": value }`. A scalar value is
shorthand for `$eq`:

```bson
{ "query": { "status": "active" } }
```

Multiple top-level filters are combined with implicit AND. Multiple operators
on the same field are also combined:

```bson
{
  "query": {
    "age": { "$gte": 18, "$lte": 65 },
    "status": "active"
  }
}
```

### Comparison operators

| Operator | Example | Matches |
|----------|---------|---------|
| `$eq` | `{ "status": { "$eq": "active" } }` | `status == "active"` |
| `$ne` | `{ "age": { "$ne": 30 } }` | `age != 30` |
| `$lt` | `{ "score": { "$lt": 100 } }` | `score < 100` |
| `$lte` | `{ "score": { "$lte": 100 } }` | `score <= 100` |
| `$gt` | `{ "count": { "$gt": 5 } }` | `count > 5` |
| `$gte` | `{ "count": { "$gte": 5 } }` | `count >= 5` |

Comparisons use BSON type ordering. Cross-type comparisons therefore follow
the ordering implemented by `BSONValue.order`, rather than coercing values to
a common type.

### Membership and range operators

| Operator | Example | Matches |
|----------|---------|---------|
| `$in` | `{ "status": { "$in": ["active", "pending"] } }` | Any listed value |
| `$between` | `{ "age": { "$between": [18, 65] } }` | `18 < age < 65` |

`$in` requires a BSON array. When an indexed array field contains several
matching values, query execution deduplicates the document.

`$between` requires exactly two elements and is strictly exclusive at both
bounds.

### String operators

| Operator | Example | Matches |
|----------|---------|---------|
| `$startsWith` | `{ "name": { "$startsWith": "Jo" } }` | String begins with `Jo` |
| `$endsWith` | `{ "domain": { "$endsWith": ".com" } }` | String ends with `.com` |

String matching is case-sensitive. Non-string field values do not match.

### Existence operators

| Operator | Example | Matches |
|----------|---------|---------|
| `$exists` | `{ "thumbnail": { "$exists": true } }` | Field is present with any value |
| `$notExists` | `{ "deleted_at": { "$notExists": true } }` | Field is absent |

The operand is accepted but ignored; the operator name determines the
behavior.

## Logical operators

`$or`, `$and`, and `$nor` accept arrays of filter-group documents. A group can
contain field filters or nested logical operators.

### `$or`

```bson
{
  "query": {
    "$or": [
      { "role": "admin" },
      { "public": true },
      { "owner_id": ObjectId("507f1f77bcf86cd799439011") }
    ]
  }
}
```

At least one group must match. If every branch has an indexable predicate,
Albedo can use an index-union plan and deduplicate overlapping results.

### `$and`

```bson
{
  "query": {
    "$and": [
      { "age": { "$gte": 18 } },
      { "status": "active" },
      { "verified": true }
    ]
  }
}
```

Every filter in every group must match. Explicit `$and` participates in
planning like top-level implicit AND:

- indexed predicates can drive the scan
- range predicates on the same indexed field can tighten bounds
- inner `$in` predicates can use the point strategy

### `$nor`

```bson
{
  "query": {
    "$nor": [
      { "spam": true },
      { "deleted": true }
    ]
  }
}
```

No group may match. When every branch is index-covered, Albedo collects the
excluded document IDs from the branch indexes and filters them from a data
page scan. This exclusion plan is eager and does not support cursors.

### Nesting and leaf filters

Logical operators may be nested:

```bson
{
  "query": {
    "$or": [
      { "role": "admin" },
      {
        "$and": [
          { "status": "active" },
          { "verified": true }
        ]
      }
    ]
  }
}
```

Leaf filters beside a logical operator are AND-ed with its result:

```bson
{
  "query": {
    "$or": [
      { "role": "admin" },
      { "public": true }
    ],
    "deleted": false
  }
}
```

This matches `(role == "admin" OR public == true) AND deleted == false`.

## Sorting

```bson
{
  "query": { "status": "active" },
  "sort": { "asc": "created_at" }
}
```

- `asc` sorts lowest first.
- `desc` sorts highest first.
- Only one sort field is supported.
- A matching index can cover the sort and avoid materialization.

For example, an index on `age` can cover:

```bson
{
  "query": { "age": { "$gte": 18 } },
  "sort": { "asc": "age" }
}
```

## Pagination with `sector`

```bson
{
  "query": { "status": "active" },
  "sector": { "offset": 20, "limit": 10 }
}
```

- `offset` defaults to `0`.
- `limit` defaults to all remaining matches.

Sector is applied after filtering and sorting.

## Projections

Use `pick` to return only selected top-level fields:

```bson
{
  "query": { "status": "active" },
  "projection": { "pick": ["name", "email"] }
}
```

Use `omit` to remove selected top-level fields:

```bson
{
  "projection": { "omit": ["internal_notes", "debug_data"] }
}
```

A projection must contain exactly one of `pick` or `omit`, whose value is an
array of non-empty field names. Projection happens after filtering, sorting,
and pagination. `pick` does not preserve `_id` automatically; include it
explicitly when needed.

## Planning and index use

The planner chooses among:

- index range scans for equality, comparison, `$between`, and `$startsWith`
- index point scans for `$in`
- index unions for fully indexable `$or` branches
- index-backed exclusions for fully indexable `$nor` branches
- full scans when an index cannot cover a useful predicate

Exact matches and narrow ranges are preferred over broader scans. A chosen
index can also cover sorting when the path and direction are compatible.
Filters not represented by the chosen index strategy are evaluated against
the document after it is read.

## More examples

Range, sort, and pagination:

```bson
{
  "query": { "age": { "$gte": 18, "$lte": 65 } },
  "sort": { "desc": "created_at" },
  "sector": { "offset": 0, "limit": 50 }
}
```

Nested field:

```bson
{
  "query": { "profile.bio": { "$startsWith": "Senior" } }
}
```

Array membership:

```bson
{
  "query": { "tags": { "$in": ["urgent", "blocked"] } }
}
```

## Related guides

- [Update expressions](update-expressions.md)
- [Streaming and cursors](streaming.md)
- [Subscriptions](subscriptions.md)
