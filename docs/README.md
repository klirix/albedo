# Albedo documentation

Albedo is an embedded BSON document database with B⁺-tree indexes, a
write-ahead log, streaming queries, subscriptions, and WAL-native replication.

## Start here

| Guide | Use it when you want to… |
|-------|---------------------------|
| [Getting started](getting-started.md) | Install Albedo, choose a binding, build the library, or call the C API |
| [Configuration and durability](configuration.md) | Configure a bucket, WAL behavior, caching, and read/write durability |
| [Query language](query-language.md) | Filter, sort, paginate, project, and understand index planning |
| [Update expressions](update-expressions.md) | Apply query-driven updates with `UpdateProgram` from Zig |
| [Streaming and cursors](streaming.md) | Keep query iterators open or resume them from exported cursor state |
| [Subscriptions](subscriptions.md) | Consume real-time insert, update, and delete events |
| [Replication](replication.md) | Stream committed WAL frames from a primary to a replica |

## API references

- [C header](../include/albedo.h) — exported C ABI declarations
- [Root project overview](../README.md) — project summary and quick start
- [Build notes](building.md) — build commands and artifact locations
- [Engine notes](../AGENTS.md) — storage internals and invariants for
  contributors and coding agents

## Suggested reading paths

For application developers:

1. [Getting started](getting-started.md)
2. [Configuration and durability](configuration.md)
3. [Query language](query-language.md)
4. Choose [streaming](streaming.md), [subscriptions](subscriptions.md), or
   [replication](replication.md) as needed.

For Zig users building mutation workflows:

1. [Query language](query-language.md)
2. [Update expressions](update-expressions.md)
3. [Configuration and durability](configuration.md)

For contributors and LLM-based tooling:

1. Read the task-specific guide above.
2. Consult [engine notes](../AGENTS.md) only when storage internals matter.
3. Use the source files linked by the guide for implementation details.
