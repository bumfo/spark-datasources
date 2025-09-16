# spark-datasources

This repository hosts experimental Spark DataSource V2 implementations.  The
primary module today is **fourmc**, a reader that understands the block
structure of 4mc-compressed text files and exposes them through the DataFrame
API. 

## Modules

| Path      | Description                                                                                                                                                          |
|-----------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `fourmc/` | Datasource implementation, planner, and documentation for reading `.4mc` files with Spark 3.2.1.  See [`fourmc/README.md`](fourmc/README.md) for a full walkthrough. |

## Getting Started

Most development tasks happen from the repository root:

```bash
sbt compile   # checks all modules still build against Scala 2.12 / Spark 3.2.1
sbt package   # assembles module artifacts into target/scala-2.12/
```

When targeting a specific datasource (for example the fourmc reader), consult
its module README for usage examples, configuration options, and architecture
notes.

## Repository Docs

- [`AGENTS.md`](AGENTS.md) – house rules for contributors and automated
  assistants (coding style, rebasing instructions, etc.).
- [`fourmc/README.md`](fourmc/README.md) – deep dive into the FourMC data
  source, including planner design and reader internals.

## Contributing

Follow the guidelines in `AGENTS.md`: keep commits focused, and run `sbt compile` before
proposing changes.  Open discussions or drafts for new datasources by placing
them under their own subdirectory and documenting their behaviour similarly to
the fourmc module.
