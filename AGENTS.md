# Repository Guidelines

## Project Structure & Module Organization
- Root module (`spark-datasources`): 4mc DataSource V2 for Spark.
  - Source: `src/main/scala/com/example/fourmc/datasource/*`
  - SPI registrations: `src/main/resources/META-INF/services/*`

## Build, Test, and Development Commands
- Root (4mc datasource)
  - `sbt compile` — compile Scala sources
  - `sbt package` — build JAR in `target/scala-2.12/`

## Coding Style & Naming Conventions
- Scala 2.12, Spark 3.2.1. Prefer 2‑space indentation; max line ~100 chars.
- Packages: keep `org.apache.spark.sql.fourmc` for 4mc sources.
- Names: classes/objects `UpperCamelCase`, methods/vals `camelCase`, constants `UPPER_SNAKE_CASE`.
- Imports: group stdlib/third‑party/project; avoid wildcard imports except Spark implicits.
- No formatter is enforced; mirror existing style in edited files.

## Testing Guidelines
- Root module currently has no tests. If adding tests, place under `src/test/scala` and follow `*Spec.scala` naming with ScalaTest.

## Commit & Pull Request Guidelines
- Commits: concise, imperative subject (<72 chars), body explains motivation and impact.
- Always add a brief summary body listing key changes; never submit only a one‑line title.
- Include a valid `Co-Authored-By: Codex CLI` trailer on every commit (use real line breaks; see formatting guidance below).
- PRs: include description, rationale, and user‑visible changes; link issues; add before/after snippets or screenshots where relevant.
- CI parity: run `sbt compile` locally before requesting review; include validation steps. For subprojects, follow their AGENTS.md.

### Commit Message Formatting
- Always use real line breaks inside bash quotes; both `\n` and `\\n` are incorrect (they are recorded literally).
- Preferred:
  - `git commit -m "Title

    Subject

    Co-Authored-By: Codex CLI"`
- Also acceptable:
  - `git commit -m "Title" -m "Subject" -m "Co-Authored-By: Codex CLI"`
- Incorrect (records backslashes):
  - `git commit -m "Title\\n\\nSubject\\nCo-Authored-By: Codex CLI"`

## Security & Configuration Tips
- Dependencies marked `provided` (Spark) must be present at runtime. Ensure the 4mc codec is on the classpath when using the datasource.
- Keep Scala/Spark versions aligned when upgrading and validate end-to-end reads with sample 4mc files.

## Agent-Specific Instructions
- Inlining policy: only inline functions the user explicitly asks for, or short private helpers that cannot be called directly.
- Prefer reusing existing Spark/Hadoop APIs over copying code.
- Always ask before inlining a large piece of code.
- If a stale Git lock prevents committing, remove it explicitly and retry:
  - `rm -f .git/index.lock && git add <files> && git commit -m "..."`
  - Use only when you’re sure no other Git process is running.

### Spark Jobs: Closure Tips
- Keep Spark closures small; avoid capturing non‑serializable state.
- Lambdas in classes capture the whole instance. Put job code in companions.
- Capture only serializable inputs (e.g., broadcast Hadoop conf, schema), not `SparkSession` or table objects.

### Referencing Spark 3.2.1 Sources
- Find file paths in the Spark SQL sources JAR:
  - `jar tf ~/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/org/apache/spark/spark-sql_2.12/3.2.1/spark-sql_2.12-3.2.1-sources.jar | grep FileScan.scala`
  - Similarly search for `PartitionedFileUtil.scala`, `FilePartition.scala`.
- Print a source file to the terminal:
  - `unzip -p ~/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/org/apache/spark/spark-sql_2.12/3.2.1/spark-sql_2.12-3.2.1-sources.jar org/apache/spark/sql/execution/datasources/v2/FileScan.scala | sed -n '1,200p'`
- Tip: use `rg` instead of `grep` if available for faster search.

### Scala-Java Interop Tips
- **In-place array shuffle:** Use `Collections.shuffle(Arrays.asList(array: _*))` for efficient mutable shuffling of object arrays in Scala. Doesn't work with primitive arrays.
- **Collection performance:** Use `collection.iterator.map(f).filter(p).toArray` instead of eager chains; avoid `Seq(...)`, `toList` and `iterator.toSeq` in Scala 2.12 (slow linked list/Stream).
