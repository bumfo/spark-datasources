# Repository Guidelines

## Project Structure & Module Organization
- Root module (`spark-datasources`): 4mc DataSource V2 for Spark.
  - Source: `src/main/scala/com/example/fourmc/datasource/*`
  - SPI registrations: `src/main/resources/META-INF/services/*`
- Subproject: `spark-protobuf-backport` (separate SBT build)
  - Scala core: `core/src/main/scala/...`
  - Tests: `core/src/test/scala/**/*Spec.scala`
  - Uber JAR: `uber/` (sbt-assembly)
  - Python glue/tests: `python/` (PySpark functional test)

## Build, Test, and Development Commands
- Root (4mc datasource)
  - `sbt compile` — compile Scala sources
  - `sbt package` — build JAR in `target/scala-2.12/`
- Protobuf backport subproject
  - `cd spark-protobuf-backport && sbt core/test` — run ScalaTest suite
  - `sbt uber/assembly` — build shaded JAR: `uber/target/scala-2.12/spark-protobuf-backport-shaded-<ver>.jar`
  - `cd python && ./run_test.sh` — run PySpark functional test (expects venv with PySpark 3.2–3.3)

## Coding Style & Naming Conventions
- Scala 2.12, Spark 3.2.1. Prefer 2‑space indentation; max line ~100 chars.
- Packages: keep `com.example.fourmc.datasource` for 4mc sources.
- Names: classes/objects `UpperCamelCase`, methods/vals `camelCase`, constants `UPPER_SNAKE_CASE`.
- Imports: group stdlib/third‑party/project; avoid wildcard imports except Spark implicits.
- No formatter is enforced; mirror existing style in edited files.

## Testing Guidelines
- Scala: ScalaTest in `spark-protobuf-backport/core/src/test/scala` with `*Spec.scala` naming.
  - Run: `sbt core/test` (benchmarks are excluded by build settings).
- Python: `spark-protobuf-backport/python/run_test.sh` runs a functional test; ensure shaded JAR is built and a venv has PySpark.
- Aim for coverage of parsing, conversion paths, and edge cases (empty/invalid inputs). Prefer small, focused specs.

## Commit & Pull Request Guidelines
- Commits: concise, imperative subject (<72 chars), body explains motivation and impact.
- PRs: include description, rationale, and user‑visible changes; link issues; add before/after snippets or screenshots where relevant.
- CI parity: run `sbt compile` (root) and `sbt core/test` (backport) locally before requesting review; include steps to validate.

## Security & Configuration Tips
- Dependencies marked `provided` (Spark) must be present at runtime. Ensure the 4mc codec is on the classpath when using the datasource.
- Keep Spark/Scala versions aligned across modules when upgrading; test both Scala and Python entry points after changes.

