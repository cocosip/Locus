# Repository Guidelines

## Project Structure & Module Organization

Locus is a .NET solution organized by responsibility. Production libraries live under `src/`: `Locus.Core` for shared primitives, `Locus.MultiTenant` for tenant management, `Locus.FileSystem` for local volume access, `Locus.Storage` for storage pools, queues, SQLite metadata, cleanup, and recovery, and `Locus` as the public package surface. Tests mirror these areas under `tests/`, with focused xUnit projects such as `Locus.Storage.Tests` and `Locus.FileSystem.Tests`. `tests/Locus.Benchmarks` contains BenchmarkDotNet performance harnesses. `samples/Locus.Sample.Console` demonstrates local usage, and `docs/` holds lifecycle, observability, and configuration notes.

## Build, Test, and Development Commands

- `dotnet restore Locus.sln`: restore centrally managed NuGet packages.
- `dotnet build Locus.sln`: build all source, test, benchmark, and sample projects.
- `dotnet test Locus.sln --no-build`: run the full xUnit test suite after a build.
- `dotnet test tests/Locus.Storage.Tests/Locus.Storage.Tests.csproj --no-restore`: run the storage-focused regression suite.
- `dotnet run --project samples/Locus.Sample.Console`: run the console sample.
- `dotnet run -c Release --project tests/Locus.Benchmarks --filter "Locus.Benchmarks.MetadataRepositoryBenchmarks*"`: run a targeted benchmark.

In the Codex app, run `dotnet build` and `dotnet test` outside the sandbox using escalated permissions; sandboxed runs may hang or fail to stream output.

## Coding Style & Naming Conventions

Use C# with four-space indentation. `common.props` enables `LangVersion=latest`, nullable reference types, XML documentation, deterministic builds, and central package metadata. Keep public APIs documented when practical. Use PascalCase for types, methods, and public members; camelCase for locals and parameters; `_camelCase` for private fields. Prefer existing async naming (`AddOrUpdateAsync`, `GetByFileKeyAsync`) and domain terms such as tenant, volume, queue journal, metadata, and cleanup.

## Testing Guidelines

Tests use xUnit with Moq and `System.IO.Abstractions.TestingHelpers` where useful. Name tests after the expected behavior, for example `Method_WhenCondition_ExpectedOutcome`. Add regression tests beside the affected subsystem, especially for SQLite metadata, queue projection, cleanup, recovery, and concurrency changes. Keep benchmark changes separate from correctness tests.

## Commit & Pull Request Guidelines

Recent commits use emoji-prefixed Conventional Commit style, commonly with scopes such as `fix(storage): ...`, `chore(config): ...`, and `build: ...`. Keep commits scoped and descriptive. Pull requests should summarize behavior changes, list verification commands, call out config or migration impact, and link related issues or design docs when applicable.

## Security & Configuration Tips

Do not commit local storage volumes, SQLite databases, queue journals, secrets, or benchmark artifacts. When changing runtime options, update the relevant docs in `docs/` and keep sample configuration aligned with code defaults.
