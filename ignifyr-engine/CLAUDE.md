# ignifyr-engine — core mapping/transformation engine

The Ignifyr engine: reads source data with Spark, applies FHIR mappings, and writes FHIR
resources. Usable as a library or as the standalone CLI/batch tool. Package root
`io.ignifyr.engine`.

## Entry points
- `Boot` ([src/main/scala/io/ignifyr/engine/Boot.scala](src/main/scala/io/ignifyr/engine/Boot.scala))
  — standalone entrypoint; routes args to `cli` (interactive, default), `run --job <path>` (batch),
  or any extension-contributed command token (e.g. the REDCap module's `extract-redcap-schemas`)
  via `ExtensionRegistry.cliCommands`. It is the Main-Class of the shaded jar.
- `IgnifyrEngine` — central orchestrator wiring config, repositories, the Spark session, and the mapping-job manager.
- `execution/MappingJobLauncher` — the single home of the batch/streaming/scheduled job dispatch;
  both the CLI (`CommandLineInterface.runJob`) and the server's `ExecutionService` launch through it.
  ⚠️ `MappingJobLaunch.Batch(completion)` carries the future returned by
  `RunningJobRegistry.registerBatchJob`, which is chained with `andThen` so it completes only after
  `handleCompletedBatchJob` (input archiving + deregistration) has run — **not** the raw mapping-task
  future. Awaiting it is therefore safe for a caller that exits the JVM immediately afterwards, which the
  one-shot CLI does (`System.exit(0)` right after the await). Don't "simplify" it back to the task future:
  that reintroduces a race where `archiveMode = archive|delete` silently leaves inputs in place.
- CLI: `cli/CommandLineInterface` + `cli/command/*` (`Run`, `Load`, `Reload`, `Stop`, `Help`,
  `ListRunningMappings`, `ListPlugins`, …). Modules contribute commands via the `CliCommandProvider`
  SPI; `list-plugins` prints the installed extensions and everything they contribute (a CI gate on
  the edition boundary).
  ⚠️ `CommandLineInterface.nextArg` is the parser behind *every* `Boot` invocation. **The first bare
  token wins the `command` slot** — a later bare token must not overwrite it, or a trailing valueless
  flag gets consumed as the command and `run --job` is reported as `unknown command --job`
  (fixed 2026-08-10, pinned by `CommandLineInterfaceTest`).

## Layout (`src/main/scala/io/ignifyr/engine/`)
- `config/` — `IgnifyrConfig`, `IgnifyrEngineConfig` (read the `ignifyr` HOCON block), `FunctionLibrariesConfig`.
  `IgnifyrConfig.createSparkConf` merges three layers — engine defaults → `ExtensionRegistry.sparkConfContributions`
  → the user's `spark { }` block — with `++` (last wins) for scalars, but **joins** the keys in
  `additiveSparkConfKeys`, the ones Spark reads as comma-separated registration lists. Without that join a
  user setting `spark.sql.extensions` would silently erase a module's contributed session extension. If you
  add a key to that set, be sure Spark really comma-separates it: `*.extraClassPath` uses the platform path
  separator and `spark.sql.catalog.spark_catalog` is a scalar, so both are deliberately excluded.
- `spi/` — the ServiceLoader extension SPI: `IgnifyrExtension` (one per module; contributes source
  connectors, sinks, terminology/identity services, CLI commands, source-failure descriptors, schema
  inferrers, streaming/scheduling capabilities, `sparkConfContributions`, `extraCapabilities`, and its own
  job-model classes and json4s serializers — `jobModelClasses`/`jobSerializers`, see `util/` below),
  `ExtensionRegistry`, the provider traits — `SourceConnector`, `SinkProvider`, `CliCommandProvider`,
  `SourceSchemaInferrer`, `SourceFailureDescriptor`, `StreamingExecutionProvider`, `SchedulerProvider`,
  `MappingTaskPipeline`, and `TerminologyServiceProvider`/`IdentityServiceProvider` (both in
  `IntegratedServiceProviders.scala`) — plus `ExtensionHints` (error-message module coordinates) and
  `core/CoreExtension`. **`CoreExtension` registers only the built-in CLI commands and the
  `LocalTerminologyService`: the engine ships no concrete I/O.** The three merge/index helpers
  (`indexUnique`, `singleCapability`, `mergeSparkConf`) are `private[ignifyr]` rather than `private`
  **on purpose**: the registries themselves are fed by ServiceLoader, so a duplicate registration cannot
  be staged on a test classpath without a second classloader, and the fail-fast contracts are asserted on
  the helpers directly (`ExtensionRegistrySpec`). Don't narrow them back.
- `data/read/` — `BaseDataSourceReader` (abstract base) + `SourceHandler`, which wraps the read and
  dispatches via `ExtensionRegistry.sourceConnectors`. The file reader (`FileDataSourceReader`) and the
  SQL/Kafka/FHIR-server readers all live in their `ignifyr-connector-*` modules.
- `data/write/` — `BaseSinkWriter` **and, declared in the same file, `object SinkWriterFactory`**, which
  is where the `ExtensionRegistry.sinkProviders` lookup happens; `SinkHandler` orchestrates the write but
  receives an already-constructed writer. Every concrete sink lives in its own
  `ignifyr-sink-*` module: the FHIR-repository writer in `ignifyr-sink-fhir`, the file-system
  writer and its pluggable output formats in `ignifyr-sink-file` (a `FileSinkFormat` sub-SPI);
  JSON-source and Delta-sink formats are the enterprise `ignifyr-format-*` modules.
- `mapping/` — `MappingTaskExecutor`, `FhirMappingService`, `job/FhirMappingJobManager` (batch +
  delegating streaming/scheduling seams), `schema/` (load/convert schemas), `context/MappingContextLoader`,
  `fhirPath/FhirPathMappingFunctions`, `service/LocalTerminologyService`. Cron scheduling itself lives
  in the enterprise `ignifyr-runtime-scheduling` module (`SchedulerProvider`).
- `execution/` — `MappingJobLauncher`, `RunningJobRegistry` (tracks running Spark jobs), `processing/`
  (`ErroneousRecordWriter`, `FileStreamInputArchiver`), `log/ExecutionLogger`.
  ⚠️ `FileStreamInputArchiver` runs inside a **single shared `TimerTask`** for the whole process, so any
  exception escaping it kills the timer thread and stops archiving for *every* job. Spark's commit
  directory does not exist until the first commit (and vanishes again after `clearCheckpoints`), and
  `File.listFiles()` returns `null` — not empty — for an absent directory; both that call and
  `SparkUtil.getLastCommitOffset` (which additionally used `.max` on a possibly empty array) now treat
  "no commits yet" as offset `-1`. Fixed 2026-08-10; pinned by `SparkUtilTest`.
- `model/` — domain models (`FhirMapping`, `FhirMappingJob`, `FhirMappingTask`, `*SinkSettings`,
  `MappingJobSourceSettings`, `BatchingStrategy`, …) and `model/exception/`. Models for ALL source
  types stay here (even extracted connectors) so any job JSON parses everywhere.
  ⚠️ `FhirMappingTask.substituteBatchParameters` must substitute **longest parameter name first**: plain
  `Map` iteration order let a shorter name rewrite the prefix of a longer one, so with `year` and
  `yearEnd` both defined `$yearEnd` became `2020End`. Fixed 2026-08-10; pinned by `FhirMappingTaskTest`.
- `repository/` — `mapping/FhirMappingFolderRepository` (file-backed mapping repository) +
  `ICachedRepository`.
- `env/` — `EnvironmentVariable` + `EnvironmentVariableResolver` (env-var substitution in job settings).
  Only names in the `EnvironmentVariable` enumeration are substitutable, and an unset one is an error
  rather than a silent pass-through. `resolveFileContent` has a `private[ignifyr]` overload taking an
  explicit environment — the substitution seam, since `sys.env` cannot be set from inside the JVM.
- `Execution.scala` (top level) — the Akka `ActorSystem` the registry reads root config from, and which
  the service-provider modules use.
- `util/` — helpers (`FileUtils`, `SparkUtil`, `CsvUtil`, …). `FhirMappingJobFormatter.formats` is the one
  json4s `Formats` every job (de)serialization uses, engine and server alike: a `ShortTypeHints` table
  (`jsonClass` = simple class name) over the engine's own job-model classes (`engineJobModelClasses`) **plus**
  the installed modules' `jobModelClasses`/`jobSerializers`, built on first use. So a module defines the models
  of its own source/sink types; the engine knows only the community ones. A simple name claimed twice fails
  fast (`ExtensionRegistry.init()` builds the formats), and a polymorphic member whose `jsonClass` no
  installed module provides fails to parse with "Unknown jsonClass '…': no installed module provides it" —
  `JobModelTypeHints` makes json4s treat such a field as a type hint instead of failing on the abstract type.
  ⚠️ An extension's `initialize` must not use the formats (building them loads the registry).

## Adding a new data source reader (common task)
1. Create (or extend) an `ignifyr-connector-<x>` module with a reader extending `BaseDataSourceReader`
   — mirror `ignifyr-connector-sql`.
2. Define the binding and settings models (`MappingSourceBinding`, `MappingJobSourceSettings`) **in that
   module** and contribute them through its `IgnifyrExtension.jobModelClasses` (plus `jobSerializers` if
   the default case-class serialization does not fit), so job JSON naming them parses. Override
   `withResolvedEnvironmentVariables` if a field may carry a `${ENV_VAR}` placeholder. (The community
   sources' models predate this and live under `model/`; there is no need to add new ones there.)
3. Register the reader via the module's `IgnifyrExtension` (`sourceConnectors`) and its
   `META-INF/services/io.ignifyr.engine.spi.IgnifyrExtension` entry; add the module to the reactor
   and to the distributions that ship it (`ignifyr-cli` for community, `ignifyr-server` for enterprise).
4. Add a lightweight registration spec **in the new module** (the engine's own suites can't reach it —
   the dependency runs the other way). Heavy fixture-based suites depend on the shared `ignifyr-testkit`
   (test scope) for `IgnifyrTestSpec` + `OnFhirTestContainer` + fixtures; if the suite writes results to
   a FHIR server it also needs `ignifyr-sink-fhir` at test scope, since the writer is no longer in the
   engine.

## Tests
- Unit: `mvn test -pl ignifyr-engine` → suites in `io.ignifyr.test` (`wildcardSuites`). **All thirteen are
  self-contained** `AnyFlatSpec`s: `ExtensionRegistrySpec`, `ListPluginsTest`, `CommandLineInterfaceTest`,
  `SinkHandlerTest`, `EnvironmentVariableResolverTest`, `FileStreamInputArchiverTest`,
  `RunningJobRegistryTest`, `SchemaConverterTest`, `FhirMappingJobExecutionTest`, `FhirMappingTaskTest`,
  `FhirMappingJobFormatterTest`, `FhirMappingUtilityTest`, `SparkUtilTest`. They do **not** use
  `IgnifyrTestSpec`: that harness lives in `ignifyr-testkit`, and the engine must never depend on the
  testkit (reactor cycle). No Docker. Keep it that way — a new engine suite that reaches for a fixture
  folder or a connector is a sign it belongs in the module that supplies the I/O.
- Two contracts in there are worth knowing before you touch the code they cover:
  `RunningJobRegistryTest` asserts that the future `registerBatchJob` returns completes **only after**
  input archiving has run (no sleep in the assertion — awaiting that future *is* the guarantee the
  one-shot CLI relies on before `System.exit(0)`), and `SchemaConverterTest` deliberately covers only the
  Spark→FHIR direction: `convertSchema` needs onFHIR's base FHIR config initialized, so it is exercised by
  the server's long-tier `SchemaEndpointTest` instead.
- The engine has **no long-tier suites of its own** — there is no `io.ignifyr.integrationtest` source
  folder here (the pom's `integration-test` execution simply finds nothing). The suites that exercise the
  engine end-to-end live in the modules that supply the I/O: `ignifyr-connector-file`
  (`FhirMappingJobManagerTest`), `ignifyr-connector-sql` (`SqlSourceTest`), and — in the enterprise
  repository — `ignifyr-runtime-scheduling` (`SchedulingTest`) and `ignifyr-runtime-streaming` (the
  folder-watch + Kafka E2E suites) — all Docker-requiring, all opt-in via `mvn -B verify -DskipITs=false`.
- One suite: `mvn test -pl ignifyr-engine -Dsuffixes='.*ListPluginsTest'`. `-DwildcardSuites`/`-Dsuites`
  are ignored — the pom sets `wildcardSuites` explicitly, which beats the command-line property.
- ⚠️ FHIRPath function libraries are validated at the `install` phase — a `scala-maven-plugin`
  launcher runs `ValidateFhirPathFunctionLibraries` over `io.ignifyr.engine.mapping`. Keep every new
  FHIRPath function annotated with `@FhirPathFunction`, or `install` fails.
