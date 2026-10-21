# Ignifyr (formerly toFHIR)

[![Research by SRDC](https://img.shields.io/badge/Research-SRDC-red)](https://srdc.com.tr)
[![Commercial Support](https://img.shields.io/badge/Commercial%20Support-Pontegra-blue)](https://pontegra.com)

> [!IMPORTANT]
> **Rebranding Announcement**
> 
> **toFHIR** has been officially rebranded as **Ignifyr**. This change reflects our transition from a research-focused engine at [**SRDC**](https://srdc.com.tr) to a commercially supported product line by [**Pontegra**](https://pontegra.com).
>
> **Note on Technical Migration:** The technical migration is complete as of this release: Maven coordinates, package names (`io.ignifyr.*`), configuration keys, REST paths, and Docker image tags all use the `ignifyr` name. If you are upgrading from a toFHIR release, see [Migrating from toFHIR](#migrating-from-tofhir).

---

## Migrating from toFHIR

Everything that carried the `tofhir` name has been renamed to `ignifyr`. When upgrading an existing deployment:

| Area | Old (toFHIR) | New (Ignifyr) |
|---|---|---|
| Maven coordinates | `io.onfhir:tofhir-engine_2.13` (and other `tofhir-*` artifacts) | `io.ignifyr:ignifyr-engine_2.13` (and `ignifyr-*`) |
| Packages & classes | `io.tofhir.*`, `ToFhirEngine`, `ToFhirError`, … | `io.ignifyr.*`, `IgnifyrEngine`, `IgnifyrError`, … |
| Configuration (HOCON) | `tofhir { … }`, `tofhir-redcap { … }`, `-Dtofhir.*` overrides | `ignifyr { … }`, `ignifyr-redcap { … }`, `-Dignifyr.*` |
| REST base path | `http://<host>:8085/tofhir` | `http://<host>:8085/ignifyr` (configurable via `webserver.base-uri`) |
| Internal database folder | `tofhir-db` | `ignifyr-db` — rename your existing folder, or point `ignifyr.db-path` at it |
| Standalone jars | `tofhir-engine-standalone.jar`, `tofhir-server-standalone.jar` | `ignifyr-engine-standalone.jar`, `ignifyr-server-standalone.jar` |
| Docker | images `srdc/tofhir-*`, env vars `TOFHIR_*`, home `/usr/local/tofhir` | images `srdc/ignifyr-*`, env vars `IGNIFYR_*`, home `/usr/local/ignifyr` |
| Log files | `logs/tofhir-engine.log`, `logs/tofhir-server.log`, `logs/tofhir-mappings.log` | `logs/ignifyr-engine.log`, `logs/ignifyr-server.log`, `logs/ignifyr-mappings.log` |
| API error `type` URIs | `https://tofhir.io/errors/<Error>` | `https://ignifyr.io/errors/<Error>` |
| Server metadata field | `toFhirRedcapVersion` | `ignifyrRedcapVersion` |

The REDCap companion service, formerly [tofhir-redcap](https://github.com/srdc/tofhir-redcap), is now `ignifyr-redcap-service`, part of the Ignifyr Enterprise Edition. Its default base path changed from `/tofhir-redcap` to `/ignifyr-redcap-service`, and the Enterprise server's `ignifyr-redcap.endpoint` points there by default. The GitHub repository is no longer developed. The web UI image is now `srdc/ignifyr-web` (formerly `srdc/tofhir-web`).

---

## Overview
**Ignifyr** is a powerful, FHIR-first ETL engine designed to map legacy health data to the **HL7 FHIR** standard. It is built to handle complex mappings with high performance and scalability.

It can be used as a library or a standalone tool for data integration. The standalone mode accepts command-line arguments to run batch executions or start a command-line interface (CLI) for interactive mapping management.

* **Website:** [ignifyr.io](https://ignifyr.io)
* **Research Paper:** _To be published soon..._

### Key Capabilities

* **Versatile Connectivity:** Read from file systems, RDBMS, Apache Kafka, REDCap, or FHIR servers.
* **Advanced Mapping:** Utilizes the [onfhir-template-engine](https://github.com/srdc/onfhir-libs/tree/main/onfhir-template-engine) to support 1-to-1, 1-to-many, many-to-1, and many-to-many mappings.
* **Flexible Output:** Generate HL7 FHIR resources and persist them to a FHIR endpoint (e.g., [Repofyr](https://repofyr.io)), or write them to a file system as NDJSON, CSV, Parquet, or Delta Lake.

## Architecture & Editions

Ignifyr is a small **core engine plus plugins**. The engine reads a source, applies FHIR mappings, and
hands the results to a sink; every concrete input and output is a plugin discovered at runtime through a
Java `ServiceLoader` service-provider interface (`IgnifyrExtension`). The engine never names a
connector, sink, format, or capability directly — it looks them up in its `ExtensionRegistry` by the
settings type a job uses. The engine itself ships **no concrete I/O at all**: even the flagship
FHIR-repository writer is a plugin. The practical upshot, and the core design rule, is: **moving a
feature between editions is a one-folder module move, with zero engine code changes.**

The data flow is always the same: **read source → apply mappings → write to sink → (optionally) archive
the input.** A mapping job's JSON *parses* everywhere, because every settings and model class lives in
the engine regardless of which edition ships the corresponding plugin; if a job names a plugin that
isn't installed, it fails at run time with an actionable "install `…`" message rather than a parse
error.

Ignifyr comes in two editions, in two repositories. **Community** — this repository, Apache-2.0 —
is the batch engine: its library modules are published to SRDC Nexus
(`https://nexus.srdc.com.tr/repository/maven-releases/`) and shaded into the `ignifyr-cli` standalone
jar. **Enterprise** — a private repository — builds on those published community artifacts and adds
the REST server, streaming and scheduling, and the advanced connectors/formats, shaded into the
`ignifyr-server` jar. Each distribution's dependency list *is* the definition of its edition, and
because every plugin is ServiceLoader-discovered, moving a module between editions is a folder move
plus one line in each distribution POM. A `maven-enforcer` gate (`ban-enterprise-deps`,
opted into by the community modules) makes the boundary mechanical: `spark-sql-kafka-0-10`, `cron4j`,
`delta-spark`, the DB2 JCC driver, and the Logstash encoder / Fluentd logger can never reach a
community module or the community fat jar.

### Module reference

Each table lists what a module does and — since "why is this its own Maven module?" is the question the
layout is designed to answer — the reason it is separate. The honest reasons fall into four kinds:
**dependency isolation** (it carries a library the community edition must not ship), **edition gating**
(the feature is paid, and the module boundary is the enforcement), **cycle avoidance / layering** (it
cannot live where you would first put it), and **seam anchoring** (it is the extension point something
else plugs into). Where a module exists mainly for structural regularity, the table says so.

#### Core

| Module | Edition | What it does | Why it is a module |
|---|---|---|---|
| `ignifyr-engine` | Community | The engine: reads with Spark, applies the mapping templates, routes results to a sink. Owns the job/mapping models and the community source/sink settings models (a plugin module defines its own, see `IgnifyrExtension.jobModelClasses`), the batch runtime, the CLI, and the `IgnifyrExtension` SPI. | It is the host, not a plugin — the one artifact every other module compiles against. Because the plugins depend on it, it cannot itself be the fat jar (that would be a cycle), which is why the assembly lives downstream in `ignifyr-cli`. Two gates keep it honest: the enforcer, which bans the enterprise libraries from its dependency tree, and its own test suite, which asserts that the engine's built-in extension contributes nothing but CLI commands and a local terminology service. |
| `ignifyr-common` | Community | Spark-free helpers below the engine: the app-version reader, onFHIR `SchemaDefinition` → `StructureDefinition` conversion, exception-chain flattening, and the `cst:` FHIRPath function library. | The only layer usable without Spark or the engine on the classpath, and the point where the `onfhir-common` / `onfhir-definition-commons` dependencies enter the build. Honestly a thin, largely historical split: nothing today would break if it folded into the engine. |

#### Source connectors — `ignifyr-connector-*` reads data **in**

| Module | Edition | What it does | Why it is a module |
|---|---|---|---|
| `ignifyr-connector-sql` | Community | Reads RDBMS tables/queries through Spark JDBC, and infers schemas from JDBC metadata for the server's schema-import flow. | Dependency isolation: JDBC drivers live here, not in the engine, so the driver set is a per-deployment choice. It ships PostgreSQL only — the enforcer bans the DB2 JCC driver from community modules, so proprietary drivers are added on the deployment classpath instead of bundled. |
| `ignifyr-connector-file` | Community | Reads the file system (local or `hdfs://`), handling path resolution, zip archives, streaming directories, and the `distinct` option. Owns the `FileSourceFormat` sub-SPI, shipping csv/tsv/parquet. | Seam anchoring. It carries no third-party dependency of its own — it exists so the engine ships no concrete reader, and so *file formats* are pluggable: because this module owns the format registry, adding JSON reading is a one-folder move with no change here or in the engine. |

#### Sinks — `ignifyr-sink-*` writes data **out**

| Module | Edition | What it does | Why it is a module |
|---|---|---|---|
| `ignifyr-sink-fhir` | Community | Writes mapped resources into a FHIR repository as transaction/batch bundles, with per-resource error attribution. Also supplies the FHIR-server-backed terminology and identity services. | Structural, and honest about it: `onfhir-client` stays an engine dependency for the settings model, so nothing is kept out of any jar. What the split buys is that the engine has **no privileged built-in sink** — `SinkProvider` is the single dispatch path — which is precisely what makes a new output target (OMOP) a pure module add. It also lets a file-only deployment omit the FHIR writer. |
| `ignifyr-sink-file` | Community | Writes to the file system, partitioned by resource type, over local or HDFS paths. Owns the `FileSinkFormat` sub-SPI (ndjson/csv/parquet) and the shared write machinery. | Seam anchoring: it is the compile anchor the enterprise Delta writer depends on and reuses, so `delta-spark` stays out of the community jar while both sinks share identical partitioning code. It was carved out of `ignifyr-connector-file` so that `connector-*` means sources only. |

#### Distribution — the shaded jar

| Module | Edition | What it does | Why it is a module |
|---|---|---|---|
| `ignifyr-cli` | Community | Shades the engine plus the community plugins into `ignifyr-engine-standalone.jar` (Main-Class `io.ignifyr.engine.Boot`). No source code of its own. | The assembly cannot live in the engine: the plugins depend on the engine, so the jar that bundles both must be built downstream of all of them. It is also the machine-checkable definition of the Community edition — and because it opts into the enforcer gate transitively, building it *proves* nothing in the community jar drags in a banned library. |

#### Features & tooling

| Module | Edition | What it does | Why it is a module |
|---|---|---|---|
| `ignifyr-testkit` | Community (test-only) | The shared test harness — `IgnifyrTestSpec`, `OnFhirTestContainer`, and the classpath fixtures (`/test-mappings`, `/test-schemas`, sample data) reused by suites across the reactor. | Three reasons: it must sit downstream of the engine to be usable by plugin test suites (so the engine must never depend on it); it declares the whole test toolchain at *compile* scope, so one test-scoped dependency hands a module scalatest, mockito, H2 and Testcontainers; and it is a Community artifact whose fixtures Enterprise suites consume — a direction that keeps working after the repo split, whereas the reverse could not. |

#### Enterprise Edition modules

These live in the private Ignifyr Enterprise repository (groupId `com.pontegra.ignifyr`), built on the
community artifacts above, and are shaded into the `ignifyr-server-standalone.jar`. They are listed so
that the feature tables below can name the module a capability comes from.

| Module | What it adds |
|---|---|
| `ignifyr-connector-kafka` | Reads Kafka topics as streaming or batch input, and translates Kafka client errors (e.g. unknown topic) into actionable job failures. |
| `ignifyr-connector-fhir-server` | Reads resources from a live FHIR API and exposes them as a Spark source, so an existing FHIR server can be the *input* of a mapping job. |
| `ignifyr-sink-omop` | Reserved skeleton for the upcoming **map-to-OMOP** feature — versioned OMOP CDM schemas, FK-ordered table writes, and OMOP-vocabulary terminology. Registers nothing yet. |
| `ignifyr-format-json` | Adds JSON and NDJSON as readable *source* formats, registered into the file connector's `FileSourceFormat` registry. |
| `ignifyr-format-delta` | Adds Delta Lake as a *sink* format for the file sink, and contributes the Spark session-extension and catalog settings Delta needs. |
| `ignifyr-runtime-streaming` | Runs mapping jobs as Spark structured-streaming queries: starts the queries, writes each micro-batch, and archives streamed input. |
| `ignifyr-runtime-scheduling` | Runs cron-scheduled batch jobs and owns the scheduled-execution state and last-sync-time files. |
| `ignifyr-server` | The Akka-HTTP REST API for managing projects, schemas, mappings, and job executions (Endpoint → Service → Repository), and the `ignifyr-server-standalone.jar` assembly. |
| `ignifyr-server-common` | Shared web-server configuration, CORS and error-handling interceptors, the REST error taxonomy, and the `IgnifyrServerExtension` SPI. |
| `ignifyr-redcap` | Turns a REDCap data dictionary into Ignifyr schemas — as the `extract-redcap-schemas` CLI command, as a server schema-import route, and as the `/redcap` proxy routes to the companion service. |
| `ignifyr-observability` | Encodes structured audit log markers as Logstash JSON and ships logs to Fluentd for the EFK stack. |
| `ignifyr-terminology-tools` | Offline tool that generates Ignifyr concept-map CSVs from an OMOP vocabulary database. |
| `ignifyr-rxnorm` | RxNorm REST API client plus `rxn:` FHIRPath functions for medication mappings. |

The reference configuration is
[ignifyr-engine/src/main/resources/application.conf](ignifyr-engine/src/main/resources/application.conf).
Modules with non-obvious internals carry their own `CLAUDE.md`; run `list-plugins` on either jar to see
what a given deployment actually has installed. How to build and test — including the short/long test
tiers and the checks that keep the edition boundary honest — is in
[test-flow/README.md](test-flow/README.md).

## Requirements
To run Ignifyr, you need:

* Java 11.0.2
* Scala 2.13
* An HL7 FHIR repository if you would like to persist the created resources (e.g., [Repofyr](https://repofyr.io))

## Supported Data Sources

Ignifyr can read data from the following data source types (the module providing each reader is in
brackets):

| Data source | Formats / notes | Module | Edition |
|---|---|---|---|
| File System | CSV, TSV, Parquet — plus zip archives, and `hdfs://` paths | `ignifyr-connector-file` | Community |
| File System | JSON, NDJSON | `ignifyr-format-json` | Enterprise |
| RDBMS | PostgreSQL driver bundled; other JDBC drivers go on the deployment classpath | `ignifyr-connector-sql` | Community |
| Apache Kafka | streaming or batch | `ignifyr-connector-kafka` | Enterprise |
| FHIR Server | Repofyr, HAPI FHIR Server, Firely Server, etc. | `ignifyr-connector-fhir-server` | Enterprise |
| REDCap | records arrive through Kafka from the `ignifyr-redcap-service` companion service (Enterprise), so the **Kafka** connector does the reading; `ignifyr-redcap` adds data-dictionary → schema extraction and the `/redcap` routes | `ignifyr-connector-kafka` + `ignifyr-redcap` | Enterprise |

And it can write the mapped results to:

| Sink | Formats | Module | Edition |
|---|---|---|---|
| FHIR repository | transaction/batch bundles to any FHIR endpoint | `ignifyr-sink-fhir` | Community |
| File System | NDJSON, CSV, Parquet | `ignifyr-sink-file` | Community |
| File System | Delta Lake | `ignifyr-format-delta` | Enterprise |
| OMOP CDM | *upcoming* — see `ignifyr-sink-omop` | `ignifyr-sink-omop` | Enterprise |

> [!NOTE]
> A mapping job naming a community source or sink whose module is not installed still **parses** — those
> settings classes live in the engine — and fails when the job runs, with a message naming the module to
> install. An Enterprise source or sink type is defined by its own module, so without that module the job
> fails to parse, naming the unknown `jsonClass`. `list-plugins` (see [CLI Commands](#cli-commands))
> reports what the running jar actually has.

## Usage

Ignifyr can be utilized via the standalone Engine (CLI/Batch, Community) or the Web Server (REST API,
Enterprise).

### 1. Ignifyr Engine (CLI & Batch)

When started as a standalone tool, the engine can run in two modes based on arguments:

- `cli`: Starts the interactive Command Line Interface (Default).
- `run`: Runs a configured mapping-job as a batch process and shuts down. `run` command accepts the following parameters:
  - `--job`: The path to the mapping-job to be executed. If provided, overrides the path provided to the JVM as the configuration parameter.
  - `--db` (alias `--db-path`): The path to the database folder that is used for scheduled jobs. If provided, overrides the path provided to the JVM as the configuration parameter.
  - `--skip-write` (no value): Runs the mappings and logs their results, but writes nothing to the sink, does not validate the sink and does not archive the source files. Use it to test a job. Only batch jobs support it; a streaming or scheduled job is rejected.

  The mappings and schemas folders are not command-line options; they are always read from the
  [configuration](#configurations) (`ignifyr.mappings.repository.folder-path` and
  `ignifyr.mappings.schemas.repository.folder-path`, defaulting to `mappings` and `schemas`, resolved
  against `context-path`). To point a single run elsewhere, override them on the JVM, e.g.
  `-Dignifyr.mappings.repository.folder-path=/data/my-mappings`.
- `extract-redcap-schemas`: Extracts schemas from a REDCap data dictionary. Provided by the `ignifyr-redcap` module (Enterprise); without it, the command reports the module to install. `extract-redcap-schemas` command accepts the following parameters:
  - `--data-dictionary`: The path to the REDCap data dictionary
  - `--definition-root-url`: The root url of FHIR resources
  - `--encoding`: The encoding of CSV file whose default value is UTF-8 (OPTIONAL)
  
#### CLI Commands

Once the interactive CLI is up, the following commands are available:
- `help`: Displays the help text and see the available commands and their use. Installed extension modules append their own commands to it.
- `load <path>`: Load the Mapping Job definition file from the path.
- `reload`: Reload the mapping definitions from their source into the mapping repository.
- `run [<url>|<name>]`: Run the task(s). Without a parameter, all task of the loaded Mapping Job are run. A specific task can be indicated with its name or URL. (Alias: `execute`.)
- `list`: Show jobs with at least one running mapping.
- `list-plugins`: Show the installed extension modules and everything they contribute — connectors, sinks, file formats, terminology/identity services, CLI commands, schema inferrers, and the streaming/scheduling capabilities. The quickest way to tell which edition/plugins a given jar carries.
- `stop`: Stop the execution of the Mapping Job (if any) or a specific Mapping Task associated with a job.
- `exit`: Exit the program. (Alias: `quit`.)

After the app is up and running, these commands are ready to be executed.
If there is no mapping job loaded initially, firstly, a mapping job needs to be loaded with the command `load <mapping-job-path>`.
This command loads the mapping job located in the path. After that, the mapping job can be run with the command `run`.

### 2. Ignifyr Server (REST API) — Enterprise
The server, part of the Enterprise Edition, provides a REST API to manage the lifecycle of mapping projects.
* **Base URL:** http://<host>:8085/ignifyr (default)
* **API Documentation:** [SwaggerHub API Docs](https://app.swaggerhub.com/apis-docs/toFHIR/toFHIR-Server/)

### Configurations
 
Ignifyr uses HOCON-based configuration. Below is a snippet of the standard ignifyr.conf structure:

```conf
ignifyr {

  # A path to a directory from where any File system readings should use within the mappingjob definition.
  # This should be pointed to the root folder of the definitions.
  context-path = "ignifyr-definitions"

  mappings = {

    # The repository where the mapping definition are kept.
    repository = {
      folder-path = "mappings"
    }

    # Configuration of the schemas used in the mapping definitions.
    schemas = {
      repository = { # The repository where the schema definitions are kept.
        folder-path = "schemas"
      }
      # Specific FHIR version used for schemas in the schema repository.
      # Represents fhirVersion field in the standard StructureDefinition e.g. 4.0.1, 5.0.0
      fhir-version = "4.0.1"
    }

    contexts = {
      # The repository where the context definitions are kept.
      repository = {
        folder-path = "mapping-contexts"
      }
    }

    # Timeout for each mapping execution on an individual input record
    timeout = 5 seconds
  }

  mapping-jobs = {
    repository = { # The repository where the job definitions are kept.
      folder-path = "mapping-jobs"
    }
    # Absolute path to the JSON file for the MappingJob definition to load at the beginning
    # initial-job-file-path = "mapping-jobs/project1-mappingjob.json"

    # Number of partitions to repartition the source data before executing the mappings for the mapping jobs
    # numOfPartitions = 10

    # Maximum number of records for batch mapping execution, if source data exceeds this it is divided into chunks
    # maxChunkSize = 10000
  }

  terminology-systems = {
    # The path to the folder where Terminology System files (config files, CodeSystems, ConceptMaps etc.) are kept.
    folder-path = "terminology-systems"
  }

  archiving = {
    # Folder to keep erroneous records
    erroneous-records-folder = "erroneous-records-folder"
    
    # Folder to keep archived files
    archive-folder = "archive-folder"
    
    # Frequency in milliseconds to run the archiving task for file streaming jobs
    stream-archiving-frequency = 5000
  }

  # Settings for FHIR repository writer
  fhir-server-writer {
    # The # of FHIR resources in the group while executing (create/update) a FHIR batch operation.
    batch-group-size = 50
  }

  # Database folder of Ignifyr (e.g., to maintain synchronization times for scheduled jobs)
  db-path = "ignifyr-db"
}

# Spark configurations
spark = {
  app.name = "DataTools4Heart Data Integration Suite"
  master = "local[4]"
  # Directory to store Spark checkpoints
  checkpoint-dir = "checkpoint"
}

akka = {
  daemonic = "on"
}
```

> [!NOTE]
> **How the `spark { }` block combines with plugin settings.** Some plugin modules need their own Spark
> settings (the Delta sink, for example, registers a Spark session extension). Your `spark { }` block
> takes precedence over those for ordinary single-valued settings. The exception is the handful of keys
> Spark reads as a *comma-separated list of things to register* — `spark.sql.extensions`,
> `spark.plugins`, `spark.jars*`, and the listener keys — where your values and the plugins' are **merged**
> rather than replaced. So adding your own `spark.sql.extensions` (for Iceberg, Sedona, a custom
> optimizer rule) will not disable an extension a plugin needs.

Considering the configuration file defined above, Ignifyr can be utilized with a folder structure like the following:

```html
ignifyr-definitions (root folder of definitions)
├── mappings
│   ├── project1
│   │   ├── mapping1.json
│   │   ├── mapping2.json
│   │   └── ...
├── mapping-jobs
│   ├── project1
│   │   ├── mapping-job1.json
│   │   ├── mapping-job2.json
│   │   └── ...
├── schemas
│   ├── project1
│   │   ├── schema1.json
│   │   ├── schema2.json
│   │   └── ...
├── mapping-contexts
│   ├── project1
│   │   ├── context1.json
│   │   ├── context2.json
│   │   └── ...
├── terminology-systems
│   ├── terminology1
│   ├── ├── ConceptMap1.csv
│   ├── ├── CodeSystem1.csv
│   └── ...
└── ignifyr.conf
```

You are free to organize the definitions in any way you like and arrange the configuration file accordingly.
However, we are suggesting to keep the definitions in a folder structure as shown above to keep the definitions organized and easy to manage.

## Definitions Used in Ignifyr

### Project

A container for schemas, mappings, mapping contexts, and mapping jobs. It organizes definitions into logical groups.

Please note that, terminology systems are not included in a project. They are defined separately and can be used in any project.

### Schema

A schema is a definition of the structure of the source data. It is used to validate the source data and to provide the context for the mappings.
They are nothing but the simple HL7 FHIR StructureDefinition resources that are defined in JSON format.

### Mapping

An example of a simple mapping definition file:
```json
{
  "url": "https://aiccelerate.eu/fhir/mappings/project1/patient-mapping",
  "name": "patient-mapping",
  "title": "Mapping of patient data to Patient FHIR Resource",
  "source": [{
    "alias": "patient",
    "url": "https://aiccelerate.eu/fhir/schemas/project1/patient"
  }],
  "mapping": [
    {
      "expression": {
        "name": "result",
        "language": "application/fhir-template+json",
        "value": {
          "resourceType": "Patient",
          "id": "{{mpp:getHashedId('Patient',pid)}}",
          "meta": {
            "source": "{{%sourceSystem.sourceUri}}"
          },
          "active": true,
          "identifier": [
            {
              "use": "official",
              "system": "{{%sourceSystem.sourceUri}}",
              "value": "{{pid}}"
            }
          ],
          "gender": "{{gender}}",
          "birthDate": "{{birthDate}}",
          "deceasedDateTime": "{{? deceasedDateTime}}",
          "address": {
            "{{#pc}}": "{{homePostalCode}}",
            "{{?}}": [
              {
                "use": "home",
                "type": "both",
                "postalCode": "{{%pc}}"
              }
            ]
          }
        }
      }
    }
  ]
}
```

The json snippet above illustrates the structure of an example mapping. On the top, the `url`, `name`, and `title` fields are the metadata of the mapping.
The `source` field is used to define the source schema of the mapping. The `mapping` field is the list of mapping definitions.
The real magic in mappings happens in the `expression` fields (e.g. {{`<expression>`}} ).
Ignifyr uses the expression to generate the FHIR resources by using [onfhir-template-engine](https://github.com/srdc/onfhir-libs/tree/main/onfhir-template-engine).
By doing so, it can generate the FHIR resources based on the source data.

For example, considering `{{gender}}` expression, it refers to "gender" column in the source data. 
When this mapping is executed, each record at "gender" column in the source replaces the expression and generate the FHIR resources.

The json keys in the `expression.value` represent the FHIR resource attributes. That is, we write the FHIR resource structure
by providing the values through a template language where we can access the fields of the source data as defined by its schema.
On the value sides, `onfhir-template-engine` is used to interpret the source data. You can get more information how template engine works on its [GitHub page](https://github.com/srdc/onfhir-libs/tree/main/onfhir-template-engine).

### Mapping Context 

Mapping contexts are CSV files that have a specific format. It refers to the additional information utilized when defining a mapping.
Mapping contexts facilitates easy exchange and integration between healthcare concepts and referenced within the mappings.

They provide two functionalities:
* **Concept Map:** Enables the mapping of different health-care concept codes between different systems.
* **Unit Conversion:** Enables mappings between different healthcare data units. e.g. mg &rarr; g, cm &rarr; m, etc.

#### 1. Concept Map

Let's say that the data source has its own EHR codes for the concepts, and we want to map these codes to the ICD10-PCS codes.
In this case, we can define and use a concept map file to define the mappings between the source codes and the ICD10-PCS codes:

| source_code | target_code | target_display                                      |
|-------------|-------------|-----------------------------------------------------|
| xyz         | 02YA0Z0     | Transplantation of Heart, Allogeneic, Open Approach |
| abc         | 02YA0Z1     | Transplantation of Heart, Syngeneic, Open Approach  |
| 123         | 02YA0Z2     | Transplantation of Heart, Zooplastic, Open Approach |
| ...         |             |                                                     |

Assume, we have a mapping context file named `heart-transplantation-code-map.csv` and it is located in the `mapping-contexts/project1` folder.
Then, we can use this context file in the mapping definitions as follows:

```json
{
  "id" : "procedure-mapping",
  "url" : "https://aiccelerate.eu/mappings/amc/procedure-mapping",
  "name" : "procedure-mapping",
  "title" : "Mapping of EHR table to Procedure FHIR Resource",
  "source" : [ {
    "alias" : "Procedure",
    "url" : "https://aiccelerate.eu/schemas/project1/procedure",
    "joinOn" : [ ]
  } ],
  "context" : {
    "heartTransplantationCodeMap" : {
      "category" : "concept-map",
      "url" : "$CONTEXT_REPO/project1/heart-transplantation-code-map.csv"
    }
  },
  "variable" : [ ],
  "mapping" : [ {
    "expression" : {
      "name" : "result",
      "language" : "application/fhir-template+json",
      "value" : {
        "resourceType" : "Procedure",
        "category" : [ {
          "coding" : [ {
            "system" : "http://snomed.info/sct",
            "code" : "387713003",
            "display" : "Surgical procedure (procedure)"
          } ]
        } ],
        "code": {
          "coding": [
            {
              "system": "http://hl7.org/fhir/sid/icd-10-pcs",
              "code": "{{ mpp:getConcept(%heartTransplantationCodeMap, type_code, 'target_code') }}",
              "display": "{{ mpp:getConcept(%heartTransplantationCodeMap, type_code, 'target_display') }}"
            }
          ]
        }
      }
    }
  } ]
}
```
Firstly, context file is registered in the mapping definition with the `context` field and a name is given to it (e.g. `heartTransplantationCodeMap`).
Then, the `mpp:getConcept` function is used to get the target code and display name from the context file. 
The first parameter of the function is the name given for the context file, the second parameter is the source code, and the third parameter is the target field name.
When the mapping is executed, type_code is replaced with the actual source code and the function returns the target code and display name.
For example, if the source code is `xyz`, the function returns `02YA0Z0` for the code and `Transplantation of Heart, Allogeneic, Open Approach` for the display name.

#### 2. Unit Conversion

Another use case for mapping contexts is the unit conversion. Let's say that the source data has the lab results in different units, and we want to convert them.
In this case, we can define and use a unit conversion file to define the conversion between units:

| source_code | source_unit | target_unit | conversion_function |
|-------------|-------------|-------------|---------------------|
| 5060        | g/L         | mg/L        | $this * 1000        |
| 8001        | g/dL        | g/L         | $this * 10          |
| ...         |             |             |                     |

Similarly, assume, we have a mapping context file named `lab-unit-conversion.csv` and it is located in the `mapping-contexts/project1` folder.
Then, we can use this context file in the mapping definitions as follows:

```json
{
  "id" : "lab-mapping",
  "url" : "https://aiccelerate.eu/mappings/amc/lab-mapping",
  "name" : "lab-mapping",
  "title" : "Mapping of EHR table to Observation FHIR Resource",
  "source" : [ {
    "alias" : "Lab",
    "url" : "https://aiccelerate.eu/schemas/project1/lab",
    "joinOn" : [ ]
  } ],
  "context" : {
    "labUnitConversion" : {
      "category" : "unit-conversion",
      "url" : "$CONTEXT_REPO/project1/lab-unit-conversion.csv"
    }
  },
  "variable" : [ ],
  "mapping" : [ {
    "expression" : {
      "name" : "result",
      "language" : "application/fhir-template+json",
      "value" : {
        "resourceType" : "Observation",
        "code" : {
          "coding" : [ {
            "system" : "http://loinc.org",
            "code" : "789-8",
            "display" : "Erythrocytes [#/volume] in Blood by Automated count"
          } ]
        },
        "valueQuantity" :"{{ mpp:convertAndReturnQuantity(%labResultUnitConversion, lab_code, value, unit) }}"
      }
    }
  } ]
}
```
Similarly, context file is registered in the mapping definition with the `context` field and a name is given to it (e.g. `labUnitConversion`).
Then, the `mpp:convertAndReturnQuantity` function is used to convert the lab result value to the target unit. 
The first parameter of the function is the name of the context file, the second parameter is the lab code (e.g. 5060), 
the third parameter is the measured value for that lab, and the fourth parameter is the source unit (e.g. g/L).
When the mapping is executed, the function converts the value to the target unit by applying the conversion function and returns the converted value.
For example, if the `lab_code` is `5060`, the `value` is `5`, and the `unit` is `g/L`, the function calculates `5000` for the value in the `mg/L` unit 
and returns a HL7 FHIR Quantity object:

```json
{
  "value": 5000,
  "unit": "mg/L",
  "system": "http://unitsofmeasure.org",
  "code": "mg/L"
}
```


### Mapping Job


#### Data Sources
##### File System

###### 1. Batch Mode

If not set explicitly, Ignifyr uses the batch mode by default. In the batch mode, Ignifyr goes through these steps:
1. Reads the source data
2. Executes the mappings
3. Persists the generated FHIR resources to the sink
4. Optionally, archives the source data and save erroneous records
5. Exits

This means that your source data is expected to be a static file/table or a set of files/tables that are not expected to be updated during the execution of the mapping job.
Example of a Mapping Job definition file with csv source type:

```json
{
  "id": "project1-mapping-job",
  "sourceSettings": {
    "source": {
      "jsonClass": "FileSystemSourceSettings",
      "name": "project1-source",
      "sourceUri": "https://aiccelerate.eu/fhir/data-integration-suite/project1-data",
      "dataFolderPath": "test-data/project1"
    }
  },
  "sinkSettings": {
    "jsonClass": "FhirRepositorySinkSettings",
    "fhirRepoUrl": "http://localhost:8081/fhir"
  },
  "dataProcessingSettings": {
    "saveErroneousRecords": false,
    "archiveMode": "off"
  },
  "mappings": [
    {
      "name": "patient-mapping",
      "mappingRef": "https://aiccelerate.eu/fhir/mappings/project1/patient-mapping",
      "sourceBinding": {
        "patient": {
          "jsonClass": "FileSystemSource",
          "path": "patients.csv",
          "contentType": "csv"
        }
      }
    },
    {
      "name": "practitioner-mapping",
      "mappingRef": "https://aiccelerate.eu/fhir/mappings/project1/practitioner-mapping",
      "sourceBinding": {
        "practitioner": {
          "jsonClass": "FileSystemSource",
          "path": "practitioners.csv",
          "contentType": "csv"
        }
      }
    }
  ]
}
```

The json snippet above illustrates the structure of an example mapping job. Let's go through the fields one by one:
- `sourceSettings` defines the source settings of the mapping job. The source settings config is used to connect to the source data.
  In this case, the source type of data is file system source and `dataFolderPath` defines the path of the source data folder.
  Please note that, `dataFolderPath` is a relative path to the root folder of the definitions. Also, it may be an absolute path as well.
- Assuming onFHIR is running on the system, `sinkSettings` defines FHIR endpoint configurations to connect to the data destination.
- `dataProcessingSettings` is used to define post-processes after the mapping is completed. It is explained in detail here: [Data Processing Settings](#Archiving)
- `mappings` is a list of mapping tasks that mapping job includes. For a purpose of illustration, the mapping job above includes two mappings:
  - https://aiccelerate.eu/fhir/mappings/project1/patient-mapping
  - https://aiccelerate.eu/fhir/mappings/project1/practitioner-mapping

Let's take the patient mapping as an example from the mappings list.
`https://aiccelerate.eu/fhir/mappings/project1/patient-mapping` is the unique reference URL of the mapping repository.
Assuming this URL refers to the first mapping example in the mapping section: [patient-mapping](#Mapping), this means that patient mapping will be
executed with the source data defined in the `sourceBinding` part.
Inside `sourceBinding` part, `patient` is the alias of the source data, and it should match with the `alias` used in `source` field in the mapping.

`jsonClass` specifies the type of the source, and `path` is the file name of the source data.
Since we have FileSystemSourceSettings defined in the source settings, `jsonClass`es of mappings are expected to be FileSystemSource.
For the file source mappings,
the `path` field should be specified, and it represents the data source file of each mapping.
This field is a relative path to the `dataFolderPath` defined in the source settings.

###### 2. Streaming Mode

Ignifyr supports streaming of file system in case you want to continuously monitor the changes on the source data and stream the
newcoming/updated data to Ignifyr mapping executions. This can be done with the `asStream` config parameter of the source.
If it is set to `true`, Ignifyr will monitor the FileSystemSource files defined at `path` paths and trigger the mapping
executions in case the files are updated. Ignifyr automatically marks the processed data source files and only processes the newcoming/updated records.

Ignifyr goes through these steps in the streaming mode:
1. Reads the initial existing source data
2. Executes the mappings
3. Persists the generated FHIR resources to the sink
4. Optionally, archives the source data and save erroneous records
5. Monitors the source data for changes
6. Executes the mappings for the newcoming/updated data
7. Persists the generated FHIR resources to the sink
8. Optionally, archives the source data and save erroneous records
9. Repeats the steps 5-8

Example of a Mapping Job definition file with csv source type in streaming mode:

```json
{
  "id": "project1-mapping-job",
  "sourceSettings": {
    "source": {
      "jsonClass": "FileSystemSourceSettings",
      "name": "project1-source",
      "sourceUri": "https://aiccelerate.eu/data-integration-suite/project1-data",
      "dataFolderPath": "D:/ignifyr/data",
      "asStream": true
    }
  },
  "sinkSettings": {
    "jsonClass": "FhirRepositorySinkSettings",
    "fhirRepoUrl": "http://localhost:8081/fhir"
  },
  "dataProcessingSettings": {
    "saveErroneousRecords": false,
    "archiveMode": "off"
  },
  "mappings": [
    {
      "name": "patient-mapping",
      "mappingRef": "https://aiccelerate.eu/fhir/mappings/project1/patient-mapping",
      "sourceBinding": {
        "patient": {
          "jsonClass": "FileSystemSource",
          "path": "patients",
          "contentType": "csv"
        }
      }
    }
  ]
}
```

The json snippet above illustrates the structure of an example mapping job in streaming mode.
Similar to the batch mode, most of the fields are the same. The only differences are:
- `asStream` field in the source settings
- `path`  in the source binding of the mapping. `path` should be the name of the **folder** this time, and it is where Ignifyr will monitor the changes.

##### SQL

Similarly, if we had a source with SQL type, `sourceSettings` and `mappings` part would look like this:
```json
{
  "sourceSettings": {
    "source": {
      "jsonClass": "SqlSourceSettings",
      "name": "project1-source",
      "sourceUri": "https://aiccelerate.eu/data-integration-suite/project1-data",
      "dataFolderPath": "jdbc:postgresql://localhost:5432/db_name",
      "username": "postgres",
      "password": "postgres"
    }
  }
}
```
```json
{
  "name": "location-sql-mapping",
  "mappingRef": "https://aiccelerate.eu/fhir/mappings/location-sql-mapping",
  "sourceBinding": {
    "source": {
      "jsonClass": "SqlSource",
      "tableName": "location"
    }
  }
}
```
We can give a table name with the `tableName` field, as well as write a query with the `query` field:
```json
{
  "name": "location-sql-mapping",
  "mappingRef": "https://aiccelerate.eu/fhir/mappings/location-sql-mapping",
  "sourceBinding": {
    "source": {
      "jsonClass": "SqlSource",
      "query": "select * from location"
    }
  }
}
```

##### Kafka

Mapping job and mapping examples shown below for the streaming type of sources like Kafka:
```json
{
  "sourceSettings": {
    "source": {
      "jsonClass": "KafkaSourceSettings",
      "name": "project1-source",
      "sourceUri": "https://aiccelerate.eu/data-integration-suite/project1-data",
      "bootstrapServers": "localhost:9092,localhost:9093"
    }
  }
}
```
```json
{
  "name": "location-sql-mapping",
  "mappingRef": "https://aiccelerate.eu/fhir/mappings/location-sql-mapping",
  "sourceBinding": {
    "source": {
      "jsonClass": "KafkaSource",
      "topicName": "patients",
      "options": {
        "startingOffsets": "earliest"
      }
    }
  }
}
```
Ignifyr only considers the value field of kafka topics. Therefore, when you subscribe a topic,
Ignifyr waits for string-type data but in correct JSON format.
For example, when you want to use the data in the topic, you should publish the data in the following format:
```json
{
  "pid": "p1",
  "gender": "male",
  "birthDate": "1995-11-10"
}
```
##### RedCAP
Ignifyr integrates seamlessly with RedCAP through the `ignifyr-redcap-service` integration service of the Enterprise Edition
(formerly [tofhir-redcap](https://github.com/srdc/tofhir-redcap)), which publishes RedCAP records to Kafka. Utilize the same configuration approach as described for Kafka, with a few key considerations:
- **Source Configuration**: In the mapping job's **sourceSettings**, use a plain Kafka source configuration. Environment
variables (e.g. `${REDCAP_PROJECT_ID}`) referenced in `sourceUri` or in the topic names are resolved automatically. Here's an example JSON configuration:

```json
{
  "sourceSettings": {
    "source": {
      "jsonClass": "KafkaSourceSettings",
      "name": "project1-source",
      "sourceUri": "https://aiccelerate.eu/data-integration-suite/project1-data",
      "bootstrapServers": "localhost:9092,localhost:9093"
    }
  }
}
```
- **Topic Name**: While defining the topic name for a mapping source binding within a mapping job, use the name generated by 
`ignifyr-redcap-service` for the corresponding RedCAP project: the project id and the instrument name joined by a hyphen,
e.g. `27-patient`. For detailed instructions, refer to the README of the integration service, which ships with the Enterprise Edition.

##### FHIR Server

Below is an example configuration for mapping jobs using a FHIR Server data source:

```json
{
  "sourceSettings" : {
    "source" : {
      "jsonClass" : "FhirServerSourceSettings",
      "name" : "pilot1-source",
      "sourceUri" : "https://aiccelerate.eu/data-integration-suite/pilot1-data",
      "serverUrl" : "http://localhost:8082",
      "securitySettings": {
        "jsonClass": "BasicAuthenticationSettings",
        "username": "username",
        "password": "password"
      }
    }
  }
}
```
In addition to specifying the server URL (**serverUrl**), you can configure security settings via the **securitySettings** field.

Within the mapping source, you can define the resource type (e.g., Patient, Observation) and apply filters using a query string:
```json
{
  "name": "patient-mapping",
  "mappingRef" : "https://aiccelerate.eu/fhir/mappings/pilot1/patient-mapping",
  "sourceBinding" : {
    "source" : {
      "jsonClass" : "FhirServerSource",
      "resourceType" : "Patient",
      "query": "gender=male&birtdate=ge1970"
    }
  }
}
```
#### Custom Options

Since Ignifyr uses Apache Spark in its core, you can give any option that is supported by Apache Spark.
Available options for different source types can be found in the following links:
- File System
  - CSV & TSV: https://spark.apache.org/docs/3.4.1/sql-data-sources-csv.html#data-source-option
  - JSON: https://spark.apache.org/docs/3.4.1/sql-data-sources-json.html#data-source-option
  - Parquet: https://spark.apache.org/docs/3.4.1/sql-data-sources-parquet.html#data-source-option
- SQL: https://spark.apache.org/docs/3.4.1/sql-data-sources-jdbc.html#data-source-option
- Apache Kafka: https://spark.apache.org/docs/3.4.1/structured-streaming-kafka-integration.html

To give any spark option, you can use the `options` field in the source binding of the mapping in a mapping job.

```json
{
  "name": "patient-mapping",
  "mappingRef": "https://aiccelerate.eu/fhir/mappings/project1/patient-mapping",
  "sourceBinding": {
    "source": {
      "jsonClass": "FileSystemSource",
      "path": "patients",
      "contentType": "csv",
      "options": {
        "sep": "\\t" // tab separated file
      }
    }
  }
}
```

#### Multiple Data Sources

In a mapping job, you can read data from more than one data source. Let's consider a scenario where you have different sources
- **patient-test-data:** Contains patient identifiers, specifically the "pid" column and this information is in a CSV 
  file named `patient.csv` under `/test-data` folder.
- **patient-gender-test-data:** Contains gender information for patients, including "pid" and "gender" columns and this
  information is served from a Postgres database.

We'll implement a mapping job that utilizes these two CSV files as data sources and runs a simple patient mapping.

##### 1. Define Source Settings

First, define the source settings pointing to the two different data sources:

```json
{
  "sourceSettings" : {
    "patientSource" : {
      "jsonClass" : "FileSystemSourceSettings",
      "name" : "patient-test-data",
      "sourceUri" : "http://test-data",
      "dataFolderPath" : "/test-data",
      "asStream" : false
    },
    "genderSource" : {
      "jsonClass" : "SqlSourceSettings",
      "name" : "patient-gender-test-data",
      "sourceUri" : "http://test-data-gender",
      "databaseUrl" : "jdbc:postgresql://localhost:5432/test-data-gender",
      "username" : "user",
      "password" : "pass"
    }
  }
}

```
The `patientSource` points to the `test-data` directory in the file system, while the `genderSource` points to a relational
database, actually a query result or a table name. It is important to note that the mapping definitions are not directly connected to
the data sources. `genderSource` can point to a folder which means that the same mapping can be executed on the 
data read from different sources. 

```json
{
  "sourceSettings" : {
    "patientSource" : {
      "jsonClass" : "FileSystemSourceSettings",
      "name" : "patient-test-data",
      "sourceUri" : "http://test-data",
      "dataFolderPath" : "/test-data",
      "asStream" : false
    },
    "genderSource" : {
      "jsonClass" : "FileSystemSourceSettings",
      "name" : "patient-gender-test-data",
      "sourceUri" : "http://test-data-gender",
      "dataFolderPath" : "/test-data-gender",
      "asStream" : false
    }
  }
}
```

##### 2. Specify Source Bindings
Next, specify the source bindings for your mappings in the job. Here's an example:

```json
{
  "mappings" : [ {
    "name": "patient-mapping-with-two-sources",
    "mappingRef" : "http://patient-mapping-with-two-sources",
    "sourceBinding" : {
      "patient" : {
        "jsonClass" : "FileSystemSource",
        "path" : "patient-simple.csv",
        "contentType" : "csv",
        "options" : { },
        "sourceRef": "patientSource"
      },
      "patientGender" : {
        "jsonClass" : "SqlSource",
        "query" : "SELECT pid, gender FROM patient_gender",
        "sourceRef": "genderSource"
      }
    }
  } ]
}
```
In this example, `patient-simple.csv` is used for the `patient` mapping source, while an SQL query result is used for the `patientGender` mapping source. 
Since the mapping job has more than one data source, we should specify the source reference in the mapping source binding using `sourceRef` field.
Here, `patient` source reads the csv file from `patientSource` whereas `patientGender` source reads the result of an SQL query from
`genderSource`.

If `sourceRef` is skipped or does not match any entry in the `sourceSettings`, the first source specified in `sourceSettings` will be used to
read the data.

If the `genderSource` was connected to file system in the job definition, the `sourceBinding` parameters would be as in the following:
```json
{
  "mappings" : [ {
    "name": "patient-mapping-with-two-sources",
    "mappingRef" : "http://patient-mapping-with-two-sources",
    "sourceBinding" : {
      "patient" : {
        "jsonClass" : "FileSystemSource",
        "path" : "patient-simple.csv",
        "contentType" : "csv",
        "options" : { },
        "sourceRef": "patientSource"
      },
      "patientGender" : {
        "jsonClass" : "FileSystemSource",
        "path" : "patient-gender-simple.csv",
        "contentType" : "csv",
        "options" : { },
        "sourceRef": "genderSource"
      }
    }
  } ]
}
```

##### 3. Join Data Sources
Finally, in the mapping definition, join these two data sources:

```json
{
  "source": [
    {
      "alias": "patient",
      "url": "http://patient-schema",
      "joinOn": [
        "pid"
      ]
    },
    {
      "alias": "patientGender",
      "url": "http://patient-gender",
      "joinOn": [
        "pid"
      ]
    }
  ]
}
```
Specify the corresponding schema URL for each data source. Use the same source keys (**patient** and **patientGender**) as alias to 
match schemas with the data sources provided in the mapping job definition. Then, join the two source data using the **pid** 
column available in both.

The first source i.e. **patient** is called the main schema, and its fields are accessible directly in the mapping. 
To access attributes of other schemas (side schemas), use the **%** operator (e.g., **%patientGender**).

Here's an example mapping that utilizes the **pid** field from the **patient** source and the **gender** information from the **patientGender** source:

```json
{
  "gender": "{{%patientGender.gender}}",
  "id": "{{pid}}"
}
```
Please refer to the following files for full definitions:

- [patient-simple.csv](ignifyr-connector-file/src/test/resources/test-data/patient-simple.csv)
- [patient-gender-simple.csv](ignifyr-connector-file/src/test/resources/test-data-gender/patient-gender-simple.csv)
- [patient-mapping-job-with-two-sources.json](ignifyr-connector-file/src/test/resources/patient-mapping-job-with-two-sources.json)
- [patient-mapping-with-two-sources.json](ignifyr-testkit/src/main/resources/test-mappings/patient-mapping-with-two-sources.json)

#### Sink Settings

Ignifyr supports persisting the generated FHIR resources to a FHIR repository. The sink settings are defined in the mapping job definition file.
The following example shows the sink settings for a FHIR repository:

```json
{
  "sinkSettings": {
    "jsonClass": "FhirRepositorySinkSettings",
    "fhirRepoUrl": "http://localhost:8081/fhir"
  }
}
```

Or you can use a local file system to persist the generated FHIR resources:

```json
{
  "sinkSettings": {
    "jsonClass": "FileSystemSinkSettings",
    "path": "sink/project1",
    "contentType": "csv"
  }
}
```

Each sink is a plugin module, and the file sink's output formats are pluggable in turn:

| `jsonClass` | `contentType` | Provided by | Edition |
|---|---|---|---|
| `FhirRepositorySinkSettings` | — | `ignifyr-sink-fhir` | Community |
| `FileSystemSinkSettings` | `ndjson`, `csv`, `parquet` | `ignifyr-sink-file` | Community |
| `FileSystemSinkSettings` | `delta` | `ignifyr-format-delta` | Enterprise |

A `contentType` whose handler is not installed fails on the first write with a message naming the module
to install; the job itself still parses.

#### Terminology Service
[A FHIR terminology service](https://hl7.org/fhir/terminology-service.html) can be automatically used by Ignifyr to handle
concept lookup and concept map operations. If a terminology service is configured, mapping definitions can use lookup and
translation services for codes/values of codesystems/valuesets.

An available FHIR terminology service can be configured as in the following:

```json
...
  "terminologyServiceSettings": {
    "jsonClass": "FhirRepositorySinkSettings",
    "fhirRepoUrl": "https://fhir.loinc.org/",
    "securitySettings":{
        "jsonClass": "BasicAuthenticationSettings",
        "username": "???",
        "password": "???"       
    }   
  }
...
```

Ignifyr provides a `LocalFhirTerminologyService` which allows to use text files for concept details and translations. You
can provide the concept map files or code/codesystem details by configuring the terminology service as in the following
example:

```json
...
  "terminologyServiceSettings": {
    "jsonClass": "LocalFhirTerminologyServiceSettings",
    "folderPath": "./src/test/resources/terminology-service",
    "conceptMapFiles": [
      {
        "fileName": "sample-concept-map.csv",
        "conceptMapUrl": "http://example.com/fhir/ConceptMap/sample1",
        "sourceValueSet": "http://terminology.hl7.org/ValueSet/v2-0487",
        "targetValueSet": "http://snomed.info/sct?fhir_vs"
      }
    ],
    "codeSystemFiles": [
      {
        "fileName":"sample-code-system.csv",
        "codeSystem": "http://snomed.info/sct"
      }
    ]
  }
...
```
Ignifyr's FHIRPath engine provides two functions becoming available when a terminology service is configured:
- `trms:lookupDisplay`: Lookup the display name of a given code and code system
- `trms:translateToCoding`: Translate the give code+codesystem within a valueset to the target code+codesystem
  (formatted as [Coding](https://hl7.org/fhir/datatypes.html#Coding)) within target valueset.

The following example gets the display name in German (`de` column) of the code 119323008 defined in SNOMED code system:
```json
{
  "system": "http://snomed.info/sct",
  "code": "111",
  "display": "{{ trms:lookupDisplay('119323008','http://snomed.info/sct','de') }}"
}
```

Similarly, when you want to translate the given code+system according to the given source value set and (optional) target value set,
you can do something like this. This creates a FHIR-Coding object automatically and replaces the expression.
```json
{
  "coding": [
    "{{? trms:translateToCoding(type,'http://terminology.hl7.org/CodeSystem/v2-0487','http://terminology.hl7.org/ValueSet/v2-0487', 'http://snomed.info/sct?fhir_vs')}}"
  ]
}
```

#### Identity Service
Ignifyr allows you to use a FHIR endpoint as and identity service in case FHIR resource identifiers need to be fetched given
the business identifiers. In this case, you can use the `idxs:resolveIdentifier` function with the following parameters:
`idxs:resolveIdentifier(FHIR resource type, Identifier.value, Identifier.system)` which returns a FHIR reference such as `Patient/455435464698`.

The following example puts the FHIR resource id of the Patient into the reference field by using the identity service:
```json
{
  "subject": {
    "reference": "{{idxs:resolveIdentifier('Patient', pid, 'https://aiccelerate.eu/data-integration-suite/test-data')}}"
  }
}
```

#### Scheduled Jobs

Ignifyr supports running scheduled jobs with defined time ranges.
To do so, you need to specify a cron expression in the mapping job definitions.
Ignifyr uses [cron4j](https://www.sauronsoftware.it/projects/cron4j/) library to handle scheduled jobs.
Scheduled patterns for the expression can be found in the documentation section of cron4j. 
Synchronization times for scheduled jobs are maintained in a folder defined `db-path` setting in the configuration file.

You can schedule a mapping job as follows:

`mapping-job.json`
```json 
{
  ...
  "schedulingSettings": {
    "jsonClass": "SchedulingSettings",
    "cronExpression": "59 11 * * *"
  },
  ...
}
```
`59 11 * * *` pattern causes a task to be launched at 11:59AM every day.

Moreover, if your data source is SQL-based and contains time or date information, and you want to pull data at time intervals according to schedule,
you can specify the initial time in your mapping job definition as follows:

`mapping-job.json`
```json
{
  ...
  "schedulingSettings": {
    "jsonClass": "SQLSchedulingSettings",
    "cronExpression": "59 11 * * *",
    "initialTime": "2000-01-01T00:00:00"
  },
  ...
}
```

`mapping.json`
```json
{
  ...,
  "name": "procedure-occurrence-mapping",
  "mappingRef": "https://aiccelerate.eu/fhir/mappings/omop/procedure-occurrence-mapping",
  "sourceBinding": {
    "source": {
      "jsonClass": "SqlSource",
      "query": "select ... from procedure_occurrence po left join concept c on po.procedure_concept_id = c.concept_id where po.procedure_date > $fromTs and po.procedure_date < $toTs"
    }
  },
  ...
}
```
`procedure_occurrence` table has a date column `procedure_date` in this example.
When your scheduled task runs, `$fromTs` and `$toTs` placeholders are replaced with corresponding timestamps.
According to the mapping job and mapping shown above,
after you run the mapping job, lets say at 2022-08-08T10:05:30, the following variables will take place as the scheduled job runs.

| fromTs              | toTs             | Explanation                                                                                                                                                 |
|---------------------|------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------|
| 2000-01-01T00:00:00 | 2022-08-08T11:59 | Configured initial time used for fromTs, current run time is used for toTs. <br/>If no initialTime provided, initial time will be midnight, January 1, 1970 |
| 2022-08-08T11:59    | 2022-08-09T11:59 | New fromTs is the previous toTs                                                                                                                             |
| 2022-08-09T11:59    | 2022-08-10T11:59 | And goes like this                                                                                                                                          |
| ...                 |                  |                                                                                                                                                             |


#### Archiving

Ignifyr supports archiving of erroneous records and the source data files.
If you want to archive only the erroneous records, which are the records that could not be processed/mapped by the mapping job, 
you can specify the config in the mapping job definitions. 
The erroneous records are saved in the `erroneous-records-folder` defined in the sub-config of the `archiving` config in the configuration file.

`mapping-job.json`
```json
{
  ...
  "dataProcessingSettings": {
    "saveErroneousRecords": true
  },
  ...
}
```

If you want to archive the source data files after processing, regardless of whether that file was processed/mapped successfully or not,
you can specify the config in the mapping job definitions. 
The source data files are saved in the `archive-folder` defined in the sub-config of the `archiving` config in the configuration file.

`mapping-job.json`
```json
{
  ...
  "dataProcessingSettings": {
    "archiveMode": "archive"
  },
  ...
}
```

Or if you want to simply delete the source data files after processing/mapping:

`mapping-job.json`
```json
{
  ...
  "dataProcessingSettings": {
    "archiveMode": "delete"
  },
  ...
}
```

Or both. This will delete the source files after processing/mapping and save the erroneous records:

`mapping-job.json`
```json
{
  ...
  "dataProcessingSettings": {
    "saveErroneousRecords": true,
    "archiveMode": "delete"
  },
  ...
}
```

While `archiveMode` works on a file-based basis, `saveErroneousRecords` works for each record/row in the source data.

Please also note that, the `archiveMode` config is only applicable for the file system source type.

#### Batching Strategy

When dealing with large data sources (e.g., 10 million+ rows), loading all data into memory at once is not practical.
Ignifyr supports a **batching strategy** that allows you to process data in smaller batches by defining parameter sets that filter the source data.

Each batch is processed sequentially: data is loaded, mapped, written to the sink, and then memory is freed before moving to the next batch.

##### Basic Usage

Define a `batchingStrategy` in your `FhirMappingTask` along with a `preprocessSql` that uses parameter placeholders (prefixed with `$`):

```json
{
  "name": "measurement-mapping",
  "mappingRef": "https://example.com/measurement-mapping",
  "sourceBinding": {
    "source": {
      "jsonClass": "SqlSource",
      "query": "SELECT * FROM MEASUREMENT",
      "preprocessSql": "SELECT * FROM MEASUREMENT WHERE EXTRACT(YEAR FROM MEASUREMENT_DATE) = $year"
    }
  },
  "batchingStrategy": {
    "batchParameterSets": [
      {"year": "2018"},
      {"year": "2019"},
      {"year": "2020"}
    ]
  }
}
```

In this example, Ignifyr will execute 3 batches, one for each year. The `$year` placeholder in `preprocessSql` is replaced with values from `batchParameterSets`.

##### Multiple Parameters

You can use multiple parameters for finer-grained batching:

```json
{
  "name": "encounter-mapping",
  "mappingRef": "https://example.com/encounter-mapping",
  "sourceBinding": {
    "encounterMain": {
      "jsonClass": "FileSystemSource",
      "path": "encounters.csv",
      "contentType": "csv",
      "preprocessSql": "SELECT * FROM encounterMain WHERE EXTRACT(YEAR FROM ENCOUNTER_DATE) = $year AND EXTRACT(MONTH FROM ENCOUNTER_DATE) = $month"
    }
  },
  "batchingStrategy": {
    "batchParameterSets": [
      {"year": "2020", "month": "01"},
      {"year": "2020", "month": "02"},
      {"year": "2020", "month": "03"}
    ]
  }
}
```

ID Ranges with Custom Logic:

```json
{
  "preprocessSql": "SELECT * FROM PATIENTS WHERE patient_id BETWEEN $id_start AND $id_end",
  "batchingStrategy": {
    "batchParameterSets": [
      {"id_start": "1", "id_end": "100000"},
      {"id_start": "100001", "id_end": "200000"},
      {"id_start": "200001", "id_end": "300000"},
      {"id_start": "300001", "id_end": "400000"}
    ]
  }
}
```

**Key points:**
- Parameter placeholders in `preprocessSql` use the `$parameterName` syntax
- All parameters in the placeholder must exist in each object of `batchParameterSets`
- Batches are processed sequentially, reducing memory footprint
- Only sources with `preprocessSql` containing parameter placeholders are affected by the batching strategy

##### Generating Parameter Sets

For large ranges (e.g., 40 years × 12 months), you can generate `batchParameterSets` programmatically using the helper scripts:

```python
import json

def generate_year_month_batches(start_year, end_year):
    """Generate batch parameter sets for year-month combinations"""
    batches = []
    for year in range(start_year, end_year + 1):
        for month in range(1, 13):
            batches.append({
                "year": str(year),
                "month": str(month)
            })
    return batches

# Generate batches for 1980-2020 (40 years)
batches = generate_year_month_batches(1980, 2020)

print(f"Total batches: {len(batches)}")  # Output: 492 batches

# Create the batching strategy
batching_strategy = {
    "batchParameterSets": batches
}

# Save to file
with open('batching_strategy.json', 'w') as f:
    json.dump(batching_strategy, f, indent=2)

print("Batching strategy saved to batching_strategy.json")
```
