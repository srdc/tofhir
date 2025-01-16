# [toFHIR](https://onfhir.io/tofhir/index.html)
toFHIR is an easy-to-use data mapping and high-performant data transformation tool to transform existing datasets from
various types of sources to HL7 FHIR. It can be used as a library or standalone tool for data integration and data
transformation into HL7 FHIR. The standalone mode accepts command line arguments to either run a batch execution right
away or to start a command line interface (CLI) to accept certain commands.

toFHIR can read from various data sources such as a file system, relational database, streaming inputs like Apache Kafka or a FHIR Server.
toFHIR's mapping language utilizes [onfhir-template-engine](https://github.com/srdc/fhir-template-engine)'s template language and allows 1-to-1, 1-to-many, 
many-to-1 and many-to-many mappings. By executing the mapping definitions, toFHIR generates HL7 FHIR resources and they can
be either persisted to a file system or to a running HL7 FHIR endpoint.

## Modules

toFHIR consists of the following modules:
- `tofhir-engine`: The core module of toFHIR which includes the main functionality of the tool.
- `tofhir-server`: A standalone web server module that provides a REST API to run mapping jobs via `tohir-engine` and manage the mapping job definitions.
- `tofhir-server-common`: Holds common files like server configuration or errors for server implementations.
- `tofhir-common`: Contains model and utility classes shared across various modules.
- `tofhir-rxnorm`: Provides a client implementation to access the RxNorm API and a FHIR Path Function library to utilize its API functionalities in mapping definitions.

For a visual representation of the dependencies between these modules, please refer to the diagram below:

![module-component-diagram.png](readme-assets%2Fmodule-component-diagram.png)

## Requirements
toFHIR requires the following to run:

* Java 11.0.2
* Scala 2.13
* An HL7 FHIR repository if you would like to persist the created resources (e.g., [onFHIR](https://github.com/srdc/onfhir))

## Supported Data Source Types
toFHIR can read data from the following data source types:

* File System (Excel, CSV, TSV, JSON, Parquet)
* RDMS (PostgreSQL)
* Apache Kafka
* REDCap
* FHIR Server (OnFHIR, Firely Server etc.)

## Usage

toFHIR can be used through its standalone `tofhir-engine` or via the web server `tofhir-server`. 

## toFHIR Engine

If the engine will be used as a standalone tool, when it is started, the engine waits for the commands from the command line interface (CLI).
Also, arguments can be provided to the executable to start the engine with a specific command or configuration file.
Possible arguments to the executable are as follows:

- `cli`: Starts the CLI. This is the default command if no arguments are provided.
- `run`: Runs the configured mapping-job as a batch job and shuts down after finishing. `run` command accepts the following parameters:
  - `--job`: The path to the mapping-job to be executed. If provided, overrides the path provided to the JVM as the configuration parameter.
  - `--mappings`: The path to the mappings folder. If provided, overrides the path provided to the JVM as the configuration parameter.
  - `--schemas`: The path to the schemas folder. If provided, overrides the path provided to the JVM as the configuration parameter.
  - `--db`: The path to the database folder that is used for scheduled jobs. If provided, overrides the path provided to the JVM as the configuration parameter.
- `extract-redcap-schemas`: Extracts schemas from a REDCap data dictionary. `extract-redcap-schemas` command accepts the following parameters:
  - `--data-dictionary`: The path to the REDCap data dictionary
  - `--definition-root-url`: The root url of FHIR resources
  - `--encoding`: The encoding of CSV file whose default value is UTF-8 (OPTIONAL)
  
### CLI

toFHIR serves via CLI with certain commands:
- `help`: Displays the help text and see the available commands and their use.
- `info`: See info about the loaded Mapping Job.
- `load`: Loads a Mapping Job Load the Mapping Job definition file from the path.
- `run [<url>|<name>]`: Run the task(s). Without a parameter, all task of the loaded Mapping Job are run. A specific task can be indicated with its name or URL.
- `extract-redcap-schemas [path] [definition-root-url] [encoding]`: Extracts schemas from the given REDCap data dictionary file. Schemas will be annotated with the given definition root url. If the encoding of CSV file is different from UTF-8, you should provide it.
- `stop`: Stop the execution of the MappingJob (if any).
- `exit|quit`: Exit the program.

After the app is up and running, these commands are ready to be executed.
If there is no mapping job loaded initially, firstly, a mapping job needs to be loaded with the command `load <mapping-job-path>`.
This command loads the mapping job located in the path. After that, the mapping job can be run with the command `run`.

### Configurations
 
Here is an example of a configuration file:

```conf
tofhir {

  # A path to a directory from where any File system readings should use within the mappingjob definition.
  # This should be pointed to the root folder of the definitions.
  context-path = "tofhir-definitions"

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

  # Database folder of toFHIR (e.g., to maintain synchronization times for scheduled jobs)
  db-path = "tofhir-db"
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

Considering the configuration file defined above, toFHIR can be utilized with a folder structure like the following:

```html
tofhir-definitions (root folder of definitions)
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
└── tofhir.conf
```

Of course, you are free to organize the definitions in any way you like and arrange the configuration file accordingly.
However, we are suggesting to keep the definitions in a folder structure as shown above to keep the definitions organized and easy to manage.

## toFHIR Server

If the web server is used, it will start the web server and the engine will be available via the REST API. 
Considering the same folder structure example given above, the server will also use the same configuration settings. 
Additionally, there are extra configurations to be made in the configuration file:

```conf
fhir = {
  # major FHIR version, currently R4 and R5 is supported
  fhir-version = "R4"
  # List of root URLs while retrieving the definitions (profiles, valuesets, codesystems).
  # The definitions below the given root URLs will be retrieved from the configured paths or FHIR endpoints.
  # All definitions will be retrieved if no root URLs are provided.
  # e.g. ["https://aiccelerate.eu", "https://fair4health.eu"]
  definitions-root-urls = ["http://hl7.org/fhir/"]

  # FHIR URL to retrieve resource definitions (profiles, valuesets and codesystems).
  # If this URL is defined, file paths (profiles-path, valuesets-path, codesystems-path) will be ignored even if they are also provided.
  # For now, toFHIR can read definitions from a single FHIR endpoint.
  definitions-fhir-endpoint = "http://localhost:8080/fhir"
  fhir-endpoint-auth = {
    # basic | token | fixed-token
    # If one of the auth methods is selected, its configurations must be provided as shown below.
    method = null

#     # basic configurations are used if the auth method is basic
#     basic = {
#       username = "user"
#       password = "pass"
#     }
#
#     # token configurations are used if the auth method is token
#     token = {
#       client-id = "id"
#       client-secret = "secret"
#       scopes = []
#       token-endpoint = "https://onauth.srdc.com.tr"
#     }

#    # fixed token configurations are used if the auth method is fixed-token
#    fixed-token = "XXX"
  }

  # Path to the zip file or folder that includes the FHIR resource and data type profile definitions (FHIR StructureDefinition) to be served by toFHIR webserver so that mappings can be performed accordingly.
  profiles-path = null

  # Path to the zip file or folder that includes the FHIR Value Set definitions (FHIR ValueSet) that are referenced by your FHIR profiles.
  valuesets-path = null

  # Path to the zip file or folder that includes the FHIR Code system definitions (FHIR CodeSystem) that are referenced by your FHIR value sets.
  codesystems-path = null
}

webserver = {
  # Hostname that toFHIR server will work. Using 0.0.0.0 will bind the server to both localhost and the IP of the server that you deploy it.
  host = 0.0.0.0

  # Port to listen
  port = 8085

  # Base Uri for server e.g. With this default configuration, the root path of toFHIR server will be http://localhost:8085/tofhir
  base-uri = tofhir

  ssl {
    # Path to the java keystore for enabling ssl for toFHIR server, use null to disable ssl
    keystore = null
    # Password of the keystore for enabling ssl for toFHIR server
    password = null
  }
}
```

After the server is up and running, the engine will be available via the REST API.

With the REST APIs, you are able to do the following operations:
* Create/edit projects and subsequently create/edit schemas, mappings, mapping contexts, mapping jobs
* Create/edit terminology systems
* Test the mappings
* Run mapping jobs
* Query and see the logs of the mapping job execution results
 
API documentations for `tofhir-server` can be found at the following URLs:  

https://app.swaggerhub.com/apis-docs/toFHIR/toFHIR-Server

## Definitions Used in toFHIR

### Project

A project is a container for schemas, mappings, mapping contexts, mapping jobs. 
It is a concept used in toFHIR to organize the definitions and to group them together.

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
toFHIR uses the expression to generate the FHIR resources by using [onfhir-template-engine](https://github.com/srdc/fhir-template-engine).
By doing so, it can generate the FHIR resources based on the source data.

For example, considering `{{gender}}` expression, it refers to "gender" column in the source data. 
When this mapping is executed, each record at "gender" column in the source replaces the expression and generate the FHIR resources.

The json keys in the `expression.value` represent the FHIR resource attributes. That is, we write the FHIR resource structure
by providing the values through a template language where we can access the fields of the source data as defined by its schema.
On the value sides, `onfhir-template-engine` is used to interpret the source data. You can get more information how template engine works on the GitHub page.

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

If not set explicitly, toFHIR uses the batch mode by default. In the batch mode, toFHIR goes through these steps:
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

toFHIR supports streaming of file system in case you want to continuously monitor the changes on the source data and stream the
newcoming/updated data to toFHIR mapping executions. This can be done with the `asStream` config parameter of the source.
If it is set to `true`, toFHIR will monitor the FileSystemSource files defined at `path` paths and trigger the mapping
executions in case the files are updated. toFHIR automatically marks the processed data source files and only processes the newcoming/updated records.

toFHIR goes through these steps in the streaming mode:
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
      "dataFolderPath": "D:/codes/onfhir-io/tofhir/data",
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
- `path`  in the source binding of the mapping. `path` should be the name of the **folder** this time, and it is where toFHIR will monitor the changes.

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
toFHIR only considers the value field of kafka topics. Therefore, when you subscribe a topic,
toFHIR waits for string-type data but in correct JSON format.
For example, when you want to use the data in the topic, you should publish the data in the following format:
```json
{
  "pid": "p1",
  "gender": "male",
  "birthDate": "1995-11-10"
}
```
##### RedCAP
toFHIR integrates seamlessly with RedCAP through the [tofhir-redcap integration module](https://github.com/srdc/tofhir-redcap). 
Utilize the same configuration approach as described for Kafka, with a few key considerations:
- **Source Configuration**: In the mapping job's **sourceSettings**, specify that the data originates from RedCAP by setting the `asRedCap` field to `true`. Here's an example JSON configuration:

```json
{
  "sourceSettings": {
    "source": {
      "jsonClass": "KafkaSourceSettings",
      "name": "project1-source",
      "sourceUri": "https://aiccelerate.eu/data-integration-suite/project1-data",
      "bootstrapServers": "localhost:9092,localhost:9093",
      "asRedCap": true
    }
  }
}
```
- **Topic Name**: While defining the topic name for a mapping source binding within a mapping job, use the name generated by 
the [tofhir-redcap integration module](https://github.com/srdc/tofhir-redcap) for the corresponding RedCAP project. 
For detailed instructions, refer to the [README](https://github.com/srdc/tofhir-redcap/blob/main/README.md) file of the integration module.

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

Since toFHIR uses Apache Spark in its core, you can give any option that is supported by Apache Spark.
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

- [patient-simple.csv](tofhir-engine/src/test/resources/test-data/patient-simple.csv)
- [patient-gender-simple.csv](tofhir-engine/src/test/resources/test-data-gender/patient-gender-simple.csv)
- [patient-mapping-job-with-two-sources.json](tofhir-engine/src/test/resources/patient-mapping-job-with-two-sources.json)
- [patient-mapping-with-two-sources.json](tofhir-engine/src/test/resources/test-mappings/patient-mapping-with-two-sources.json)

#### Sink Settings

toFHIR supports persisting the generated FHIR resources to a FHIR repository. The sink settings are defined in the mapping job definition file.
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

#### Terminology Service
[A FHIR terminology service](https://hl7.org/fhir/terminology-service.html) can be automatically used by toFHIR to handle
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

toFHIR provides a `LocalFhirTerminologyService` which allows to use text files for concept details and translations. You
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
toFHIR's FHIRPath engine provides two functions becoming available when a terminology service is configured:
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
toFHIR allows you to use a FHIR endpoint as and identity service in case FHIR resource identifiers need to be fetched given
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

toFHIR supports running scheduled jobs with defined time ranges.
To do so, you need to specify a cron expression in the mapping job definitions.
toFHIR uses [cron4j](https://www.sauronsoftware.it/projects/cron4j/) library to handle scheduled jobs.
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

toFHIR supports archiving of erroneous records and the source data files.
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