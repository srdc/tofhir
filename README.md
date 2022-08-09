# toFHIR
toFHIR is a data mapping tool to transform existing datasets from various types of sources to HL7 FHIR. 
It can be used as a library or standalone tool for data integration and data transformation into HL7 FHIR.
The standalone mode accepts command line arguments to either run a batch execution right away or to start a command
line interface (CLI) to accept certain commands.

## Requirements

toFHIR requires the following to run:

* Java 11.0.2 or higher
* Scala 2.13 or higher
* An HL7 FHIR repository if you would like to persist the created resources (e.g., [onfhir](https://github.com/srdc/onfhir))

## Supported Data Source Types

* File System (Excel, csv)
* RDMS (PostgresSQL)
* Kafka

## Usage

If no arguments are provided, toFHIR starts the command line interface (CLI). Possible arguments to the executable are as follows:
- `cli`: Starts the CLI. This is the default command if no arguments are provided.
- `run`: Runs the configured mapping-job as a batch job and shuts down after finishing. `run` command accepts the following parameters:
  - `--job`: The path to the mapping-job to be executed. If provided, overrides the path provided to the JVM as the configuration parameter.
  - `--mappings`: The path to the mappings folder. If provided, overrides the path provided to the JVM as the configuration parameter.
  - `--schemas`: The path to the schemas folder. If provided, overrides the path provided to the JVM as the configuration parameter.
  - `--syncTimes`: The path to the sync times log folder that is used for only scheduled jobs. If provided, overrides the path provided to the JVM as the configuration parameter.

## CLI

toFHIR serves via CLI with certain commands:
- `help`: Displays the help text and see the available commands and their use.
- `info`: See info about the loaded Mapping Job.
- `load`: Loads a Mapping Job Load the Mapping Job definition file from the path.
- `run [<url>|<name>]`: Run the task(s). Without a parameter, all task of the loaded Mapping Job are run. A specific task can be indicated with its name or URL.
- `stop`: Stop the execution of the MappingJob (if any).
- `exit|quit`: Exit the program.

After the app is up and running, these commands are ready to be executed.
If there is no mapping job loaded initially, firstly, a mapping job needs to be loaded with the command `load <mapping-job-path>`.
This command loads the mapping job located in the path. After that, the mapping job can be run with the command `run`.

## Configurations

With a config file like the example below, we can specify the location of schemas and mappings and the mapping-job it will use at startup.

```conf
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
  }
}

# Absolute path to the JSON file for the MappingJob definition to load at the beginning
mapping-job.file-path = "mapping-jobs/pilot1-mappingjob.json"

# Absolute path to the folder for recording run times of the jobs. Necessary for only scheduled jobs.
toFhir.db = "db/mapping-job-sync-times"

```

## Mapping Job

### Terminology Service
toFhir also supports terminology server as well as file supported terminology service. 
You can specify your concept maps and code systems by giving the path of terminology service folder.
An example of terminology service configuration in a mapping job as follows:
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

After configuring terminology service settings, you can fetch display data by using, for example, `trms:lookupDisplay` fhirpath function in the mappings. 
Following example gets German display (`de` column) of the data with code 119323008 and code system SNOMED:
```json
{
  "system": "http://snomed.info/sct",
  "code": "111",
  "display": "{{ trms:lookupDisplay('119323008','http://snomed.info/sct','de') }}"
}
```
Similarly, when you want to translate the given code+system according to the given source value set and optional target value set, 
you can do something like this. This creates a FHIR-Coding object automatically and replaces the expression.
```json
{
  "coding": [
    "{{? trms:translateToCoding(type,'http://terminology.hl7.org/CodeSystem/v2-0487','http://terminology.hl7.org/ValueSet/v2-0487', 'http://snomed.info/sct?fhir_vs')}}",
  ]
}
```

### Fhir Identity Service
Sometimes, FHIR Resource identifier needs to be fetched from FHIR when you only have business identifier. In this case, 
you can use a fhirpath function called `idxs:resolveIdentifier`.
This example resolve the given business identifier and return the FHIR Resource identifier (together with the resource type) by using the supplied identity service:
```json
{
  "subject": {
    "reference": "{{idxs:resolveIdentifier('Patient', pid, 'https://aiccelerate.eu/data-integration-suite/test-data')}}"
  }
}
```

### Data Sources
#### File System
Example of a Mapping Job definition file with csv source type:

```json
{
  "id": "pilot1-mapping-job",
  "sourceSettings": {
    "source": {
      "jsonClass": "FileSystemSourceSettings",
      "name": "pilot1-source",
      "sourceUri": "https://aiccelerate.eu/data-integration-suite/pilot1-data",
      "dataFolderPath": "test-data/pilot1"
    }
  },
  "sinkSettings": {
    "jsonClass": "FhirRepositorySinkSettings",
    "fhirRepoUrl": "http://localhost:8081/fhir",
    "writeErrorHandling": "continue"
  },
  "mappingErrorHandling": "continue",
  "mappings": [
    {
      "mappingRef": "https://aiccelerate.eu/fhir/mappings/pilot1/patient-mapping",
      "sourceContext": {
        "source": {
          "jsonClass": "FileSystemSource",
          "filePath": "patients.csv"    
        }
      }
    },
    {
      "mappingRef": "https://aiccelerate.eu/fhir/mappings/pilot1/practitioner-mapping",
      "sourceContext": {
        "source": {
          "jsonClass": "FileSystemSource",
          "filePath": "practitioners.csv"
        }
      }
    }
  ]
}
```
The json snippet above illustrates the structure of an example mapping job.
`sourceSettings` defines the source settings of the mapping job. The source settings config is used to connect to the source data.
In this case, the source type of data is file system source and `dataFolderPath` defines the path of the source data folder.

Assuming onFHIR is running on the system, `sinkSettings` defines onFHIR configurations to connect to the data destination.

`mappings` is a list of mapping tasks that mapping job includes. For a purpose of illustration, the mapping job above includes two mappings: 
- https://aiccelerate.eu/fhir/mappings/pilot1/patient-mapping
- https://aiccelerate.eu/fhir/mappings/pilot1/practitioner-mapping

Since we have FileSystemSourceSettings defined in the source settings, `jsonClass`es of mappings are expected to be FileSystemSource.
In the mapping list, `mappingRef` is the reference url of the mapping repository. For the file source mappings, 
the `filePath` field should be specified, and it represents the source file of each mapping.

#### SQL

Similarly, if we had a source with SQL type, `sourceSettings` and `mappings` part would look like this:
```json
{
  "sourceSettings": {
    "source": {
      "jsonClass": "SqlSourceSettings",
      "name": "pilot1-source",
      "sourceUri": "https://aiccelerate.eu/data-integration-suite/pilot1-data",
      "dataFolderPath": "jdbc:postgresql://localhost:5432/db_name",
      "username": "postgres",
      "password": "postgres"
    }
  }
}
```
```json
{
  "mappingRef": "https://aiccelerate.eu/fhir/mappings/location-sql-mapping",
  "sourceContext": {
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
  "mappingRef": "https://aiccelerate.eu/fhir/mappings/location-sql-mapping",
  "sourceContext": {
    "source": {
      "jsonClass": "SqlSource",
      "query": "select * from location"
    }
  }
}
```
Also, toFhir supports running scheduled jobs with defined time ranges. 
To do so, you need to specify a cron expression and an optional initial time in the mapping job definitions.
toFhir uses [cron4j](https://www.sauronsoftware.it/projects/cron4j/) library to handle scheduled jobs. 
Scheduled patterns for the expression can be found in the documentation section of cron4j.

If your data has time or date information, and you want to pull data at time intervals according to schedule, 
you can do something like this:

mapping-job.json
```json
{
  ...
  "schedulingSettings": {
    "cronExpression": "59 11 * * *",
    "initialTime": "2000-01-01T00:00:00"
  },
  ...
}
```
`59 11 * * *` pattern causes a task to be launched at 11:59AM every day.

mapping.json
```json
{
  ...
  "mappingRef": "https://aiccelerate.eu/fhir/mappings/omop/procedure-occurrence-mapping",
  "sourceContext": {
    "source": {
      "jsonClass": "SqlSource",
      "query": "select ... from procedure_occurrence po left join concept c on po.procedure_concept_id = c.concept_id where po.procedure_date > $fromTs and po.procedure_date < $toTs"
    }
  },
  ...
}
```
`procedure_occurrence` table has a date column `procedure_date` in this example. 
When your scheduled task runs, `$fromTs` and `$toTs` keywords are replaced with corresponding timestamps.
According to the mapping job and mapping shown above, 
after you run the mapping job, lets say at 2022-08-08T10:05:30, the following variables will take place as the scheduled job runs.

| fromTs              | toTs             | Explanation                                                                                                                                                 |
|---------------------|------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------|
| 2000-01-01T00:00:00 | 2022-08-08T11:59 | Configured initial time used for fromTs, current run time is used for toTs. <br/>If no initialTime provided, initial time will be midnight, January 1, 1970 |
| 2022-08-08T11:59    | 2022-08-09T11:59 | New fromTs is the previous toTs                                                                                                                             |
| 2022-08-09T11:59    | 2022-08-10T11:59 | And goes like this                                                                                                                                          |
| ...                 |                  |                                                                                                                                                             |

#### Kafka

Mapping job and mapping examples shown below for the streaming type of sources like Kafka:
```json
{
  "sourceSettings": {
    "source": {
      "jsonClass": "StreamingSourceSettings",
      "name": "pilot1-source",
      "sourceUri": "https://aiccelerate.eu/data-integration-suite/pilot1-data",
      "bootstrapServers": "localhost:9092,localhost:9093"
    }
  }
}
```
```json
{
  "mappingRef": "https://aiccelerate.eu/fhir/mappings/location-sql-mapping",
  "sourceContext": {
    "source": {
      "jsonClass": "KafkaSource",
      "topicName": "patients",
      "groupId": "tofhir",
      "startingOffsets": "earliest"
    }
  }
}
```
toFhir only considers the value field of kafka topics. Therefore, when you subscribe a topic, 
toFhir waits for string-type data but in correct JSON format.

## Mappings
Example of a Mapping definition file:
```json
{
  "url": "https://aiccelerate.eu/fhir/mappings/pilot1/patient-mapping",
  "name": "patient-mapping",
  "title": "Mapping of patient schema for pilots to AIC-Patient FHIR profile",
  "source": [{
    "alias": "source",
    "url": "https://aiccelerate.eu/fhir/StructureDefinition/Ext-plt1-patient"
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
            "profile": ["https://aiccelerate.eu/fhir/StructureDefinition/AIC-Patient"],
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

The json snippet above illustrates the structure of an example mapping.
It can be seen that exemplified mapping job with the file system source type in the previous section includes the mapping shown here.
The real magic in mappings happens in the `expression` field.
toFHIR uses the expression to generate the FHIR resources by using [onfhir-template-engine](https://github.com/aiccelerate/onfhir-template-engine). 
By doing so, it can generate the FHIR resources based on the source data.

The json keys in the `expression.value` represent the FHIR resource attributes. That is, we write the FHIR resource structure 
by providing the values through a template language where we can access the fields of the source data as defined by its schema.
On the value sides, onfhir-template-engine is used to interpret the source data. You can get more information how template engine works on the GitHub page.

