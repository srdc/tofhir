# toFHIR
toFHIR is a data mapping tool to transform existing datasets from various types of sources to HL7 FHIR. 
It can be used as a library or standalone tool for data integration and data transformation into HL7 FHIR.
The standalone mode accepts command line arguments to either run a batch execution right away or to start a command
line interface (CLI) to accept certain commands.

Requirements
------------

toFHIR requires the following to run:

* Java 11.0.2 or higher
* Scala 2.13 or higher
* An HL7 FHIR repository if you would like to persist the created resources (e.g., [onfhir](https://github.com/srdc/onfhir))

Usage
-----

If no arguments are provided, toFHIR starts the command line interface (CLI). Possible arguments to the executable are as follows:
- `cli`: Starts the CLI. This is the default command if no arguments are provided.
- `run`: Runs the configured mapping-job as a batch job and shuts down after finishing. `run` command accepts the following parameters:
  - `--job`: The path to the mapping-job to be executed. If provided, overrides the path provided to the JVM as the configuration parameter.
  - `--mappings`: The path to the mappings folder. If provided, overrides the path provided to the JVM as the configuration parameter.
  - `--schemas`: The path to the schemas folder. If provided, overrides the path provided to the JVM as the configuration parameter.

CLI
---

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


Mapping Definitions
-------------------
The app expects mappings and schemas under the folder named `mappings` and `schemas` respectively by default. 
But a user can also specify the path to the mappings and schemas folder by using `application.conf` file or VM options.

All files of mapping jobs, mappings and schemas are expected to be in the JSON format and all of them have their own structure.

##### Mapping Job

Example of a Mapping Job definition file:
```json
{
  "id": "pilot1-mapping-job",
  "sourceSettings": {
    "jsonClass": "FileSystemSourceSettings",
    "name": "pilot1-source",
    "sourceUri": "https://aiccelerate.eu/data-integration-suite/pilot1-data",
    "dataFolderPath": "test-data/pilot1"
  },
  "sinkSettings": {
    "jsonClass": "FhirRepositorySinkSettings",
    "fhirRepoUrl": "http://localhost:8081/fhir",
    "writeErrorHandling": "continue"
  },
  "mappingErrorHandling": "continue",
  "mappings": [
    {
      "jsonClass": "FileSourceMappingDefinition",
      "mappingRef": "https://aiccelerate.eu/fhir/mappings/pilot1/patient-mapping",
      "filePath": "patients.csv"
    },
    {
      "jsonClass": "FileSourceMappingDefinition",
      "mappingRef": "https://aiccelerate.eu/fhir/mappings/pilot1/practitioner-mapping",
      "filePath": "practitioners.csv"
    }
  ]
}
```
The json snippet above illustrates the structure of an example mapping job.
`sourceSettings` defines the source settings of the mapping job. The source settings config is used to connect to the source data.
In this case, the source type of data is file system source and `dataFolderPath` defines the path of the source data.

Assuming onFhir is running on the system, `sinkSettings` defines onFhir configurations to connect to the data destination.

`mappings` is a list mapping refs that mapping job includes. For a purpose of illustration, the mapping job above includes two mappings: 
- https://aiccelerate.eu/fhir/mappings/pilot1/patient-mapping
- https://aiccelerate.eu/fhir/mappings/pilot1/practitioner-mapping

Since we have FileSystemSourceSettings defined in the source settings, `jsonClass`es of mappings are expected to be FileSourceMappingDefinition.
In the mapping list, `mappingRef` is the reference url of the mapping repository. For the file source mappings, 
the `filePath` field should be specified and it represents the source file of each mapping.

##### Mappings
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
It can be seen that exemplified mapping job in the previous section includes the mapping shown here.
The real magic in mappings happens in the `expression` field.
toFHIR uses the expression to generate the FHIR resources by using [onfhir-template-engine](https://github.com/aiccelerate/onfhir-template-engine). 
By doing so, it can generate the FHIR resources based on the source data.

The json keys in the `expression.value` represent the FHIR resource properties. 
On the value sides, onfhir-template-engine is used to interpret the source data. 
You can get more information how template engine works on the GitHub page.

