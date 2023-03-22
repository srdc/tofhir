package io.tofhir.engine.cli

import java.io.{File, FileNotFoundException, FileWriter}

import io.onfhir.api.Resource
import io.onfhir.util.JsonFormatter.formats
import io.tofhir.engine.config.ToFhirConfig
import io.tofhir.engine.util.{CsvUtil, RedCapUtil}
import org.json4s.jackson.Serialization.writePretty

/**
 * Command to extract schemas from a REDCap data dictionary.
 * */
class ExtractRedCapSchemas extends Command {
  override def execute(args: Seq[String], context: CommandExecutionContext): CommandExecutionContext = {
    if (args.isEmpty) {
      println("extract-redcap-schemas command requires the path of RedCap data dictionary to extract from and definition root url.")
      context
    } else if (args.length == 1) {
      println("extract-redcap-schemas command requires definition root url.")
      context
    } else {
      val filePath = args.head
      try {
        // read REDCap data dictionary
        val content: Seq[Map[String, String]] = CsvUtil.readFromCSV(filePath)
        // extract schemas
        val fhirResources: Seq[Resource] = RedCapUtil.extractSchemas(content, args(1))
        // write each schema as a file
        fhirResources.foreach(fhirResource => {
          val resourceType = fhirResource.values("type")
          val file = new File(ToFhirConfig.engineConfig.schemaRepositoryFolderPath + File.separatorChar + resourceType + ".StructureDefinition.json")
          // when the schema already exists, warn user
          if (file.exists()) {
            println(s"Schema '$resourceType' already exists. Overriding it...")
          }
          // write schema to the file
          val fw = new FileWriter(file)
          try fw.write(writePretty(fhirResource)) finally fw.close()
          println(s"Schema '$resourceType' successfully created.")
        })

        context
      } catch {
        case _: FileNotFoundException =>
          println(s"The file cannot be found at the specified path:$filePath")
          context
      }
    }
  }
}
