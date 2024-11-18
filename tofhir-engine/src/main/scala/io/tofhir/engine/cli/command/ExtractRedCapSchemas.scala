package io.tofhir.engine.cli.command

import io.onfhir.api.Resource
import io.onfhir.definitions.common.model.Json4sSupport.formats
import io.tofhir.engine.config.ToFhirConfig
import io.tofhir.engine.util.redcap.RedCapUtil
import io.tofhir.engine.util.{CsvUtil, FileUtils}
import org.json4s.jackson.Serialization.writePretty

import java.io.{FileNotFoundException, FileWriter}

/**
 * Command to extract schemas from a REDCap data dictionary.
 * */
class ExtractRedCapSchemas extends Command {
  /**
   * @param args The list of arguments.
   *             The first argument is the path of REDCap data dictionary file.
   *             The second argument is the definition root url.
   *             The third argument is the encoding of CSV file (OPTIONAL).
   */
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
        val content: Seq[Map[String, String]] = CsvUtil.readFromCSV(filePath,args.lift(2).getOrElse("UTF-8"))
        // extract schemas
        val fhirResources: Seq[Resource] = RedCapUtil.extractSchemas(content, args(1))
        // write each schema as a file
        fhirResources.foreach(fhirResource => {
          val resourceType = fhirResource.values("type")
          val file = FileUtils.getPath(ToFhirConfig.engineConfig.schemaRepositoryFolderPath,resourceType + ".StructureDefinition.json").toFile
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
