package io.tofhir.engine.model

import io.tofhir.engine.model.ArchiveModes.ArchiveModes

/**
 * Archive mode types to specify what to do with data source after mapping execution
 * off - do nothing
 * delete - delete the data source after mapping execution
 * archive - archive the data source after mapping execution
 */
object ArchiveModes extends Enumeration {
  type ArchiveModes = String
  final val OFF = "off"
  final val DELETE = "delete"
  final val ARCHIVE = "archive"
}

/**
 * Data source processing settings
 *
 * @param saveErroneousRecords If true, erroneous records will be saved to archive folder with the same path as the input file
 * @param archiveMode Archive mode for erroneous records (off, delete, archive)
 */
case class DataProcessingSettings(saveErroneousRecords: Boolean = false,
                                  archiveMode: ArchiveModes = ArchiveModes.OFF) {}
