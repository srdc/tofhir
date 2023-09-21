package io.tofhir.engine.model

import io.tofhir.engine.config.ErrorHandlingType.ErrorHandlingType
import io.tofhir.engine.model.ArchiveModes.ArchiveModes

object ArchiveModes extends Enumeration {
  type ArchiveModes = String
  final val OFF = "off"
  final val DELETE = "delete"
  final val ARCHIVE = "archive"
}

/**
 * Data source processing settings
 * @param mappingErrorHandling Error handling type HALT or CONTINUE
 * @param saveErroneousRecords If true, erroneous records will be saved to archive folder with the same path as the input file
 * @param archiveMode Archive mode for erroneous records (off, delete, archive)
 */
case class DataProcessingSettings(mappingErrorHandling: ErrorHandlingType,
                                  saveErroneousRecords: Boolean = false,
                                  archiveMode: ArchiveModes = ArchiveModes.OFF) {}
