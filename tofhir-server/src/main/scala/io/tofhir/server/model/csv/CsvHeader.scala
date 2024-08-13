package io.tofhir.server.model.csv

/**
 * Represents a CSV header with a new name and a previously saved name.
 * @param currentName The current name of the header.
 * @param previousName The previously saved name of the header, used for column name change without data loss
 * If a new header is desired to be created, saved name may be any placeholder
 */
case class CsvHeader(currentName: String, previousName: String)