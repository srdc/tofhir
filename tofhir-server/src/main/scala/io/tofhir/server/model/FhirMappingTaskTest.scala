package io.tofhir.server.model

import io.tofhir.engine.model.FhirMappingTask

/**
 * FhirMappingTask and selection of rows to test a mapping
 * @param fhirMappingTask task to be used for testing
 * @param selection row selection for testing
 */
case class FhirMappingTaskTest(fhirMappingTask: FhirMappingTask,
                               selection: RowSelection)

/**
 * Row selection for testing a mapping
 * @param numberOfRows number of rows to select
 * @param order order of rows to select
 */
case class RowSelection(numberOfRows: Int, order: String)

/**
 * Available row selection orders
 */
object RowSelectionOrder {
  final val START = "start"
  final val END = "end"
  final val RANDOM = "random"

  def isValid(order: String): Boolean = allValues().contains(order)

  private def allValues(): Set[String] = Set(START, END, RANDOM)
}
