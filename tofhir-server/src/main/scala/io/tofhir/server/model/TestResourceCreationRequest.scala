package io.tofhir.server.model

import io.tofhir.engine.model.FhirMappingTask

/**
 * FhirMappingTask and resource filter to test a mapping
 *
 * @param fhirMappingTask task to be used for testing
 * @param resourceFilter  resource filter for testing
 */
case class TestResourceCreationRequest(fhirMappingTask: FhirMappingTask,
                                       resourceFilter: ResourceFilter)

/**
 * Resource filter config for testing a mapping
 *
 * @param numberOfRows number of rows to select
 * @param order        order of rows to select
 */
case class ResourceFilter(numberOfRows: Int, order: String)

/**
 * Available row selection orders
 */
object RowSelectionOrder {
  final val START = "start"
  final val RANDOM = "random"

  def isValid(order: String): Boolean = allValues().contains(order)

  private def allValues(): Set[String] = Set(START, RANDOM)
}
