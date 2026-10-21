package io.ignifyr.engine.spi

import io.ignifyr.engine.model.FhirMappingJobExecution

/**
 * Validation of the mapped resources against their profile(s) before they are written to the sink, for the jobs that
 * ask for it (`sinkSettings.validateResources`). The engine decides what happens to a resource with
 * validation errors — it is not written and is reported as an invalid resource, like a resource the sink rejects —
 * while how a resource is validated (which profiles, which validator) is up to the installed provider.
 *
 * Lifecycle, per mapping task execution:
 *  1. [[validatorFor]] is called on the driver for every batch chunk / streaming micro-batch of a task whose job asks
 *     for validation. The returned [[MappedResourceValidator]] is serialized to the Spark executors.
 *  1. [[MappedResourceValidator.validate]] is called there for every mapped resource.
 */
trait ResourceValidatorProvider {

  /** Driver side: a validator for the mapped resources of the given task execution. */
  def validatorFor(mappingJobExecution: FhirMappingJobExecution, mappingTaskName: String): MappedResourceValidator
}

/**
 * Validates mapped resources on the Spark executors. Implementations must be serializable and thread-safe.
 */
trait MappedResourceValidator extends Serializable {

  /**
   * Validates a mapped resource.
   *
   * @param mappedResource The mapped resource, as JSON
   * @return The issues found; empty if the resource conforms. Only issues with severity `error` or `fatal` make the
   *         resource invalid.
   */
  def validate(mappedResource: String): Seq[ValidationIssue]
}

/**
 * A validation issue, in the terms of a FHIR `OperationOutcome.issue`.
 *
 * @param severity    `fatal`, `error`, `warning` or `information`
 * @param code        The issue type, e.g. `invalid`, `not-supported`
 * @param diagnostics Human-readable description of the issue
 * @param expression  Paths of the elements the issue is about, e.g. `Observation.code`
 */
case class ValidationIssue(
    severity: String,
    code: String,
    diagnostics: Option[String] = None,
    expression: Seq[String] = Nil
) {

  /** Whether the issue makes the resource invalid. */
  def isError: Boolean = severity == "error" || severity == "fatal"
}
