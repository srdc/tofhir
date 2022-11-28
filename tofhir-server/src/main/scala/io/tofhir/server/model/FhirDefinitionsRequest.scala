package io.tofhir.server.model

import io.tofhir.server.common.model.IToFhirRequest

case class FhirDefinitionsRequest(override val requestId: String) extends IToFhirRequest {

}
