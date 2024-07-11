package io.tofhir.server.service.fhir

import io.onfhir.path.AbstractFhirPathFunctionLibrary
import io.onfhir.path.annotation.FhirPathFunction
import org.reflections.Reflections

import scala.jdk.javaapi.CollectionConverters.asScala
import scala.reflect.runtime.currentMirror
import scala.reflect.runtime.universe._

/**
 * Service to manage FhirPath functions.
 * */
class FhirPathFunctionsService {

  // list of packages where FhirPathFunction libraries will be searched
  private val packages: Seq[String] = Seq("io.onfhir.path", "io.tofhir.engine.mapping")

  // keeps the FhirPath functions documentation of FhirPath libraries
  // it searches for the libraries in 'packages' and calls 'getFunctionDocumentation' method of classes
  // to retrieve function documentation.
  private lazy val fhirPathFunctionsDocumentation: Seq[FhirPathFunction] = packages.flatMap(p => {
    val reflections = new Reflections(p)
    asScala(reflections.getSubTypesOf(classOf[AbstractFhirPathFunctionLibrary])).flatMap(c => {
      val classSymbol = currentMirror.classSymbol(c)

      // create an instance of class
      val constructorSymbol: MethodSymbol = classSymbol.primaryConstructor.asMethod
      val classInstance = currentMirror.reflectClass(classSymbol).reflectConstructor(constructorSymbol).apply(constructorSymbol.paramLists.head.map(_ => null): _*)
      val instanceMirror: InstanceMirror = currentMirror.reflect(classInstance)

      // call getFunctionDocumentation method
      instanceMirror.reflectMethod(classSymbol.toType.members.find(_.name.toString == "getFunctionDocumentation").map(_.asMethod).get).apply().asInstanceOf[Seq[FhirPathFunction]]
    }).toSeq
  })

  /**
   * Returns the documentations of FhirPath functions.
   *
   * @return the documentations of FhirPath functions i.e. a list of FhirPathFunction
   * */
  def getFhirPathFunctionsDocumentation: Seq[FhirPathFunction] = {
    fhirPathFunctionsDocumentation
  }
}
