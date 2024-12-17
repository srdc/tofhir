package io.tofhir.engine.config

import com.typesafe.config.Config
import io.onfhir.path.IFhirPathFunctionLibraryFactory

/**
 * A configuration class responsible for loading external function libraries for FHIRPath expressions. It dynamically
 * initializes instances of function libraries and retrieves their package names.
 *
 * @param librariesConfig The configuration object containing the definitions of function libraries.
 */
class FunctionLibrariesConfig(librariesConfig: Config) {

  /**
   * A tuple containing a lazy-loaded map of function library factories and a list of package names.
   *
   * - `functionLibrariesFactories`: A map where the keys are library names (as defined in the configuration)
   * and the values are instances of `IFhirPathFunctionLibraryFactory`.
   * - `libraryPackageNames`: A sequence of package names for the dynamically loaded function library classes.
   */
  lazy val (functionLibrariesFactories: Map[String, IFhirPathFunctionLibraryFactory], libraryPackageNames: Seq[String]) = loadFunctionLibraryFactories()

  /**
   * Loads function library factories and their corresponding package names based on the provided configuration.
   *
   * @return A tuple containing:
   *         - A map of function library names to their respective instances (`Map[String, IFhirPathFunctionLibraryFactory]`).
   *         - A sequence of package names for the loaded classes (`Seq[String]`).
   * @throws IllegalArgumentException If no matching constructor is found for the provided arguments.
   */
  private def loadFunctionLibraryFactories(): (Map[String, IFhirPathFunctionLibraryFactory], Seq[String]) = {
    val factoriesBuilder = Map.newBuilder[String, IFhirPathFunctionLibraryFactory]
    val packageNamesBuilder = Seq.newBuilder[String]

    // Iterate over configuration keys
    librariesConfig.root().keySet().forEach { key =>
      val libraryConfig = librariesConfig.getConfig(key)

      // Dynamically load the class
      val className = libraryConfig.getString("className")
      val clazz = Class.forName(className)

      // Add the package name to the list
      packageNamesBuilder += clazz.getPackageName

      // Check if arguments (args) are provided
      val instance = if (libraryConfig.hasPath("args")) {
        val args = libraryConfig.getAnyRefList("args").toArray
        val constructors = clazz.getConstructors

        // Find a matching constructor and pass the arguments
        val constructor = constructors.find(_.getParameterCount == args.length)
          .getOrElse(throw new IllegalArgumentException(s"No matching constructor found for $className with args: ${args.mkString(",")}"))

        constructor.newInstance(args: _*).asInstanceOf[IFhirPathFunctionLibraryFactory]
      } else {
        // Default no-arg constructor
        clazz.getDeclaredConstructor().newInstance().asInstanceOf[IFhirPathFunctionLibraryFactory]
      }

      factoriesBuilder += key -> instance
    }

    // Return both the map and the package names
    (factoriesBuilder.result(), packageNamesBuilder.result())
  }
}
