package io.tofhir.common.env

object EnvironmentVariableResolver {

  def replaceEnvironmentVariables(fileContent: String): String = {
    var returningContent = fileContent;
    //    val regex = """\$\{(.*?)\}""".r
    EnvironmentVariable.values.foreach { e =>
      val regex = "\\$\\{" + e.toString + "\\}"
      if (sys.env.contains(e.toString)) returningContent = returningContent.replaceAll(regex, sys.env(e.toString))
    }
    returningContent
  }

}
