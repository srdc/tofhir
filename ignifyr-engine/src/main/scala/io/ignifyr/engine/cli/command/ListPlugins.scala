package io.ignifyr.engine.cli.command

import io.ignifyr.engine.spi.{ExtensionRegistry, IgnifyrExtension}

/**
 * Lists the installed Ignifyr extension modules discovered through ServiceLoader and everything each
 * one contributes to the engine — source connectors, sinks, terminology/identity services, CLI
 * commands, schema inferrers, streaming/scheduling capabilities and Spark-conf entries — plus any
 * extra capabilities a module surfaces for itself (e.g. the file connector's discovered file
 * formats, via [[IgnifyrExtension.extraCapabilities]]).
 *
 * Runnable one-shot (`java -jar ignifyr-engine-standalone.jar list-plugins`) or interactively. It is
 * the machine-checkable form of "which plugins does this distribution ship?", used as a CI gate on
 * the community vs enterprise edition boundary.
 */
class ListPlugins extends Command {

  override def execute(args: Seq[String], context: CommandExecutionContext): CommandExecutionContext = {
    render()
    context
  }

  /**
   * Print the installed extensions and their contributions. Reads only the [[ExtensionRegistry]]
   * (populated by ServiceLoader off the classpath), so it needs no [[io.ignifyr.engine.IgnifyrEngine]]
   * and no configured workspace — `Boot` invokes this directly for the one-shot `list-plugins`.
   */
  def render(): Unit = {
    val extensions = ExtensionRegistry.extensions
    println(s"Installed Ignifyr extension(s): ${extensions.size}")
    extensions.foreach { ext =>
      println(s"\n  ${ext.id}")
      describe(ext).foreach(line => println(s"    $line"))
    }
  }

  /** Non-empty, human-readable contribution lines for a single extension. */
  private def describe(ext: IgnifyrExtension): Seq[String] = {
    def line[A](label: String, items: Seq[A])(render: A => String): Option[String] =
      if (items.isEmpty) None else Some(s"$label: ${items.map(render).mkString(", ")}")

    Seq(
      line("source connectors", ext.sourceConnectors)(c => s"${c.id} (${c.bindingClass.getSimpleName})"),
      line("sink providers", ext.sinkProviders)(p => s"${p.id} (${p.settingsClass.getSimpleName})"),
      line("terminology services", ext.terminologyServiceProviders)(_.settingsClass.getSimpleName),
      line("identity services", ext.identityServiceProviders)(_.settingsClass.getSimpleName),
      line("schema inferrers", ext.schemaInferrers)(_.settingsClass.getSimpleName),
      line("CLI commands", ext.cliCommands.flatMap(c => c.name +: c.aliases).sorted)(identity),
      line("source failure descriptors", ext.sourceFailureDescriptors)(_.getClass.getSimpleName),
      line("lookup observers", ext.lookupObservers)(_.getClass.getSimpleName),
      ext.streamingProvider.map(p => s"streaming provider: ${p.getClass.getSimpleName}"),
      ext.schedulerProvider.map(p => s"scheduler provider: ${p.getClass.getSimpleName}"),
      ext.resourceValidatorProvider.map(p => s"resource validator: ${p.getClass.getSimpleName}"),
      line("spark conf keys", ext.sparkConfContributions.keys.toSeq.sorted)(identity)
    ).flatten ++ ext.extraCapabilities
  }
}
