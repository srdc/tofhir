package io.tofhir.server.common.config

import com.typesafe.config.Config

import scala.util.Try

/**
 * Configuration for the Http serving (host, port, etc)
 * @param webserverConfig
 */
class WebServerConfig(webserverConfig: Config) {

  /** Host name/address to start service on. */
  lazy val serverHost:String = Try(webserverConfig.getString("host")).getOrElse("localhost")

  /** Port to start service on. */
  lazy val serverPort:Int = Try(webserverConfig.getInt("port")).getOrElse(8085)

  /** SSL settings if server will be server over SSL */
  lazy val serverSsl:Boolean = Try(webserverConfig.getConfig("ssl").isEmpty || webserverConfig.getString("ssl.keystore") != null).getOrElse(false)
  lazy val sslKeystorePath: Option[String] = Try(webserverConfig.getString("ssl.keystore")).toOption
  lazy val sslKeystorePassword: Option[String] = Try(webserverConfig.getString("ssl.password")).toOption

  /** Protocol of the server. Either http or https and the value is determined from spray properties */
  lazy val serverProtocol:String = if(serverSsl) "https" else "http"

  /** Full address of the server */
  lazy val serverLocation:String =  serverProtocol + "://" + serverHost + ":" + serverPort

  /** Base URI for Services to be served from */
  lazy val baseUri:String = Try(webserverConfig.getString("base-uri")).getOrElse("tofhir")
}
