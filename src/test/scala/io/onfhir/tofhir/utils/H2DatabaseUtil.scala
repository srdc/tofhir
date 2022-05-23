package io.onfhir.tofhir.utils

import com.typesafe.scalalogging.Logger

import java.io.FileNotFoundException
import java.nio.charset.StandardCharsets
import java.sql.{Connection, DriverManager, Statement}
import scala.io.{BufferedSource, Source}

object H2DatabaseUtil {
  val logger: Logger = Logger(this.getClass)

  val DATABASE_URL = "jdbc:h2:mem:inputDb;MODE=PostgreSQL;DB_CLOSE_DELAY=-1;DATABASE_TO_UPPER=FALSE"

  def runSqlQuery(fileName: String): Unit = {
    logger.info("Running query file: " +  fileName)
    val con: Connection = DriverManager.getConnection(DATABASE_URL)
    val stm: Statement = con.createStatement
    try{
      val source: BufferedSource = Source.fromFile(getClass.getResource("/sql/" + fileName).toURI.getPath, StandardCharsets.UTF_8.name())
      val sql: String = try source.mkString finally source.close()
      stm.execute(sql)
    }catch {
      case _: FileNotFoundException =>
        val msg = "The file cannot be found at the specified path"
        logger.error(msg)
    }
  }

}
