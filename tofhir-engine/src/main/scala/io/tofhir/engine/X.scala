package io.tofhir.engine

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.util.control.Breaks

object X {
  def main(args: Array[String]): Unit = {
    for(a <- 1 to 10) {
      if (a == 5) Breaks.break()
      println(a)
    }
  }

  def x(): Unit = {
    Future.apply {
      Thread.sleep(2000)
      println("X")
    }
  }
}
