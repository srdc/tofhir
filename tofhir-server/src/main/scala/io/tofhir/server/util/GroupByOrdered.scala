package io.tofhir.server.util

import scala.collection.mutable
import scala.language.implicitConversions

/**
 * Object to be imported so that Sequences (Seq) have a new functions called groupByASequenceOrdered
 * which is a groupBy implementation that keeps the order of Sequence entries while creating the Map of the
 * groups. The return type is the Seq of those Map entries.
 */
object GroupByOrdered {

  class OrderedGrouped[A](xs: Seq[A]) {

    /**
     * Helper, parent function to handle type conversion from immutable.Seq to collection.Seq
     */
    def groupByASequenceOrdered[K](f: A => K): Seq[(K, Seq[A])] = {
      _groupByASequenceOrdered(f).toSeq.map(s => s._1 -> s._2.toSeq)
    }

    /**
     * Implementation of groupBy with LinedHashMap and ArrayBuffer.
     */
    private def _groupByASequenceOrdered[K](f: A => K): collection.Seq[(K, collection.Seq[A])] = {
      val m = mutable.LinkedHashMap.empty[K, collection.Seq[A]].withDefault(_ => new mutable.ArrayBuffer[A])
      xs.foreach { x =>
        val k = f(x)
        m(k) = m(k) :+ x
      }
      m.toSeq
    }

  }

  /**
   * The method added to Seq types.
   */
  implicit def groupByOrdered[A](seq: Seq[A]): OrderedGrouped[A] = new OrderedGrouped[A](seq)
}
