package io.onfhir.tofhir.model

import org.apache.spark.util.AccumulatorV2

import scala.collection.mutable

/**
 * Mapping job statistics
 * @param mappingTaskStats  Stats for each mapping task (mapping url -> stats)
 */
case class MappingJobStats(mappingTaskStats:mutable.Map[String, MappingTaskStats]= new mutable.HashMap[String, MappingTaskStats]())

/**
 * Statistics for a mapping executed on some data
 * @param numOfRecordsMapped         Total number of source records mapped successfully
 * @param numOfRecordsFailed         Total number of source records where mapping is failed
 * @param numOfResultingResources    Total number of resulting FHIR resources as a result of mapping
 */
case class MappingTaskStats(  var numOfRecordsMapped:Long = 0, var numOfRecordsFailed:Long = 0, var numOfResultingResources:Long = 0)

/**
 * Spark accumulator to accumalate overall mapping job statistics
 * @param mappingJobStats Mapping job statistics
 */
class MappingJobAccumulator(val mappingJobStats:MappingJobStats = MappingJobStats()) extends AccumulatorV2[IMappingResult, MappingJobStats]{
  /**
   * Returns if this accumulator is zero value or not.
   * @return
   */
  override def isZero: Boolean = mappingJobStats.mappingTaskStats.values.forall(ts => ts.numOfRecordsMapped == 0 && ts.numOfRecordsFailed == 0)

  /**
   * Creates a new copy of this accumulator.
   * @return
   */
  override def copy(): AccumulatorV2[IMappingResult, MappingJobStats] = new MappingJobAccumulator(mappingJobStats.copy())

  /**
   * Resets this accumulator, which is zero value.
   */
  override def reset(): Unit =
    mappingJobStats
      .mappingTaskStats
      .clear()

  /**
   * Takes the inputs and accumulates.
   * @param v A result of mapping of a source record
   */
  override def add(v: IMappingResult): Unit = {
    v match {
      case SuccessfulMappingResult(mappingRef, n) =>
        mappingJobStats.mappingTaskStats.get(mappingRef) match {
          case None => mappingJobStats.mappingTaskStats.put(mappingRef, MappingTaskStats(1, 0, n))
          case Some(mts) =>
            mts.numOfRecordsMapped = mts.numOfRecordsMapped + 1
            mts.numOfResultingResources = mts.numOfResultingResources + n
        }
      case FailedMappingResult(mappingRef, _) =>
        mappingJobStats.mappingTaskStats.get(mappingRef) match {
          case None => mappingJobStats.mappingTaskStats.put(mappingRef, MappingTaskStats(0, 1, 0))
          case Some(mts) =>
            mts.numOfRecordsFailed = mts.numOfRecordsFailed + 1
        }
    }
  }

  /**
   * Merges another same-type accumulator into this one and update its state, i.e.
   * @param other Other accumulator results
   */
  override def merge(other: AccumulatorV2[IMappingResult, MappingJobStats]): Unit = {
    other match {
      case mjo:MappingJobAccumulator =>
        mjo.mappingJobStats
          .mappingTaskStats
          .foreach {
            case (mappingRef, otherMts) =>
              mappingJobStats.mappingTaskStats.get(mappingRef) match {
                case None => mappingJobStats.mappingTaskStats.put(mappingRef, otherMts)
                case Some(currentMts) =>
                  currentMts.numOfRecordsFailed = currentMts.numOfRecordsFailed + otherMts.numOfRecordsFailed
                  currentMts.numOfRecordsMapped = currentMts.numOfRecordsMapped + otherMts.numOfRecordsMapped
                  currentMts.numOfResultingResources = currentMts.numOfResultingResources + otherMts.numOfResultingResources
              }
          }
    }
  }

  /**
   * Defines the current value of this accumulator
   * @return
   */
  override def value: MappingJobStats = mappingJobStats
}

/**
 * Trait for any mapping result for mapping a source record
 */
trait IMappingResult {
  val mappingRef:String
}

/**
 * A successful mapping result
 * @param mappingRef          Mapping url
 * @param numOfFhirResources  Number of resulting FHIR resources created
 */
case class SuccessfulMappingResult(mappingRef:String, numOfFhirResources:Long) extends IMappingResult

/**
 * A failed mapping result
 * @param mappingRef          Mapping url
 * @param failedSourceData    JSON serialization of source data row that is failed
 */
case class FailedMappingResult(mappingRef:String, failedSourceData:String) extends IMappingResult