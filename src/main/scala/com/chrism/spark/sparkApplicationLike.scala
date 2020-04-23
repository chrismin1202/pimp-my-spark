/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *      http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.chrism.spark

import cats.effect.{IO, Resource}
import com.chrism.spark.ApplicationStatus._
import org.apache.spark.sql.SparkSession

import scala.collection.mutable

trait SparkApplicationLike extends MainLike {
  this: SparkSessionLike =>

  def stages(args: Array[String])(implicit spark: SparkSession): Seq[SparkStage]

  /** Runs the Spark application defined in [[stages()]].
    *
    * @param args the command line arguments
    * @throws RuntimeException thrown if any of the stages returns failed status
    */
  override final def run(args: Array[String]): Unit = {
    import SparkApplicationLike.FailedStatuses

    val statuses = Resource
      .make(IO(getOrCreateSparkSession()))(s => IO.delay(s.stop()))
      .use(s => IO(runStages(args)(s)))
      .unsafeRunSync()
    if (statuses.exists(FailedStatuses.contains)) {
      val formattedStatuses = statuses.zipWithIndex
        .map(t => s"  Stage ${t._2 + 1}: ${t._1}")
        .mkString(System.lineSeparator())
      throw new RuntimeException(s"""One or more of the stages failed!
                                    |Statuses:
                                    |$formattedStatuses""".stripMargin)
    }
  }

  private[spark] def runStages(args: Array[String])(implicit spark: SparkSession): Seq[ApplicationStatus] = {
    val sparkStages = stages(args)
    val statuses = new mutable.ArrayBuffer[ApplicationStatus](sparkStages.size)
    val iterator = sparkStages.iterator
    var prevStatus: ApplicationStatus = ApplicationStatus.Success
    while (iterator.hasNext) {
      val stage = iterator.next()
      prevStatus match {
        case Success | FailedContinue | SkippedContinue => prevStatus = stage.run(args)
        case Failed                                     => prevStatus = SkippedFailed
        case Stop                                       => prevStatus = SkippedStop
        case SkippedStop | SkippedFailed                => // preserve prevStatus
        case other                                      =>
          // Throw an exception to ensure that the matching is exhaustive
          throw new UnsupportedOperationException(s"$other is not supported ApplicationStatus!")
      }
      statuses += prevStatus
    }
    assert(statuses.size == sparkStages.size)
    statuses
  }
}

private[this] object SparkApplicationLike {

  private val FailedStatuses: Set[ApplicationStatus] = Set(Failed, Stop, SkippedStop, SkippedFailed)
}

trait SimpleSparkApplicationLike extends SparkApplicationLike {
  this: SparkSessionLike =>

  /** Override to implement the Spark logic.
    *
    * @param args the command line arguments
    * @param spark the [[SparkSession]] passed in implicitly
    * @return the status of the application
    */
  def stage(args: Array[String])(implicit spark: SparkSession): ApplicationStatus

  override final def stages(args: Array[String])(implicit spark: SparkSession): Seq[SparkStage] =
    Seq(
      new SparkStage {
        override def runSpark(args: Array[String])(implicit spark: SparkSession): ApplicationStatus = stage(args)
      }
    )
}
