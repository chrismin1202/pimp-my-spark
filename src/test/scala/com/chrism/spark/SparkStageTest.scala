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

import com.chrism.commons.FunTestSuite
import org.apache.spark.sql.SparkSession

import scala.util.{Failure, Random, Success, Try}

final class SparkStageTest extends FunTestSuite with SparkTestSuiteLike {

  test("implementing a simple Spark application") {
    val stage = new SparkStage {
      override def runSpark(args: Array[String])(implicit spark: SparkSession): ApplicationStatus = {
        import spark.implicits._

        val testChar = args
          .grouped(2)
          .collectFirst {
            case Array("--test-char", c) =>
              c should have length 1
              assert(c >= "a" && c <= "z", "The character must be between a and z")
              c
          }
          .getOrElse(fail(s"Failed to parse the test character from ${args.mkString(" ")}"))
        val testCharBroadcast = spark.sparkContext.broadcast(testChar)
        val wordCounts = spark
          .createDataset((97 until (97 + 26)).map(_.toChar.toString))
          .filter(_ != testCharBroadcast.value)
          .flatMap(Seq.fill(1 + Random.nextInt(10))(_))
          .map(c => (c, 1))
          .groupByKey(_._1)
          .reduceGroups((a, b) => (a._1, a._2 + b._2))
          .map(_._2)
          .collect()
        Try {
          // There are 26 characters in total, but 1 char (testChar) is excluded
          wordCounts should have length 25
          assert(wordCounts.exists(_._1 == testChar) === false)
        } match {
          case Success(_) => ApplicationStatus.Success
          case Failure(_) => ApplicationStatus.Failed
        }
      }
    }
    assert(stage.run(Array("--test-char", "z")) === ApplicationStatus.Success)
  }
}
