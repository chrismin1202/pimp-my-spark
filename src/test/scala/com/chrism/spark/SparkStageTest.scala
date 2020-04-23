package com.chrism.spark

import org.apache.spark.sql.SparkSession

import scala.util.{Failure, Random, Success, Try}

final class SparkStageTest extends SparkFunTestSuite {

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
