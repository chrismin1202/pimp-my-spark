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

import com.chrism.commons.log.Log4jConfigurer
import org.apache.log4j
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, Suite, SuiteMixin}

trait TestSparkSessionLike extends SuiteMixin with BeforeAndAfterAll with SparkSessionLike {
  this: Suite =>

  @transient
  protected implicit final lazy val spark: SparkSession = getOrCreateSparkSession()

  /** A map of log4j log levels for the loggers that log a lot of information.
    *
    * Override to add or change log levels.
    * Note that this method should be overridden by adding additional log levels
    * {{{
    *   override protected def logLevels: Map[String, Level] = super.logLevels + ("name" -> Level.OFF)
    * }}}
    * rather than completely overriding unless all of the default log levels need to be overridden.
    *
    * @return the log name and log4j [[log4j.Level]] pairs
    */
  protected def logLevels: Map[String, log4j.Level] =
    Map(
      "org.apache.spark" -> log4j.Level.WARN,
      "org.sparkproject" -> log4j.Level.WARN,
      "io.netty" -> log4j.Level.WARN,
    )

  private[this] def setLogLevels(): Unit = Log4jConfigurer.setLevels(logLevels)

  override protected def buildSparkSession(builder: SparkSession.Builder): SparkSession.Builder =
    super.buildSparkSession(builder).master("local[*]")

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    setLogLevels()
  }

  override protected def afterAll(): Unit = {
    super.afterAll()
    spark.stop()
  }
}
