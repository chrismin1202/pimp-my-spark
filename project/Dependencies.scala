import sbt.librarymanagement.InclExclRule

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
object Dependencies {

  import sbt._

  private val SparkGroupId: String = "org.apache.spark"

  private val SparkVersion: String = "3.0.0"
  // Spark 3.0.0 is compiled with Hadoop 2.7.4,
  // but the corresponding version of hadoop-aws's anonymous S3 access support is a little wonky.
  private val HadoopVersion: String = "3.2.1"

  private val ScalacheckVersion: String = "1.14.0"
  private val ScalatestVersion: String = "3.0.8"
  private val Specs2CoreVersion: String = "4.7.0"

  private val ExcludeHadoop: InclExclRule = ExclusionRule("org.apache.hadoop")

  val Commons4s: ModuleID = "com.chrism" %% "commons4s" % "1.2.0"

  val SparkCore: ModuleID = (SparkGroupId %% "spark-core" % SparkVersion).excludeAll(ExcludeHadoop)
  val SparkSql: ModuleID = (SparkGroupId %% "spark-sql" % SparkVersion).excludeAll(ExcludeHadoop)
  val SparkHive: ModuleID = (SparkGroupId %% "spark-hive" % SparkVersion).excludeAll(ExcludeHadoop)
  val HadoopClient: ModuleID = "org.apache.hadoop" % "hadoop-client" % HadoopVersion
  val HadoopAws: ModuleID = "org.apache.hadoop" % "hadoop-aws" % HadoopVersion

  val CatsEffect: ModuleID = "org.typelevel" %% "cats-effect" % "2.1.3"

  val Scalacheck: ModuleID = "org.scalacheck" %% "scalacheck" % ScalacheckVersion
  val Scalatest: ModuleID = "org.scalatest" %% "scalatest" % ScalatestVersion
  val Specs2Core: ModuleID = "org.specs2" %% "specs2-core" % Specs2CoreVersion
}
