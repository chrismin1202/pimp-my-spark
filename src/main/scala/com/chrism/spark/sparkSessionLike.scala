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

import org.apache.spark.sql.SparkSession

trait SparkSessionLike {

  /** Override to add Spark related configurations to to the given [[SparkSession.Builder]] instance.
    *
    * If cake pattern is employed, this trait should not be overridden as {{{ final }}}.
    *
    * @param builder the [[SparkSession.Builder]] instance to build upon
    * @return the builder instance with additional configurations added
    */
  protected def buildSparkSession(builder: SparkSession.Builder): SparkSession.Builder = builder

  protected final def getOrCreateSparkSession(): SparkSession = buildSparkSession(SparkSession.builder()).getOrCreate()
}

trait LocalSparkSessionLike extends SparkSessionLike {

  override protected def buildSparkSession(builder: SparkSession.Builder): SparkSession.Builder =
    super.buildSparkSession(builder).master("local[*]")
}

trait HiveSparkSessionLike extends SparkSessionLike {

  override protected def buildSparkSession(builder: SparkSession.Builder): SparkSession.Builder =
    super.buildSparkSession(builder).enableHiveSupport()
}
