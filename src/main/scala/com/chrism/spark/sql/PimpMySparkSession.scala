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
package com.chrism.spark.sql

import com.chrism.spark.aws.AwsCredentialsLike
import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.SparkSession

trait PimpMySparkSession {

  /** The object that contains implicit methods for [[SparkSession]].
    *
    * To use the implicit methods defined in this object, simply import the entire object in your scope:
    * {{{ import spark_session_implicits._ }}}
    */
  object spark_session_implicits {

    implicit final class SparkSessionOps(ss: SparkSession) {

      import com.chrism.spark.aws.AwsHadoopConfiguration.{S3aFileSystemCls, S3aImplName}

      /** @return the Hadoop [[Configuration]] instance associated with the given [[SparkSession]] */
      def hadoopConf: Configuration = ss.sparkContext.hadoopConfiguration

      /** Sets the given name-value pair to the Hadoop [[Configuration]] instance.
        *
        * @param name the name of the configuration
        * @param value the value of the configuration
        * @throws IllegalArgumentException thrown when the name and/or value is null
        */
      def setHadoopConf(name: String, value: String): Unit = hadoopConf.set(name, value)

      /** Sets the given [[AwsCredentialsLike]] instance to the Hadoop [[Configuration]] instance.
        *
        * @param awsCreds the AWS credentials to use for accessing S3
        */
      def setS3aAccess(awsCreds: AwsCredentialsLike): Unit = {
        val conf = hadoopConf
        conf.set(S3aImplName, S3aFileSystemCls.getName)
        awsCreds.setToHadoopConf(conf)
      }
    }
  }
}
