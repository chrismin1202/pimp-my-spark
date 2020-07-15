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

import com.chrism.commons.FunTestSuite
import com.chrism.spark.SparkTestSuiteLike
import com.chrism.spark.aws.{AwsAnonymousCredentials, AwsCredentials}

final class PimpMySparkSessionTest extends FunTestSuite with SparkTestSuiteLike with PimpMySparkSession {

  import com.chrism.spark.aws.AwsHadoopConfiguration.{
    AccessKeyName,
    AnonymousCredentialsProviderCls,
    CredentialsProviderName,
    S3aFileSystemCls,
    S3aImplName,
    SecretKeyName,
    SimpleCredentialsProviderCls
  }
  import spark_session_implicits._

  test("setting AWS credentials: AwsCredentials") {
    spark.setHadoopConf(CredentialsProviderName, "")
    spark.setS3aAccess(AwsCredentials("access-key", "secret-key"))
    val hadoopConf = spark.hadoopConf
    assert(hadoopConf.get(S3aImplName) === S3aFileSystemCls.getName)
    assert(hadoopConf.get(CredentialsProviderName) === SimpleCredentialsProviderCls.getName)
    assert(hadoopConf.get(AccessKeyName) === "access-key")
    assert(hadoopConf.get(SecretKeyName) === "secret-key")
  }

  test("setting AWS credentials: AwsAnonymousCredentials") {
    spark.setHadoopConf(CredentialsProviderName, "")
    spark.setS3aAccess(AwsAnonymousCredentials)
    val hadoopConf = spark.hadoopConf
    assert(hadoopConf.get(S3aImplName) === S3aFileSystemCls.getName)
    assert(hadoopConf.get(CredentialsProviderName) === AnonymousCredentialsProviderCls.getName)
    assert(hadoopConf.get(AccessKeyName) === "access-key")
    assert(hadoopConf.get(SecretKeyName) === "secret-key")
  }
}
