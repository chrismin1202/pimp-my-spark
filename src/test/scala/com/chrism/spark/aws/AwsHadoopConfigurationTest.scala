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
package com.chrism.spark.aws

import com.chrism.commons.FunTestSuite

final class AwsHadoopConfigurationTest extends FunTestSuite {

  import AwsHadoopConfiguration.{addCredentialsProvider, AnonymousCredentialsProviderCls, SimpleCredentialsProviderCls}

  test("adding a provider: AnonymousAWSCredentialsProvider") {
    val providers = addCredentialsProvider(
      AnonymousCredentialsProviderCls,
      s"${SimpleCredentialsProviderCls.getName},com.other.provider1, com.other.provider2")
    val expected = "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider," +
      "org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider," +
      "com.other.provider1," +
      "com.other.provider2"
    assert(providers === expected)
  }

  test("adding a provider: SimpleAWSCredentialsProvider") {
    val providers = addCredentialsProvider(
      SimpleCredentialsProviderCls,
      s"${AnonymousCredentialsProviderCls.getName}, com.other.provider1, com.other.provider2")
    val expected = "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider," +
      "org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider," +
      "com.other.provider1," +
      "com.other.provider2"
    assert(providers === expected)
  }
}
