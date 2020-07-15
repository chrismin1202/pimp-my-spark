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

import com.chrism.commons.util.StringUtils

import scala.util.matching.Regex

object AwsCredentialsHadoopConfiguration {

  val CredentialsProviderName: String = "fs.s3a.aws.credentials.provider"
  val AccessKeyName: String = "fs.s3a.access.key"
  val SecretKeyName: String = "fs.s3a.secret.key"

  val SimpleCredentialsProvider: Class[_] = classOf[org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider]
  val AnonymousCredentialsProvider: Class[_] = classOf[org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider]
  val SupportedCredentialsProviders: Map[String, Int] = Map(
    SimpleCredentialsProvider.getName -> 1000,
    AnonymousCredentialsProvider.getName -> 100,
  )

  private val CommaSplit: Regex = ",\\s*".r

  private[aws] def addProvider(provider: Class[_], chainedProviders: String): String =
    if (StringUtils.isBlank(chainedProviders)) provider.getName
    else if (chainedProviders.contains(provider.getName)) chainedProviders
    else
      (CommaSplit.split(chainedProviders) :+ provider.getName)
        .map(p => p -> SupportedCredentialsProviders.getOrElse(p, 0))
        .sortBy(_._2)(Ordering.Int.reverse)
        .map(_._1)
        .mkString(",")
}
