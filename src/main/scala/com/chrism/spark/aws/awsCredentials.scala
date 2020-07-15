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

sealed trait AwsCredentialsLike extends Product with Serializable {

  def accessKey: String

  def secretKey: String
}

final case class AwsCredentials(accessKey: String, secretKey: String) extends AwsCredentialsLike {

  require(StringUtils.isNotBlank(accessKey), "The access key cannot be blank!")
  require(StringUtils.isNotBlank(secretKey), "The secret key cannot be blank!")

  override def toString: String = s"$productPrefix($accessKey,${AwsCredentials.obfuscate(secretKey)})"
}

object AwsCredentials {

  private[aws] def obfuscate(key: String): String = {
    val len = key.length
    if (len > 2) key(0) +: ("*" * (len - 1))
    else "*" * len
  }
}

case object AwsAnonymousCredentials extends AwsCredentialsLike {

  override val accessKey: String = null

  override val secretKey: String = null
}
