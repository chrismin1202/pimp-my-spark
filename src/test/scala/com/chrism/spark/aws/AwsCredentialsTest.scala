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

final class AwsCredentialsTest extends FunTestSuite {

  test("obfuscating key in AwsCredentials: 1 character") {
    assert(AwsCredentials.obfuscate("a") === "*")
  }

  test("obfuscating key in AwsCredentials: 2 characters") {
    assert(AwsCredentials.obfuscate("ab") === "**")
  }

  test("obfuscating key in AwsCredentials: more than 2 characters") {
    assert(AwsCredentials.obfuscate("abc") === "a**")
  }

  test("overriding toString to obfuscate secretKey in AwsCredentials") {
    val creds = AwsCredentials("access-key", "secret-key")
    assert(creds.toString === "AwsCredentials(access-key,s*********)")
  }
}
