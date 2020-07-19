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

final class PimpMyDataFrameReaderTest extends FunTestSuite {

  test("formatting S3 path only with bucket name") {
    val path = PimpMyDataFrameReader.formatS3aPath("bucket")
    assert(path === "s3a://bucket")
  }

  test("formatting S3 path with 1 relative path") {
    val path = PimpMyDataFrameReader.formatS3aPath("bucket", "path-within-bucket")
    assert(path === "s3a://bucket/path-within-bucket")
  }

  test("formatting S3 path with multiple relative paths") {
    val path = PimpMyDataFrameReader.formatS3aPath("bucket", "outer", "inner")
    assert(path === "s3a://bucket/outer/inner")
  }
}
