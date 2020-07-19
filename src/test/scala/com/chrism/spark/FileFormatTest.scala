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

import com.chrism.commons.FunTestSuite

final class FileFormatTest extends FunTestSuite {

  test("parsing format name: csv") {
    assert(FileFormat.valueOf("csv") === FileFormat.Csv)
    assert(FileFormat.valueOf("CSV") === FileFormat.Csv)
    assert(FileFormat.valueOf("Csv") === FileFormat.Csv)
  }

  test("parsing format name: json") {
    assert(FileFormat.valueOf("json") === FileFormat.Json)
    assert(FileFormat.valueOf("JSON") === FileFormat.Json)
    assert(FileFormat.valueOf("Json") === FileFormat.Json)
  }

  test("parsing format name: orc") {
    assert(FileFormat.valueOf("orc") === FileFormat.Orc)
    assert(FileFormat.valueOf("ORC") === FileFormat.Orc)
    assert(FileFormat.valueOf("Orc") === FileFormat.Orc)
  }

  test("parsing format name: parquet") {
    assert(FileFormat.valueOf("parquet") === FileFormat.Parquet)
    assert(FileFormat.valueOf("PARQUET") === FileFormat.Parquet)
    assert(FileFormat.valueOf("Parquet") === FileFormat.Parquet)
  }

  test("parsing format name: text") {
    assert(FileFormat.valueOf("text") === FileFormat.Text)
    assert(FileFormat.valueOf("TEXT") === FileFormat.Text)
    assert(FileFormat.valueOf("Text") === FileFormat.Text)
  }
}
