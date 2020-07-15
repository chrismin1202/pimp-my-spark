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
