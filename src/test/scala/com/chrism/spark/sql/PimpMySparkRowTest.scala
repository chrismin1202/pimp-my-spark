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

import java.time.{LocalDate, LocalDateTime, Month}

import com.chrism.commons.FunTestSuite
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}

final class PimpMySparkRowTest extends FunTestSuite with PimpMySparkRow {

  import spark_row_implicits._

  test("pimping string column") {
    val schema = StructType(
      Seq(
        StructField("col", DataTypes.StringType),
        StructField("blank_col", DataTypes.StringType),
        StructField("null_col", DataTypes.StringType),
      ))
    val row = RowBuilder(schema)
      .withStringValue("col", "s")
      .withStringValue("blank_col", "")
      .build()

    assert(row.getStringByName("col") === "s")

    assertOption("", row.getStringByNameOrNone("blank_col"))
    assert(row.getNonBlankStringByNameOrNone("blank_col").isEmpty)

    assertNull(row.getStringByName("null_col"))
    assert(row.getStringByNameOrNone("null_col").isEmpty)

    intercept[IllegalArgumentException] {
      row.getStringByName("no_such_col")
    }
    assert(row.getStringByNameOrNone("no_such_col").isEmpty)
  }

  test("pimping boolean column") {
    val schema = StructType(
      Seq(
        StructField("col", DataTypes.BooleanType),
        StructField("null_col", DataTypes.BooleanType),
      ))
    val row = RowBuilder(schema)
      .withBooleanValue("col", false)
      .withNullBooleanValue("null_col")
      .build()

    assert(row.getBooleanByName("col") === false)

    assertNull(row.getBooleanByNameOrNull("null_col"))
    assert(row.getBooleanByNameOrNone("null_col").isEmpty)

    intercept[IllegalArgumentException] {
      row.getBooleanByName("no_such_col")
    }
    assert(row.getBooleanByNameOrNone("no_such_col").isEmpty)
  }

  test("pimping short column") {
    val schema = StructType(
      Seq(
        StructField("col", DataTypes.ShortType),
        StructField("null_col", DataTypes.ShortType),
      ))
    val row = RowBuilder(schema)
      .withShortValue("col", 1)
      .withNullShortValue("null_col")
      .build()

    assert(row.getShortByName("col") === 1.toShort)

    assertNull(row.getShortByNameOrNull("null_col"))
    assert(row.getShortByNameOrNone("null_col").isEmpty)

    intercept[IllegalArgumentException] {
      row.getShortByName("no_such_col")
    }
    assert(row.getShortByNameOrNone("no_such_col").isEmpty)
  }

  test("pimping byte column") {
    val schema = StructType(
      Seq(
        StructField("col", DataTypes.ByteType),
        StructField("null_col", DataTypes.ByteType),
      ))
    val row = RowBuilder(schema)
      .withByteValue("col", 2)
      .withNullByteValue("null_col")
      .build()

    assert(row.getByteByName("col") === 2.toByte)

    assertNull(row.getByteByNameOrNull("null_col"))
    assert(row.getByteByNameOrNone("null_col").isEmpty)

    intercept[IllegalArgumentException] {
      row.getByteByName("no_such_col")
    }
    assert(row.getByteByNameOrNone("no_such_col").isEmpty)
  }

  test("pimping int column") {
    val schema = StructType(
      Seq(
        StructField("col", DataTypes.IntegerType),
        StructField("null_col", DataTypes.IntegerType),
      ))
    val row = RowBuilder(schema)
      .withIntValue("col", 3)
      .withNullIntValue("null_col")
      .build()

    assert(row.getIntByName("col") === 3)

    assertNull(row.getIntByNameOrNull("null_col"))
    assert(row.getIntByNameOrNone("null_col").isEmpty)

    intercept[IllegalArgumentException] {
      row.getIntByName("no_such_col")
    }
    assert(row.getIntByNameOrNone("no_such_col").isEmpty)
  }

  test("pimping long column") {
    val schema = StructType(
      Seq(
        StructField("col", DataTypes.LongType),
        StructField("null_col", DataTypes.LongType),
      ))
    val row = RowBuilder(schema)
      .withLongValue("col", 4L)
      .withNullLongValue("null_col")
      .build()

    assert(row.getLongByName("col") === 4L)

    assertNull(row.getLongByNameOrNull("null_col"))
    assert(row.getLongByNameOrNone("null_col").isEmpty)

    intercept[IllegalArgumentException] {
      row.getLongByName("no_such_col")
    }
    assert(row.getLongByNameOrNone("no_such_col").isEmpty)
  }

  test("pimping float column") {
    val schema = StructType(
      Seq(
        StructField("col", DataTypes.FloatType),
        StructField("null_col", DataTypes.FloatType),
      ))
    val row = RowBuilder(schema)
      .withFloatValue("col", 5.0f)
      .withNullFloatValue("null_col")
      .build()

    assert(row.getFloatByName("col") === 5.0f)

    assertNull(row.getFloatByNameOrNull("null_col"))
    assert(row.getFloatByNameOrNone("null_col").isEmpty)

    intercept[IllegalArgumentException] {
      row.getFloatByName("no_such_col")
    }
    assert(row.getFloatByNameOrNone("no_such_col").isEmpty)
  }

  test("pimping double column") {
    val schema = StructType(
      Seq(
        StructField("col", DataTypes.DoubleType),
        StructField("null_col", DataTypes.DoubleType),
      ))
    val row = RowBuilder(schema)
      .withDoubleValue("col", 6.0)
      .withNullDoubleValue("null_col")
      .build()

    assert(row.getDoubleByName("col") === 6.0)

    assertNull(row.getDoubleByNameOrNull("null_col"))
    assert(row.getDoubleByNameOrNone("null_col").isEmpty)

    intercept[IllegalArgumentException] {
      row.getDoubleByName("no_such_col")
    }
    assert(row.getDoubleByNameOrNone("no_such_col").isEmpty)
  }

  test("pimping decimal column: BigDecimal") {
    val schema = StructType(
      Seq(
        StructField("col", DataTypes.createDecimalType(5, 2)),
        StructField("null_col", DataTypes.createDecimalType()),
      ))
    val row = RowBuilder(schema)
      .withBigDecimalValue("col", BigDecimal("123.45"))
      .withBigDecimalValue("null_col", null)
      .build()

    assertBigDecimal(BigDecimal("123.45"), row.getBigDecimalByName("col"))

    assertNull(row.getBigDecimalByName("null_col"))
    assert(row.getBigDecimalByNameOrNone("null_col").isEmpty)

    intercept[IllegalArgumentException] {
      row.getBigDecimalByName("no_such_col")
    }
    assert(row.getBigDecimalByNameOrNone("no_such_col").isEmpty)
  }

  test("pimping decimal column: BigInt") {
    val schema = StructType(
      Seq(
        StructField("col", DataTypes.createDecimalType(6, 0)),
        StructField("null_col", DataTypes.createDecimalType(7, 0)),
      ))
    val row = RowBuilder(schema)
      .withBigIntValue("col", BigInt("123456"))
      .withBigIntValue("null_col", null)
      .build()

    assert(row.getBigIntByName("col") === BigInt("123456"))

    assertNull(row.getBigIntByName("null_col"))
    assert(row.getBigIntByNameOrNone("null_col").isEmpty)

    intercept[IllegalArgumentException] {
      row.getBigIntByName("no_such_col")
    }
    assert(row.getBigIntByNameOrNone("no_such_col").isEmpty)
  }

  test("pimping date column") {
    val schema = StructType(
      Seq(
        StructField("col", DataTypes.DateType),
        StructField("null_col", DataTypes.DateType),
      ))
    val row = RowBuilder(schema)
      .withDateValue("col", 2020, Month.JANUARY, 1)
      .withLocalDateValue("null_col", null)
      .build()

    assert(row.getLocalDateByName("col") === LocalDate.of(2020, 1, 1))

    assertNull(row.getLocalDateByName("null_col"))
    assert(row.getLocalDateByNameOrNone("null_col").isEmpty)

    intercept[IllegalArgumentException] {
      row.getLocalDateByName("no_such_col")
    }
    assert(row.getLocalDateByNameOrNone("no_such_col").isEmpty)
  }

  test("pimping timestamp column") {
    val schema = StructType(
      Seq(
        StructField("col", DataTypes.TimestampType),
        StructField("null_col", DataTypes.TimestampType),
      ))
    val row = RowBuilder(schema)
      .withDateTimeValue("col", 2020, Month.JANUARY, 2, 3, 4)
      .withLocalDateTimeValue("null_col", null)
      .build()

    assert(row.getLocalDateTimeByName("col") === LocalDateTime.of(2020, 1, 2, 3, 4))

    assertNull(row.getLocalDateTimeByName("null_col"))
    assert(row.getLocalDateTimeByNameOrNone("null_col").isEmpty)

    intercept[IllegalArgumentException] {
      row.getLocalDateTimeByName("no_such_col")
    }
    assert(row.getLocalDateTimeByNameOrNone("no_such_col").isEmpty)
  }

  test("pimping array column") {
    val schema = StructType(
      Seq(
        StructField("col", DataTypes.createArrayType(DataTypes.IntegerType)),
        StructField("empty_col", DataTypes.createArrayType(DataTypes.StringType)),
        StructField("null_col", DataTypes.createArrayType(DataTypes.DoubleType)),
      ))
    val row = RowBuilder(schema)
      .withSeqValue("col", Seq(1, 2, 3))
      .withSeqValue("empty_col", Seq.empty[String])
      .build()

    row.getSeqByName[Int]("col") should contain theSameElementsInOrderAs Seq(1, 2, 3)

    assertOption(Seq.empty[String], row.getSeqByNameOrNone[String]("empty_col"))
    assert(row.getNonEmptySeqByNameOrNone[String]("empty_col").isEmpty)

    assertNull(row.getSeqByName[Double]("null_col"))
    assert(row.getSeqByNameOrNone[Double]("null_col").isEmpty)

    intercept[IllegalArgumentException] {
      row.getSeqByName[Any]("no_such_col")
    }
    assert(row.getSeqByNameOrNone[Any]("no_such_col").isEmpty)
  }

  test("pimping map column") {
    val schema = StructType(
      Seq(
        StructField("col", DataTypes.createMapType(DataTypes.StringType, DataTypes.IntegerType)),
        StructField("empty_col", DataTypes.createMapType(DataTypes.LongType, DataTypes.StringType)),
        StructField("null_col", DataTypes.createMapType(DataTypes.BooleanType, DataTypes.DoubleType)),
      ))
    val row = RowBuilder(schema)
      .withMapValue("col", Map("one" -> 1, "two" -> 2, "three" -> 3))
      .withMapValue("empty_col", Map.empty[String, Int])
      .build()

    row.getMapByName[String, Int]("col") should contain theSameElementsAs Map("one" -> 1, "two" -> 2, "three" -> 3)

    assertOption(Map.empty[Long, String], row.getMapByNameOrNone[Long, String]("empty_col"))
    assert(row.getNonEmptyMapByNameOrNone[Long, String]("empty_col").isEmpty)

    assertNull(row.getMapByName[Boolean, Double]("null_col"))
    assert(row.getMapByNameOrNone[Boolean, Double]("null_col").isEmpty)

    intercept[IllegalArgumentException] {
      row.getMapByName[Any, Any]("no_such_col")
    }
    assert(row.getMapByNameOrNone[Any, Any]("no_such_col").isEmpty)
  }

  test("pimping struct column") {
    import PimpMySparkRowTest.{Dummy, DummySchema}

    val schema = StructType(
      Seq(
        StructField("col", DummySchema),
        StructField("null_col", DummySchema),
      ))
    val row = RowBuilder(schema)
      .withStructValue("col", Dummy(1L, false))
      .withStructValue("null_col", null)
      .build()

    val expectedStruct = RowBuilder(DummySchema)
      .withLongValue("l", 1L)
      .withBooleanValue("b", false)
      .build()
    assert(row.getStructByName("col") === expectedStruct)

    assertNull(row.getStructByName("null_col"))
    assert(row.getStructByNameOrNone("null_col").isEmpty)

    intercept[IllegalArgumentException] {
      row.getStructByName("no_such_col")
    }
    assert(row.getStructByNameOrNone("no_such_col").isEmpty)
  }
}

private[this] object PimpMySparkRowTest {

  private val DummySchema: StructType = ScalaReflection.schemaFor[Dummy].dataType.asInstanceOf[StructType]

  private final case class Dummy(l: Long, b: Boolean)
}
