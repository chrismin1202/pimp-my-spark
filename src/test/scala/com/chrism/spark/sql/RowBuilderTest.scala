package com.chrism.spark.sql

import java.time.{LocalDate, LocalDateTime}
import java.{math => jm, sql => js}

import com.chrism.commons.FunTestSuite
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}

final class RowBuilderTest extends FunTestSuite {

  test("validating field name") {
    val schema = StructType(Seq(StructField("string_col", DataTypes.StringType)))
    intercept[IllegalArgumentException] {
      RowBuilder(schema).withStringValue("not_string_col", "s")
    }
  }

  test("validating field type") {
    val schema = StructType(
      Seq(
        StructField("string_col", DataTypes.StringType),
        StructField("int_col", DataTypes.IntegerType),
      ))
    intercept[IllegalArgumentException] {
      RowBuilder(schema).withStringValue("int_col", "s")
    }
    intercept[IllegalArgumentException] {
      RowBuilder(schema).withIntValue("string_col", 1)
    }
  }

  test("validating BigDecimal: precision too high") {
    val schema = StructType(Seq(StructField("bigdecimal_col", DataTypes.createDecimalType(5, 2))))
    intercept[IllegalArgumentException] {
      RowBuilder(schema).withBigDecimalValue("bigdecimal_col", BigDecimal("1234.56"))
    }
  }

  test("validating BigDecimal: scale too high") {
    val schema = StructType(Seq(StructField("bigdecimal_col", DataTypes.createDecimalType(6, 3))))
    intercept[IllegalArgumentException] {
      RowBuilder(schema).withBigDecimalValue("bigdecimal_col", BigDecimal("12.3456"))
    }
  }

  test("building a row with null values") {
    val schema = StructType(
      Seq(
        StructField("null_string_col", DataTypes.StringType),
        StructField("null_byte_col", DataTypes.ByteType),
        StructField("null_short_col", DataTypes.ShortType),
        StructField("null_int_col", DataTypes.IntegerType),
        StructField("null_long_col", DataTypes.LongType),
        StructField("null_bigint_col", DataTypes.createDecimalType(5, 0)),
        StructField("null_bigdecimal_col", DataTypes.createDecimalType(7, 2)),
        StructField("null_date_col", DataTypes.DateType),
        StructField("null_timestamp_col", DataTypes.TimestampType),
        StructField("null_bool_col", DataTypes.BooleanType),
        StructField("null_array_col", DataTypes.createArrayType(DataTypes.StringType)),
        StructField("null_map_col", DataTypes.createMapType(DataTypes.IntegerType, DataTypes.BooleanType)),
      ))

    val row = RowBuilder(schema)
      .withStringValue("null_string_col", null)
      .withNullByteValue("null_byte_col")
      .withNullShortValue("null_short_col")
      .withNullIntValue("null_int_col")
      .withNullLongValue("null_long_col")
      .withBigIntValue("null_bigint_col", null)
      .withBigDecimalValue("null_bigdecimal_col", null)
      .withLocalDateValue("null_date_col", null)
      .withLocalDateTimeValue("null_timestamp_col", null)
      .withNullBooleanValue("null_bool_col")
      .withSeqValue("null_array_col", null)
      .withMapValue("null_map_col", null)
      .build()

    assert(row.isNullAt(0))
    assert(row.isNullAt(1))
    assert(row.isNullAt(2))
    assert(row.isNullAt(3))
    assert(row.isNullAt(4))
    assert(row.isNullAt(5))
    assert(row.isNullAt(6))
    assert(row.isNullAt(7))
    assert(row.isNullAt(8))
    assert(row.isNullAt(9))
    assert(row.isNullAt(10))
    assert(row.isNullAt(11))
  }

  test("building a row with different data types") {
    val schema = StructType(
      Seq(
        StructField("string_col", DataTypes.StringType),
        StructField("byte_col", DataTypes.ByteType),
        StructField("jbyte_col", DataTypes.ByteType),
        StructField("short_col", DataTypes.ShortType),
        StructField("jshort_col", DataTypes.ShortType),
        StructField("int_col", DataTypes.IntegerType),
        StructField("jint_col", DataTypes.IntegerType),
        StructField("long_col", DataTypes.LongType),
        StructField("jlong_col", DataTypes.LongType),
        StructField("bigint_col", DataTypes.createDecimalType(5, 0)),
        StructField("jbigint_col", DataTypes.createDecimalType(6, 0)),
        StructField("bigdecimal_col", DataTypes.createDecimalType(7, 2)),
        StructField("jbigdecimal_col", DataTypes.createDecimalType(8, 3)),
        StructField("localdate_col", DataTypes.DateType),
        StructField("sqldate_col", DataTypes.DateType),
        StructField("localdatetime_col", DataTypes.TimestampType),
        StructField("sqltimestamp_col", DataTypes.TimestampType),
        StructField("bool_col", DataTypes.BooleanType),
        StructField("jbool_col", DataTypes.BooleanType),
        StructField("array_col", DataTypes.createArrayType(DataTypes.StringType)),
        StructField("map_col", DataTypes.createMapType(DataTypes.IntegerType, DataTypes.BooleanType)),
      ))

    val row = RowBuilder(schema)
      .withStringValue("string_col", "s")
      .withByteValue("byte_col", 1)
      .withJByteValue("jbyte_col", 2.toByte)
      .withShortValue("short_col", 3)
      .withJShortValue("jshort_col", 4.toShort)
      .withIntValue("int_col", 5)
      .withJIntValue("jint_col", 6)
      .withLongValue("long_col", 7L)
      .withJLongValue("jlong_col", 8L)
      .withBigIntValue("bigint_col", BigInt("12345"))
      .withJBigIntValue("jbigint_col", new jm.BigInteger("123456"))
      .withBigDecimalValue("bigdecimal_col", BigDecimal("12345.67"))
      .withJBigDecimalValue("jbigdecimal_col", new jm.BigDecimal("12345.678"))
      .withLocalDateValue("localdate_col", LocalDate.of(2020, 1, 1))
      .withSqlDateValue("sqldate_col", js.Date.valueOf(LocalDate.of(2020, 1, 2)))
      .withLocalDateTimeValue("localdatetime_col", LocalDateTime.of(2020, 1, 3, 4, 5))
      .withSqlTimestampValue("sqltimestamp_col", js.Timestamp.valueOf(LocalDateTime.of(2020, 1, 4, 5, 6)))
      .withBooleanValue("bool_col", false)
      .withJBooleanValue("jbool_col", true)
      .withSeqValue("array_col", Seq("a", "b", "c"))
      .withMapValue("map_col", Map(1 -> true, 2 -> false, 3 -> true, 4 -> false))
      .build()

    assert(row.getString(0) === "s")
    assert(row.getByte(1) === 1.toByte)
    assertJByte(2, row.getByte(2))
    assert(row.getShort(3) === 3.toShort)
    assertJShort(4, row.getShort(4))
    assert(row.getInt(5) === 5)
    assertJInt(6, row.getInt(6))
    assert(row.getLong(7) === 7L)
    assertJLong(8L, row.getLong(8))
    assertJBigInteger(new jm.BigInteger("12345"), row.getDecimal(9).toBigIntegerExact)
    assertJBigInteger(new jm.BigInteger("123456"), row.getDecimal(10).toBigIntegerExact)
    assertJBigDecimal(new jm.BigDecimal("12345.67"), row.getDecimal(11))
    assertJBigDecimal(new jm.BigDecimal("12345.678"), row.getDecimal(12))
    assert(row.getDate(13).toLocalDate === LocalDate.of(2020, 1, 1))
    assert(row.getDate(14).toLocalDate === LocalDate.of(2020, 1, 2))
    assert(row.getTimestamp(15).toLocalDateTime === LocalDateTime.of(2020, 1, 3, 4, 5))
    assert(row.getTimestamp(16).toLocalDateTime === LocalDateTime.of(2020, 1, 4, 5, 6))
    assert(row.getBoolean(17) === false)
    assert(row.getBoolean(18))
    row.getSeq[String](19) should contain theSameElementsInOrderAs Seq("a", "b", "c")
    row.getMap[Int, Boolean](20) should contain theSameElementsAs Map(1 -> true, 2 -> false, 3 -> true, 4 -> false)
  }
}
