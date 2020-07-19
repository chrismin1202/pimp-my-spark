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
import java.{lang => jl, math => jm, sql => js}

import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types.{ArrayType, DataType, DataTypes, DecimalType, MapType, StructField, StructType}
import org.apache.spark.sql.{Encoders, Row}

import scala.collection.mutable
import scala.reflect.runtime.universe.TypeTag

/** A helper class for building a [[Row]] instance with schema.
  *
  * @param schema the schema for the row
  */
final class RowBuilder private (val schema: StructType) {

  import RowBuilder.{isArrayType, isMapType, matchName, matchType}

  private[this] val _values: mutable.Map[String, Any] = mutable.Map.empty

  def build(): Row = {
    val fields = new Array[Any](schema.size)
    val valueIterator = _values.iterator
    while (valueIterator.hasNext) {
      val value = valueIterator.next()
      fields(schema.fieldIndex(value._1)) = value._2
    }
    new GenericRowWithSchema(fields, schema)
  }

  def withValue(fieldName: String, value: Any): this.type = {
    requireFieldExists(fieldName)
    // TODO: match types
    withRawValue(fieldName, value)
  }

  def withBooleanValue(fieldName: String, value: Boolean): this.type = {
    requireFieldAndType(fieldName, DataTypes.BooleanType)
    withRawValue(fieldName, value)
  }

  def withNullBooleanValue(fieldName: String): this.type = withJBooleanValue(fieldName, null)

  def withJBooleanValue(fieldName: String, value: jl.Boolean): this.type = {
    requireFieldAndType(fieldName, DataTypes.BooleanType)
    withRawValue(fieldName, value)
  }

  def withByteValue(fieldName: String, value: Byte): this.type = {
    requireFieldAndType(fieldName, DataTypes.ByteType)
    withRawValue(fieldName, value)
  }

  def withNullByteValue(fieldName: String): this.type = withJByteValue(fieldName, null)

  def withJByteValue(fieldName: String, value: jl.Byte): this.type = {
    requireFieldAndType(fieldName, DataTypes.ByteType)
    withRawValue(fieldName, value)
  }

  def withShortValue(fieldName: String, value: Short): this.type = {
    requireFieldAndType(fieldName, DataTypes.ShortType)
    withRawValue(fieldName, value)
  }

  def withNullShortValue(fieldName: String): this.type = withJShortValue(fieldName, null)

  def withJShortValue(fieldName: String, value: jl.Short): this.type = {
    requireFieldAndType(fieldName, DataTypes.ShortType)
    withRawValue(fieldName, value)
  }

  def withIntValue(fieldName: String, value: Int): this.type = {
    requireFieldAndType(fieldName, DataTypes.IntegerType)
    withRawValue(fieldName, value)
  }

  def withNullIntValue(fieldName: String): this.type = withJIntValue(fieldName, null)

  def withJIntValue(fieldName: String, value: jl.Integer): this.type = {
    requireFieldAndType(fieldName, DataTypes.IntegerType)
    withRawValue(fieldName, value)
  }

  def withLongValue(fieldName: String, value: Long): this.type = {
    requireFieldAndType(fieldName, DataTypes.LongType)
    withRawValue(fieldName, value)
  }

  def withNullLongValue(fieldName: String): this.type = withJLongValue(fieldName, null)

  def withJLongValue(fieldName: String, value: jl.Long): this.type = {
    requireFieldAndType(fieldName, DataTypes.LongType)
    withRawValue(fieldName, value)
  }

  def withFloatValue(fieldName: String, value: Float): this.type = {
    requireFieldAndType(fieldName, DataTypes.FloatType)
    withRawValue(fieldName, value)
  }

  def withNullFloatValue(fieldName: String): this.type = withJFloatValue(fieldName, null)

  def withJFloatValue(fieldName: String, value: jl.Float): this.type = {
    requireFieldAndType(fieldName, DataTypes.FloatType)
    withRawValue(fieldName, value)
  }

  def withDoubleValue(fieldName: String, value: Double): this.type = {
    requireFieldAndType(fieldName, DataTypes.DoubleType)
    withRawValue(fieldName, value)
  }

  def withNullDoubleValue(fieldName: String): this.type = withJDoubleValue(fieldName, null)

  def withJDoubleValue(fieldName: String, value: jl.Double): this.type = {
    requireFieldAndType(fieldName, DataTypes.DoubleType)
    withRawValue(fieldName, value)
  }

  def withBigDecimalValue(fieldName: String, value: BigDecimal): this.type =
    withJBigDecimalValue(fieldName, if (value == null) null else value.bigDecimal)

  def withBigIntValue(fieldName: String, value: BigInt): this.type =
    withJBigIntValue(fieldName, if (value == null) null else value.bigInteger)

  def withJBigIntValue(fieldName: String, value: jm.BigInteger): this.type =
    withJBigDecimalValue(fieldName, if (value == null) null else new jm.BigDecimal(value))

  def withJBigDecimalValue(fieldName: String, value: jm.BigDecimal): this.type = {
    val fieldIterator = schema.iterator
    var nameMatch = false
    var found = false
    while (!found && fieldIterator.hasNext) {
      val field = fieldIterator.next()
      if (matchName(field, fieldName)) {
        nameMatch = true
        field.dataType match {
          case dt: DecimalType =>
            if (value != null)
              require(
                (value.precision() <= dt.precision) && (value.scale() <= dt.scale),
                "The precision (" + value.precision() + ") and/or " +
                  "scale (" + value.scale() + ") of the decimal value is greater than " +
                  "those of the DecimalType (precision=" + dt.precision + ", scale=" + dt.scale + ")!"
              )
          case other => throw new IllegalArgumentException(s"The data type ($other) is not DecimalType!")
        }
        found = true
      }
    }
    require(nameMatch, s"There is no field named $fieldName in the schema!")
    withRawValue(fieldName, value)
  }

  def withSqlDateValue(fieldName: String, value: js.Date): this.type = {
    requireFieldAndType(fieldName, DataTypes.DateType)
    withRawValue(fieldName, value)
  }

  def withLocalDateValue(fieldName: String, value: LocalDate): this.type =
    withSqlDateValue(fieldName, if (value == null) null else js.Date.valueOf(value))

  def withDateValue(fieldName: String, year: Int, month: Int, dayOfMonth: Int): this.type =
    withLocalDateValue(fieldName, LocalDate.of(year, month, dayOfMonth))

  def withDateValue(fieldName: String, year: Int, month: Month, dayOfMonth: Int): this.type =
    withLocalDateValue(fieldName, LocalDate.of(year, month, dayOfMonth))

  def withSqlTimestampValue(fieldName: String, value: js.Timestamp): this.type = {
    requireFieldAndType(fieldName, DataTypes.TimestampType)
    withRawValue(fieldName, value)
  }

  def withLocalDateTimeValue(fieldName: String, value: LocalDateTime): this.type =
    withSqlTimestampValue(fieldName, if (value == null) null else js.Timestamp.valueOf(value))

  def withDateTimeValue(fieldName: String, year: Int, month: Int, dayOfMonth: Int, hour: Int, minute: Int): this.type =
    withLocalDateTimeValue(fieldName, LocalDateTime.of(year, month, dayOfMonth, hour, minute))

  def withDateTimeValue(
    fieldName: String,
    year: Int,
    month: Month,
    dayOfMonth: Int,
    hour: Int,
    minute: Int
  ): this.type =
    withLocalDateTimeValue(fieldName, LocalDateTime.of(year, month, dayOfMonth, hour, minute))

  def withDateTimeValue(
    fieldName: String,
    year: Int,
    month: Int,
    dayOfMonth: Int,
    hour: Int,
    minute: Int,
    second: Int
  ): this.type =
    withLocalDateTimeValue(fieldName, LocalDateTime.of(year, month, dayOfMonth, hour, minute, second))

  def withDateTimeValue(
    fieldName: String,
    year: Int,
    month: Month,
    dayOfMonth: Int,
    hour: Int,
    minute: Int,
    second: Int
  ): this.type =
    withLocalDateTimeValue(fieldName, LocalDateTime.of(year, month, dayOfMonth, hour, minute, second))

  def withDateTimeValue(
    fieldName: String,
    year: Int,
    month: Int,
    dayOfMonth: Int,
    hour: Int,
    minute: Int,
    second: Int,
    nanoOfSecond: Int
  ): this.type =
    withLocalDateTimeValue(fieldName, LocalDateTime.of(year, month, dayOfMonth, hour, minute, second, nanoOfSecond))

  def withDateTimeValue(
    fieldName: String,
    year: Int,
    month: Month,
    dayOfMonth: Int,
    hour: Int,
    minute: Int,
    second: Int,
    nanoOfSecond: Int
  ): this.type =
    withLocalDateTimeValue(fieldName, LocalDateTime.of(year, month, dayOfMonth, hour, minute, second, nanoOfSecond))

  def withStringValue(fieldName: String, value: String): this.type = {
    requireFieldAndType(fieldName, DataTypes.StringType)
    withRawValue(fieldName, value)
  }

  def withSeqValue[T](fieldName: String, value: Seq[T]): this.type = {
    require(
      schema.exists(f => matchName(f, fieldName) && isArrayType(f.dataType)),
      s"There is no field named $fieldName in the schema or the data type of the field is not ArrayType!"
    )
    withRawValue(fieldName, value)
  }

  def withMapValue[K, V](fieldName: String, value: Map[K, V]): this.type = {
    require(
      schema.exists(f => matchName(f, fieldName) && isMapType(f.dataType)),
      s"There is no field named $fieldName in the schema or the data type of the field is not MapType!"
    )
    withRawValue(fieldName, value)
  }

  def withStructValue[T <: Product: TypeTag](fieldName: String, value: T): this.type =
    if (value == null) withRawValue(fieldName, null.asInstanceOf[Row])
    else {
      val fieldIterator = schema.iterator
      var nameMatch = false
      var found = false
      lazy val valueSchema: StructType = Encoders.product[T].schema
      while (!found && fieldIterator.hasNext) {
        val field = fieldIterator.next()
        if (matchName(field, fieldName)) {
          nameMatch = true
          field.dataType match {
            case st: StructType =>
              require(
                st == valueSchema,
                s"The actual schema of the object ($valueSchema) does not match the expected schema ($st)")
            case other => throw new IllegalArgumentException(s"The data type ($other) is not StructType!")
          }
          found = true
        }
      }
      require(nameMatch, s"There is no field named $fieldName in the schema!")
      require(found, "The schema of the field is missing!")

      withRawValue(fieldName, new GenericRowWithSchema(value.productIterator.toArray, valueSchema))
    }

  private[this] def withRawValue(fieldName: String, value: Any): this.type = {
    _values += (fieldName -> value)
    this
  }

  private[this] def requireFieldExists(fieldName: String): Unit =
    require(schema.exists(matchName(_, fieldName)), s"There is no field named $fieldName in the schema!")

  private[this] def requireFieldAndType(fieldName: String, dataType: DataType): Unit =
    require(
      schema.exists(f => matchName(f, fieldName) && matchType(f, dataType)),
      s"There is no field named $fieldName in the schema or the data type of the field is not $dataType!"
    )
}

object RowBuilder {

  def apply(schema: StructType): RowBuilder = new RowBuilder(schema)

  private def matchName(field: StructField, fieldName: String): Boolean = field.name == fieldName

  private def matchType(field: StructField, dataType: DataType): Boolean = field.dataType == dataType

  private def isArrayType(dataType: DataType): Boolean = dataType.isInstanceOf[ArrayType]

  private def isMapType(dataType: DataType): Boolean = dataType.isInstanceOf[MapType]
}
