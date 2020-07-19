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

import java.time.{LocalDate, LocalDateTime}
import java.{lang => jl, math => jm, sql => js}

import com.chrism.commons.util.StringUtils
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{
  ArrayType,
  BooleanType,
  ByteType,
  DataType,
  DateType,
  DecimalType,
  DoubleType,
  FloatType,
  IntegerType,
  LongType,
  MapType,
  ShortType,
  StringType,
  StructField,
  StructType,
  TimestampType
}

trait PimpMySparkRow {

  /** The object that contains implicit methods for [[Row]], [[StructType]], and [[StructField]].
    *
    * To use the implicit methods defined in this object, simply import the entire object in your scope:
    * {{{ import spark_row_implicits._ }}}
    */
  object spark_row_implicits {

    /** @param row the [[Row]] instance with schema defined */
    implicit final class RowOps(row: Row) {

      private[this] lazy val _schema: StructType = {
        val s = row.schema
        require(s != null, "The Row instance does not have a schema associated with it!")
        s
      }

      def getStringByName(fieldName: String): String = row.getAs[String](fieldName)

      def getStringByNameOrNone(fieldName: String): Option[String] =
        if (_schema.containsFieldName(fieldName)) Option(getStringByName(fieldName))
        else None

      def getNonBlankStringByNameOrNone(fieldName: String): Option[String] =
        getStringByNameOrNone(fieldName).filter(StringUtils.isNotBlank)

      def getBooleanByName(fieldName: String): Boolean = row.getAs[Boolean](fieldName)

      def getBooleanByNameOrNull(fieldName: String): jl.Boolean =
        if (isNullAtByName(fieldName)) null
        else getBooleanByName(fieldName)

      def getBooleanByNameOrNone(fieldName: String): Option[Boolean] =
        if (!_schema.containsFieldName(fieldName) || isNullAtByName(fieldName)) None
        else Some(getBooleanByName(fieldName))

      def getByteByName(fieldName: String): Byte = row.getAs[Byte](fieldName)

      def getByteByNameOrNull(fieldName: String): jl.Byte =
        if (isNullAtByName(fieldName)) null
        else getByteByName(fieldName)

      def getByteByNameOrNone(fieldName: String): Option[Byte] =
        if (!_schema.containsFieldName(fieldName) || isNullAtByName(fieldName)) None
        else Some(getByteByName(fieldName))

      def getShortByName(fieldName: String): Short = row.getAs[Short](fieldName)

      def getShortByNameOrNull(fieldName: String): jl.Short =
        if (isNullAtByName(fieldName)) null
        else getShortByName(fieldName)

      def getShortByNameOrNone(fieldName: String): Option[Short] =
        if (!_schema.containsFieldName(fieldName) || isNullAtByName(fieldName)) None
        else Some(getShortByName(fieldName))

      def getIntByName(fieldName: String): Int = row.getAs[Int](fieldName)

      def getIntByNameOrNull(fieldName: String): jl.Integer =
        if (isNullAtByName(fieldName)) null
        else getIntByName(fieldName)

      def getIntByNameOrNone(fieldName: String): Option[Int] =
        if (!_schema.containsFieldName(fieldName) || isNullAtByName(fieldName)) None
        else Some(getIntByName(fieldName))

      def getLongByName(fieldName: String): Long = row.getAs[Long](fieldName)

      def getLongByNameOrNull(fieldName: String): jl.Long =
        if (isNullAtByName(fieldName)) null
        else getLongByName(fieldName)

      def getLongByNameOrNone(fieldName: String): Option[Long] =
        if (!_schema.containsFieldName(fieldName) || isNullAtByName(fieldName)) None
        else Some(getLongByName(fieldName))

      def getFloatByName(fieldName: String): Float = row.getAs[Float](fieldName)

      def getFloatByNameOrNull(fieldName: String): jl.Float =
        if (isNullAtByName(fieldName)) null
        else getFloatByName(fieldName)

      def getFloatByNameOrNone(fieldName: String): Option[Float] =
        if (!_schema.containsFieldName(fieldName) || isNullAtByName(fieldName)) None
        else Some(getFloatByName(fieldName))

      def getDoubleByName(fieldName: String): Double = row.getAs[Double](fieldName)

      def getDoubleByNameOrNull(fieldName: String): jl.Double =
        if (isNullAtByName(fieldName)) null
        else getDoubleByName(fieldName)

      def getDoubleByNameOrNone(fieldName: String): Option[Double] =
        if (!_schema.containsFieldName(fieldName) || isNullAtByName(fieldName)) None
        else Some(getDoubleByName(fieldName))

      def getJBigDecimalByName(fieldName: String): jm.BigDecimal = row.getAs[jm.BigDecimal](fieldName)

      def getJBigDecimalByNameOrNone(fieldName: String): Option[jm.BigDecimal] =
        if (_schema.containsFieldName(fieldName)) Option(getJBigDecimalByName(fieldName))
        else None

      def getBigDecimalByName(fieldName: String): BigDecimal = {
        val jbd = getJBigDecimalByName(fieldName)
        if (jbd == null) null else jbd
      }

      def getBigDecimalByNameOrNone(fieldName: String): Option[BigDecimal] =
        if (_schema.containsFieldName(fieldName)) Option(getBigDecimalByName(fieldName))
        else None

      def getJBigIntByName(fieldName: String): jm.BigInteger = {
        val bd = getJBigDecimalByName(fieldName)
        if (bd == null) null else bd.toBigInteger
      }

      def getJBigIntByNameOrNone(fieldName: String): Option[jm.BigInteger] =
        if (_schema.containsFieldName(fieldName)) Option(getJBigIntByName(fieldName))
        else None

      def getBigIntByName(fieldName: String): BigInt = {
        val bd = getJBigIntByName(fieldName)
        if (bd == null) null else bd
      }

      def getBigIntByNameOrNone(fieldName: String): Option[BigInt] =
        if (_schema.containsFieldName(fieldName)) Option(getBigIntByName(fieldName))
        else None

      def getSqlDateByName(fieldName: String): js.Date = row.getAs[js.Date](fieldName)

      def getSqlDateByNameOrNone(fieldName: String): Option[js.Date] =
        if (_schema.containsFieldName(fieldName)) Option(getSqlDateByName(fieldName))
        else None

      def getLocalDateByName(fieldName: String): LocalDate = {
        val d = getSqlDateByName(fieldName)
        if (d == null) null else d.toLocalDate
      }

      def getLocalDateByNameOrNone(fieldName: String): Option[LocalDate] =
        if (_schema.containsFieldName(fieldName)) Option(getLocalDateByName(fieldName))
        else None

      def getSqlTimestampByName(fieldName: String): js.Timestamp = row.getAs[js.Timestamp](fieldName)

      def getSqlTimestampByNameOrNone(fieldName: String): Option[js.Timestamp] =
        if (_schema.containsFieldName(fieldName)) Option(getSqlTimestampByName(fieldName))
        else None

      def getLocalDateTimeByName(fieldName: String): LocalDateTime = {
        val t = getSqlTimestampByName(fieldName)
        if (t == null) null else t.toLocalDateTime
      }

      def getLocalDateTimeByNameOrNone(fieldName: String): Option[LocalDateTime] =
        if (_schema.containsFieldName(fieldName)) Option(getLocalDateTimeByName(fieldName))
        else None

      def getSeqByName[T](fieldName: String): Seq[T] = row.getAs[Seq[T]](fieldName)

      def getSeqByNameOrNone[T](fieldName: String): Option[Seq[T]] =
        if (_schema.containsFieldName(fieldName)) Option(getSeqByName(fieldName))
        else None

      def getNonEmptySeqByNameOrNone[T](fieldName: String): Option[Seq[T]] =
        getSeqByNameOrNone(fieldName).filter(_.nonEmpty)

      def getMapByName[K, V](fieldName: String): Map[K, V] = row.getAs[Map[K, V]](fieldName)

      def getMapByNameOrNone[K, V](fieldName: String): Option[Map[K, V]] =
        if (_schema.containsFieldName(fieldName)) Option(getMapByName(fieldName))
        else None

      def getNonEmptyMapByNameOrNone[K, V](fieldName: String): Option[Map[K, V]] =
        getMapByNameOrNone(fieldName).filter(_.nonEmpty)

      def getStructByName(fieldName: String): Row = row.getAs[Row](fieldName)

      def getStructByNameOrNone(fieldName: String): Option[Row] =
        if (_schema.containsFieldName(fieldName)) Option(getStructByName(fieldName))
        else None

      def isNullAtByName(fieldName: String): Boolean = row.isNullAt(row.fieldIndex(fieldName))
    }

    implicit final class StructTypeOps(schema: StructType) {

      def containsFieldName(fieldName: String): Boolean = schema.exists(_.name == fieldName)
    }

    implicit final class StructFieldOps(field: StructField) {

      def isStringField: Boolean = field.dataType.isStringType

      def isBooleanField: Boolean = field.dataType.isBooleanType

      def isByteField: Boolean = field.dataType.isByteType

      def isShortField: Boolean = field.dataType.isShortType

      def isIntField: Boolean = field.dataType.isIntegerType

      def isLongField: Boolean = field.dataType.isLongType

      def isFloatField: Boolean = field.dataType.isFloatType

      def isDoubleField: Boolean = field.dataType.isDoubleType

      def isDecimalField: Boolean = field.dataType.isDecimalType

      def isDateField: Boolean = field.dataType.isDateType

      def isTimestampField: Boolean = field.dataType.isTimestampType

      def isArrayField: Boolean = field.dataType.isArrayType

      def isMapField: Boolean = field.dataType.isMapType
    }

    implicit final class DataTypeOps(dataType: DataType) {

      def isStringType: Boolean = dataType.isInstanceOf[StringType]

      def isBooleanType: Boolean = dataType.isInstanceOf[BooleanType]

      def isByteType: Boolean = dataType.isInstanceOf[ByteType]

      def isShortType: Boolean = dataType.isInstanceOf[ShortType]

      def isIntegerType: Boolean = dataType.isInstanceOf[IntegerType]

      def isLongType: Boolean = dataType.isInstanceOf[LongType]

      def isFloatType: Boolean = dataType.isInstanceOf[FloatType]

      def isDoubleType: Boolean = dataType.isInstanceOf[DoubleType]

      def isDecimalType: Boolean = dataType.isInstanceOf[DecimalType]

      def isDateType: Boolean = dataType.isInstanceOf[DateType]

      def isTimestampType: Boolean = dataType.isInstanceOf[TimestampType]

      def isArrayType: Boolean = dataType.isInstanceOf[ArrayType]

      def isMapType: Boolean = dataType.isInstanceOf[MapType]

      def asStringType: StringType =
        dataType match {
          case t: StringType => t
          case other         => throw new ClassCastException(s"The DataType ($other) cannot be cast to StringType!")
        }

      def asBooleanType: BooleanType =
        dataType match {
          case t: BooleanType => t
          case other          => throw new ClassCastException(s"The DataType ($other) cannot be cast to BooleanType!")
        }

      def asByteType: ByteType =
        dataType match {
          case t: ByteType => t
          case other       => throw new ClassCastException(s"The DataType ($other) cannot be cast to ByteType!")
        }

      def asShortType: ShortType =
        dataType match {
          case t: ShortType => t
          case other        => throw new ClassCastException(s"The DataType ($other) cannot be cast to ShortType!")
        }

      def asIntegerType: IntegerType =
        dataType match {
          case t: IntegerType => t
          case other          => throw new ClassCastException(s"The DataType ($other) cannot be cast to IntType!")
        }

      def asLongType: LongType =
        dataType match {
          case t: LongType => t
          case other       => throw new ClassCastException(s"The DataType ($other) cannot be cast to LongType!")
        }

      def asFloatType: FloatType =
        dataType match {
          case t: FloatType => t
          case other        => throw new ClassCastException(s"The DataType ($other) cannot be cast to FloatType!")
        }

      def asDoubleType: DoubleType =
        dataType match {
          case t: DoubleType => t
          case other         => throw new ClassCastException(s"The DataType ($other) cannot be cast to DoubleType!")
        }

      def asDecimalType: DecimalType =
        dataType match {
          case t: DecimalType => t
          case other          => throw new ClassCastException(s"The DataType ($other) cannot be cast to DecimalType!")
        }

      def asDateType: DateType =
        dataType match {
          case t: DateType => t
          case other       => throw new ClassCastException(s"The DataType ($other) cannot be cast to DateType!")
        }

      def asTimestampType: TimestampType =
        dataType match {
          case t: TimestampType => t
          case other            => throw new ClassCastException(s"The DataType ($other) cannot be cast to TimestampType!")
        }

      def asArrayType: ArrayType =
        dataType match {
          case t: ArrayType => t
          case other        => throw new ClassCastException(s"The DataType ($other) cannot be cast to ArrayType!")
        }

      def asMapType: MapType =
        dataType match {
          case t: MapType => t
          case other      => throw new ClassCastException(s"The DataType ($other) cannot be cast to MapType!")
        }
    }
  }
}
