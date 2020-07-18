package com.chrism.spark.sql

import java.time.{LocalDate, LocalDateTime}
import java.{lang => jl, math => jm, sql => js}

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types.{ArrayType, DataType, DataTypes, DecimalType, MapType, StructField, StructType}

import scala.collection.mutable

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
    while (fieldIterator.hasNext) {
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

  def withSqlTimestampValue(fieldName: String, value: js.Timestamp): this.type = {
    requireFieldAndType(fieldName, DataTypes.TimestampType)
    withRawValue(fieldName, value)
  }

  def withLocalDateTimeValue(fieldName: String, value: LocalDateTime): this.type =
    withSqlTimestampValue(fieldName, if (value == null) null else js.Timestamp.valueOf(value))

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

  private def withRawValue(fieldName: String, value: Any): this.type = {
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
