package example

import java.nio.ByteBuffer
import java.time.LocalTime
import java.time.format.DateTimeFormatter
import java.util

import com.google.api.services.bigquery.model.{
  TableRow,
  TableSchema,
  TableFieldSchema => BigQueryFieldSchema
}
import org.apache.avro.Schema.{Field => AvroField, Type => AvroType}
import org.apache.avro.generic.GenericRecord
import org.apache.avro.{
  Conversions,
  LogicalType => AvroLogicalType,
  LogicalTypes => AvroLogicalTypes,
  Schema => AvroSchema
}
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Verify.verify
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.io.BaseEncoding
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat

import scala.collection.JavaConverters._

object BigQueryAvroUtils {

  def getTableSchema(schema: AvroSchema): TableSchema = {
    val fields = getTableFieldSchema(schema)
    (new TableSchema).setFields(fields.asJava)
  }

  sealed trait BigQueryType { def value: String }
  private object BigQueryType {
    case object STRING    extends BigQueryType { val value = "STRING"    }
    case object GEOGRAPHY extends BigQueryType { val value = "GEOGRAPHY" }
    case object BYTES     extends BigQueryType { val value = "BYTES"     }
    case object INTEGER   extends BigQueryType { val value = "INTEGER"   }
    case object FLOAT     extends BigQueryType { val value = "FLOAT"     }
    case object FLOAT64   extends BigQueryType { val value = "FLOAT64"   }
    case object NUMERIC   extends BigQueryType { val value = "NUMERIC"   }
    case object BOOLEAN   extends BigQueryType { val value = "BOOLEAN"   }
    case object INT64     extends BigQueryType { val value = "INT64"     }
    case object LONG      extends BigQueryType { val value = "LONG"      }
    case object TIMESTAMP extends BigQueryType { val value = "TIMESTAMP" }
    case object RECORD    extends BigQueryType { val value = "RECORD"    }
    case object DATE      extends BigQueryType { val value = "DATE"      }
    case object DATETIME  extends BigQueryType { val value = "DATETIME"  }
    case object TIME      extends BigQueryType { val value = "TIME"      }
    case object STRUCT    extends BigQueryType { val value = "STRUCT"    }
    case object ARRAY     extends BigQueryType { val value = "ARRAY"     }

    def valueOf(str: String): BigQueryType = str match {
      case "STRING"    => STRING
      case "GEOGRAPHY" => GEOGRAPHY
      case "BYTES"     => BYTES
      case "INTEGER"   => INTEGER
      case "FLOAT"     => FLOAT
      case "FLOAT64"   => FLOAT64
      case "NUMERIC"   => NUMERIC
      case "BOOLEAN"   => BOOLEAN
      case "INT64"     => INT64
      case "LONG"      => LONG
      case "TIMESTAMP" => TIMESTAMP
      case "RECORD"    => RECORD
      case "DATE"      => DATE
      case "DATETIME"  => DATETIME
      case "TIME"      => TIME
      case "STRUCT"    => STRUCT
      case "ARRAY"     => ARRAY
    }
  }

  private def getTableFieldSchema(
      schema: AvroSchema): Seq[BigQueryFieldSchema] = {
    if (schema == null) {
      Nil
    } else {
      for (field <- schema.getFields.asScala) yield {
        getBigQueryType(field) match {
          case (BigQueryType.ARRAY, _) =>
            val childSchema = field.schema.getElementType
            if (childSchema.getType == AvroType.RECORD) {
              val child = getTableFieldSchema(field.schema.getElementType)
              new BigQueryFieldSchema()
                .setName(field.name)
                .setType("STRUCT")
                .setFields(child.asJava)
                .setMode("REPEATED")
            } else {
              new BigQueryFieldSchema()
                .setName(field.name)
                .setType(getBigQueryType(childSchema.getFields.get(0))._1.value)
                .setMode("REPEATED")
            }
          case (BigQueryType.RECORD, _) =>
            new BigQueryFieldSchema()
              .setName(field.name)
              .setType("STRUCT")
              .setFields(getTableFieldSchema(field.schema).asJava)
          case (elmType, mode) =>
            new BigQueryFieldSchema()
              .setName(field.name)
              .setType(elmType.value)
              .setMode(mode)
        }
      }
    }
  }

  def getAvroType(bqType: BigQueryType): AvroType = {
    bqType match {
      case BigQueryType.STRING    => AvroType.STRING
      case BigQueryType.GEOGRAPHY => AvroType.STRING
      case BigQueryType.BYTES     => AvroType.BYTES
      case BigQueryType.INTEGER   => AvroType.INT
      case BigQueryType.FLOAT     => AvroType.FLOAT
      case BigQueryType.FLOAT64   => AvroType.DOUBLE
      case BigQueryType.NUMERIC   => AvroType.BYTES
      case BigQueryType.BOOLEAN   => AvroType.BOOLEAN
      case BigQueryType.INT64     => AvroType.LONG
      case BigQueryType.TIMESTAMP => AvroType.LONG
      case BigQueryType.RECORD    => AvroType.RECORD
      case BigQueryType.DATE      => AvroType.INT
      case BigQueryType.DATETIME  => AvroType.STRING
      case BigQueryType.TIME      => AvroType.LONG
      case BigQueryType.STRUCT    => AvroType.RECORD
      case BigQueryType.ARRAY     => AvroType.ARRAY
      case _ =>
        throw new UnsupportedOperationException(
          s"Unsupported BigQuery type: $bqType")
    }
  }

  type BigQueryMode = String
  def getBigQueryType(avroField: AvroField): (BigQueryType, BigQueryMode) = {
    val avroType = avroField.schema.getType
    val logicalTypeMaybe =
      Option(avroField.schema.getLogicalType).map(_.getName)
    if (avroType == AvroType.UNION)
      avroField.schema.getTypes.asScala match {
        case Seq(typeNull, typeAny) if typeNull.getType == AvroType.NULL =>
          (getBigQueryType(typeAny.getType, logicalTypeMaybe), "NULLABLE")
        case Seq(typeAny, typeNull) if typeNull.getType == AvroType.NULL =>
          (getBigQueryType(typeAny.getType, logicalTypeMaybe), "NULLABLE")
      } else {
      (getBigQueryType(avroType, logicalTypeMaybe), "REQUIRED")
    }
  }

  def getBigQueryType(avroType: AvroType,
                      logicalTypeMaybe: Option[String]): BigQueryType = {
    avroType match {
      case AvroType.STRING =>
        BigQueryType.STRING
      case AvroType.BYTES => BigQueryType.BYTES
      case AvroType.INT =>
        logicalTypeMaybe match {
          case Some(logicalType) if logicalType == "date" => BigQueryType.DATE
          case _                                          => BigQueryType.INTEGER
        }
      case AvroType.FLOAT   => BigQueryType.FLOAT
      case AvroType.DOUBLE  => BigQueryType.FLOAT64
      case AvroType.BOOLEAN => BigQueryType.BOOLEAN
      case AvroType.LONG =>
        logicalTypeMaybe match {
          case Some(logicalType) if logicalType == "timestamp-millis" =>
            BigQueryType.TIMESTAMP
          case _ => BigQueryType.INT64
        }
      case AvroType.RECORD => BigQueryType.RECORD
      case AvroType.ARRAY  => BigQueryType.ARRAY
      case _ =>
        throw new UnsupportedOperationException(
          s"Unsupported Avro type: $avroType, logicalType: $logicalTypeMaybe")
    }
  }

  class BigQueryAvroMapper private () {}

  def convertSpecificRecordToTableRow(record: GenericRecord,
                                      schema: TableSchema): TableRow =
    convertSpecificRecordToTableRow(record, schema.getFields.asScala)

  private def convertSpecificRecordToTableRow(
      record: GenericRecord,
      fields: Seq[BigQueryFieldSchema]) = {
    val row = new TableRow
    for {
      subSchema <- fields
      field = record.getSchema.getField(subSchema.getName)
      if field != null && field.name != null
      convertedValue = getTypedCellValue(field.schema,
                                         subSchema,
                                         record.get(field.pos))
      if convertedValue != null

    } row.set(field.name, convertedValue)
    row
  }

  private def getTypedCellValue(schema: AvroSchema,
                                fieldSchema: BigQueryFieldSchema,
                                value: Any) = {

    Option(fieldSchema.getMode).getOrElse("NULLABLE") match {
      case "REQUIRED" =>
        convertRequiredField(schema.getType,
                             schema.getLogicalType,
                             fieldSchema,
                             value)
      case "REPEATED" =>
        convertRepeatedField(schema, fieldSchema, value)
      case "NULLABLE" =>
        convertNullableField(schema, fieldSchema, value)
      case _ =>
        throw new UnsupportedOperationException(
          "Parsing a field with BigQuery field schema mode " + fieldSchema.getMode)
    }
  }

  private def convertRequiredField(avroType: AvroType,
                                   avroLogicalType: AvroLogicalType,
                                   fieldSchema: BigQueryFieldSchema,
                                   value: Any): Any = {
    val bigQeryType      = BigQueryType.valueOf(fieldSchema.getType)
    val expectedAvroType = getAvroType(bigQeryType)
    assert(
      expectedAvroType == avroType,
      s"Expected Avro schema types $expectedAvroType for BigQuery $bigQeryType field ${fieldSchema.getName}, but received $avroType"
    )

    bigQeryType match {
      case BigQueryType.STRING | BigQueryType.DATETIME | BigQueryType.GEOGRAPHY
          if value.isInstanceOf[CharSequence] =>
        value.toString
      case BigQueryType.DATE
          if avroType == AvroType.INT
            && value.isInstanceOf[java.lang.Integer]
            && avroLogicalType != null
            && avroLogicalType.isInstanceOf[AvroLogicalTypes.Date] =>
        formatDate(value.asInstanceOf[java.lang.Integer])
      case BigQueryType.TIME
          if avroType == AvroType.LONG
            && value.isInstanceOf[java.lang.Long]
            && avroLogicalType != null
            && avroLogicalType.isInstanceOf[AvroLogicalTypes.TimeMicros] =>
        formatTime(value.asInstanceOf[java.lang.Long])
      case BigQueryType.TIME | BigQueryType.DATE
          if value.isInstanceOf[CharSequence] =>
        value.toString
      case BigQueryType.INTEGER if value.isInstanceOf[java.lang.Integer] =>
        value
      case BigQueryType.INT64 | BigQueryType.LONG
          if value.isInstanceOf[java.lang.Long] =>
        value
      case BigQueryType.FLOAT64 if value.isInstanceOf[java.lang.Double] =>
        value
      case BigQueryType.FLOAT if value.isInstanceOf[java.lang.Float] =>
        value
      case BigQueryType.NUMERIC
          if value.isInstanceOf[ByteBuffer]
            && avroLogicalType != null
            && avroLogicalType.isInstanceOf[AvroLogicalTypes.Decimal] =>
        val numericValue = new Conversions.DecimalConversion()
          .fromBytes(value.asInstanceOf[ByteBuffer],
                     AvroSchema.create(avroType),
                     avroLogicalType)
        numericValue.toString
      case BigQueryType.BOOLEAN if value.isInstanceOf[java.lang.Boolean] =>
        value
      case BigQueryType.TIMESTAMP
          if value.isInstanceOf[org.joda.time.DateTime] =>
        formatTimestamp(value.asInstanceOf[org.joda.time.DateTime])
      case BigQueryType.TIMESTAMP if value.isInstanceOf[java.lang.Long] =>
        value.asInstanceOf[java.lang.Long] / 1000
      case BigQueryType.STRUCT if value.isInstanceOf[GenericRecord] =>
        convertSpecificRecordToTableRow(value.asInstanceOf[GenericRecord],
                                        fieldSchema.getFields.asScala)
      case BigQueryType.RECORD if value.isInstanceOf[GenericRecord] =>
        convertGenericRecordToTableRow(value.asInstanceOf[GenericRecord],
                                       fieldSchema.getFields.asScala)
      case BigQueryType.BYTES if value.isInstanceOf[ByteBuffer] =>
        val byteBuffer = value.asInstanceOf[ByteBuffer]
        val bytes      = new Array[Byte](byteBuffer.limit)
        byteBuffer.get(bytes)
        BaseEncoding.base64.encode(bytes)
      case _ =>
        throw new UnsupportedOperationException(
          s"Unexpected BigQuery ${bigQeryType.value} field schema type ${fieldSchema.getType} for field named ${fieldSchema.getName}, actual value type ${value.getClass.getName}"
        )
    }
  }

  private def formatDate(epochDay: Int) = {
    DateTimeFormatter.ISO_DATE.format(java.time.LocalDate.ofEpochDay(epochDay))
  }

  private def formatTime(timeMicros: Long) = {
    val formatter = timeMicros match {
      case _ if timeMicros % 1000000 == 0 =>
        DateTimeFormatter.ofPattern("HH:mm:ss")
      case _ if timeMicros % 1000 == 0 =>
        DateTimeFormatter.ofPattern("HH:mm:ss.SSS")
      case _ =>
        DateTimeFormatter.ofPattern("HH:mm:ss.SSSSSS")
    }
    LocalTime.ofNanoOfDay(timeMicros * 1000).format(formatter)
  }

  private def formatTimestamp(dt: DateTime) = {
    val dayAndTime =
      dt.toString(DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss").withZoneUTC)
    s"$dayAndTime UTC"
  }

  private def convertGenericRecordToTableRow(
      record: GenericRecord,
      fields: Seq[BigQueryFieldSchema]) = {
    val row = new TableRow
    for (subSchema <- fields) {
      val field = record.getSchema.getField(subSchema.getName)
      val convertedValue =
        getTypedCellValue(field.schema, subSchema, record.get(field.name))
      row.set(field.name, convertedValue)
    }
    row
  }

  private def convertRepeatedField(schema: AvroSchema,
                                   fieldSchema: BigQueryFieldSchema,
                                   value: Any): util.List[Any] = {
    val arrayType = schema.getType
    verify(
      arrayType eq AvroType.ARRAY,
      s"BigQuery REPEATED field ${fieldSchema.getName} should be Avro ARRAY, not $arrayType")

    if (value == null) {
      new util.ArrayList[Any]
    } else {
      val elements           = value.asInstanceOf[util.List[Any]].asScala
      val elementType        = schema.getElementType.getType
      val elementLogicalType = schema.getElementType.getLogicalType

      elements
        .map(
          element =>
            convertRequiredField(elementType,
                                 elementLogicalType,
                                 fieldSchema,
                                 element))
        .asJava
    }

  }

  private def convertNullableField(avroSchema: AvroSchema,
                                   fieldSchema: BigQueryFieldSchema,
                                   value: Any): Any = {
    verify(
      avroSchema.getType eq AvroType.UNION,
      s"Expected Avro schema type UNION, not ${avroSchema.getType}, for BigQuery NULLABLE field ${fieldSchema.getName}")
    val unionTypes: util.List[AvroSchema] = avroSchema.getTypes
    verify(
      unionTypes.size == 2,
      s"BigQuery NULLABLE field ${fieldSchema.getName} should be an Avro UNION of NULL and another type, not $unionTypes")
    if (value == null) {
      null
    } else {
      unionTypes.asScala match {
        case Seq(typeNull, typeAny) if typeNull.getType == AvroType.NULL =>
          convertRequiredField(typeAny.getType,
                               typeAny.getLogicalType,
                               fieldSchema,
                               value)
        case Seq(typeAny, typeNull) if typeNull.getType == AvroType.NULL =>
          convertRequiredField(typeAny.getType,
                               typeAny.getLogicalType,
                               fieldSchema,
                               value)
      }
    }
  }

}
