/** MACHINE-GENERATED FROM AVRO SCHEMA. DO NOT EDIT DIRECTLY */
package avro

import scala.annotation.switch

final case class OrderDetails(var id: Long, var timestamp: java.time.Instant, var dt: java.time.LocalDate, var items: Seq[OrderItem]) extends org.apache.avro.specific.SpecificRecordBase {
  def this() = this(0L, java.time.Instant.now, java.time.LocalDate.now, Seq.empty)
  def get(field$: Int): AnyRef = {
    (field$: @switch) match {
      case 0 => {
        id
      }.asInstanceOf[AnyRef]
      case 1 => {
        timestamp.toEpochMilli
      }.asInstanceOf[AnyRef]
      case 2 => {
        dt.toEpochDay.toInt
      }.asInstanceOf[AnyRef]
      case 3 => {
        scala.collection.JavaConverters.bufferAsJavaListConverter({
          items map { x =>
            x
          }
        }.toBuffer).asJava
      }.asInstanceOf[AnyRef]
      case _ => new org.apache.avro.AvroRuntimeException("Bad index")
    }
  }
  def put(field$: Int, value: Any): Unit = {
    (field$: @switch) match {
      case 0 => this.id = {
        value
      }.asInstanceOf[Long]
      case 1 => this.timestamp = {
        value match {
          case (l: Long) => {
            java.time.Instant.ofEpochMilli(l)
          }
        }
      }.asInstanceOf[java.time.Instant]
      case 2 => this.dt = {
        value match {
          case (i: Integer) => {
            java.time.LocalDate.ofEpochDay(i.toInt)
          }
        }
      }.asInstanceOf[java.time.LocalDate]
      case 3 => this.items = {
        value match {
          case (array: java.util.List[_]) => {
            Seq((scala.collection.JavaConverters.asScalaIteratorConverter(array.iterator).asScala.toSeq map { x =>
              x
            }: _*))
          }
        }
      }.asInstanceOf[Seq[OrderItem]]
      case _ => new org.apache.avro.AvroRuntimeException("Bad index")
    }
    ()
  }
  def getSchema: org.apache.avro.Schema = OrderDetails.SCHEMA$
}

object OrderDetails {
  val SCHEMA$ = new org.apache.avro.Schema.Parser().parse("{\"type\":\"record\",\"name\":\"OrderDetails\",\"namespace\":\"com.google.cloud.solutions.beamavro.beans\",\"fields\":[{\"name\":\"id\",\"type\":\"long\"},{\"name\":\"timestamp\",\"type\":{\"type\":\"long\",\"logicalType\":\"timestamp-millis\"}},{\"name\":\"dt\",\"type\":{\"type\":\"int\",\"logicalType\":\"date\"}},{\"name\":\"items\",\"type\":{\"type\":\"array\",\"items\":{\"type\":\"record\",\"name\":\"OrderItem\",\"fields\":[{\"name\":\"id\",\"type\":\"long\"},{\"name\":\"name\",\"type\":\"string\"},{\"name\":\"price\",\"type\":\"float\"}]}}}]}")
}