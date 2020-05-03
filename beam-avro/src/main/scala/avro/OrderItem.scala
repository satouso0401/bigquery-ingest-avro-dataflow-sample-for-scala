/** MACHINE-GENERATED FROM AVRO SCHEMA. DO NOT EDIT DIRECTLY */
package avro

import scala.annotation.switch

final case class OrderItem(var id: Long, var name: String, var price: Float) extends org.apache.avro.specific.SpecificRecordBase {
  def this() = this(0L, "", 0.0F)
  def get(field$: Int): AnyRef = {
    (field$: @switch) match {
      case 0 => {
        id
      }.asInstanceOf[AnyRef]
      case 1 => {
        name
      }.asInstanceOf[AnyRef]
      case 2 => {
        price
      }.asInstanceOf[AnyRef]
      case _ => new org.apache.avro.AvroRuntimeException("Bad index")
    }
  }
  def put(field$: Int, value: Any): Unit = {
    (field$: @switch) match {
      case 0 => this.id = {
        value
      }.asInstanceOf[Long]
      case 1 => this.name = {
        value.toString
      }.asInstanceOf[String]
      case 2 => this.price = {
        value
      }.asInstanceOf[Float]
      case _ => new org.apache.avro.AvroRuntimeException("Bad index")
    }
    ()
  }
  def getSchema: org.apache.avro.Schema = OrderItem.SCHEMA$
}

object OrderItem {
  val SCHEMA$ = new org.apache.avro.Schema.Parser().parse("{\"type\":\"record\",\"name\":\"OrderItem\",\"namespace\":\"com.google.cloud.solutions.beamavro.beans\",\"fields\":[{\"name\":\"id\",\"type\":\"long\"},{\"name\":\"name\",\"type\":\"string\"},{\"name\":\"price\",\"type\":\"float\"}]}")
}