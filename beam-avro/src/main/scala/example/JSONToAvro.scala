package example

import java.time.{Instant, LocalDate}

import avro.{OrderDetails, OrderItem}
import org.apache.avro.generic.GenericRecord
import org.apache.beam.sdk.transforms.DoFn
import org.apache.beam.sdk.transforms.DoFn.ProcessElement

class JSONToAvro extends DoFn[String, GenericRecord] {
  @ProcessElement def processElement(
      context: DoFn[String, GenericRecord]#ProcessContext): Unit = {
    val line = context.element

    import spray.json._
    object MyJsonProtocol extends DefaultJsonProtocol {

      implicit object LocalDateFormat extends JsonFormat[LocalDate] {
        def write(localDate: LocalDate): JsValue =
          JsNumber(localDate.toEpochDay)
        def read(value: JsValue): LocalDate = value match {
          case JsNumber(epochDay) =>
            java.time.LocalDate.ofEpochDay(epochDay.toInt)
          case _ => deserializationError("LocalDate expected.")
        }
      }

      implicit object InstantFormat extends JsonFormat[Instant] {
        def write(instant: Instant): JsValue = JsNumber(instant.toEpochMilli)
        def read(value: JsValue): Instant = value match {
          case JsNumber(epochMilli) =>
            java.time.Instant.ofEpochMilli(epochMilli.longValue())
          case _ => deserializationError("Instant expected.")
        }
      }

      implicit val colorFormat2 = jsonFormat3(OrderItem.apply)
      implicit val colorFormat  = jsonFormat4(OrderDetails.apply)
    }

    import MyJsonProtocol._
    val order = line.parseJson.convertTo[OrderDetails]

    context.output(order)
  }
}
