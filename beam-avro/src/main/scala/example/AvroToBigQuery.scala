package example

import avro.OrderDetails
import org.apache.avro.generic.GenericRecord
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions
import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.coders.AvroCoder
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition.WRITE_APPEND
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO
import org.apache.beam.sdk.options._
import org.apache.beam.sdk.transforms.ParDo
import org.apache.beam.sdk.values.PCollection

object AvroToBigQuery {

  def main(args: Array[String]): Unit = {
    val options = PipelineOptionsFactory
      .fromArgs(args: _*)
      .withValidation
      .as(classOf[AvroToBigQuery.OrderAvroOptions])
    options.setStreaming(true)

    val pipeline = Pipeline.create(options)
    val tableStr =
      s"${options.getProject}:${options.getDataset.get}.${options.getBqTable.get}"
    val tableSchema = BigQueryAvroUtils.getTableSchema(OrderDetails.SCHEMA$)

    // Read JSON objects from PubSub
    val ods: PCollection[GenericRecord] =
      getInputCollection(pipeline,
                         options.getInputPath.get,
                         FORMAT.valueOf(options.getFormat.get()))

    // Write to GCS
    ods.apply("Write to GCS",
              new AvroWriter[GenericRecord]()
                .withOutputPath(options.getOutputPath)
                .withRecordSchema(OrderDetails.SCHEMA$))

    // Write to BigQuery
    ods.apply(
      "Write to BigQuery",
      BigQueryIO
        .write[GenericRecord]
        .to(tableStr)
        .withSchema(tableSchema)
        .withWriteDisposition(WRITE_APPEND)
        .withCreateDisposition(CREATE_IF_NEEDED)
        .withFormatFunction(TableRowParser.TABLE_ROW_PARSER)
    )

    pipeline.run.waitUntilFinish
  }

  private def getInputCollection(
      pipeline: Pipeline,
      inputPath: String,
      format: AvroToBigQuery.FORMAT): PCollection[GenericRecord] =
    format match {
      case FORMAT.JSON =>
        pipeline
          .apply("Read JSON from PubSub",
                 PubsubIO.readStrings.fromTopic(inputPath))
          .apply("To binary", ParDo.of(new JSONToAvro))
          .setCoder(AvroCoder.of(classOf[GenericRecord], OrderDetails.SCHEMA$))
      case FORMAT.AVRO =>
        pipeline.apply(
          "Read Avro from PubSub",
          PubsubIO
            .readAvroGenericRecords(OrderDetails.SCHEMA$)
            .fromTopic(inputPath)
        )
    }

  sealed trait FORMAT
  object FORMAT {
    case object JSON extends FORMAT
    case object AVRO extends FORMAT
    def valueOf(str: String): FORMAT = str match {
      case "JSON" => JSON
      case "AVRO" => AVRO
    }
  }

  trait OrderAvroOptions extends PipelineOptions with DataflowPipelineOptions {
    @Description("Input path")
    @Validation.Required
    def getInputPath: ValueProvider[String]

    def setInputPath(path: ValueProvider[String]): Unit

    @Description("Output path")
    @Validation.Required
    def getOutputPath: ValueProvider[String]

    def setOutputPath(path: ValueProvider[String]): Unit

    @Description("BigQuery Dataset")
    @Validation.Required
    def getDataset: ValueProvider[String]

    def setDataset(dataset: ValueProvider[String]): Unit

    @Description("BigQuery Table")
    @Validation.Required
    def getBqTable: ValueProvider[String]

    def setBqTable(bqTable: ValueProvider[String]): Unit

    @Description("Input Format")
    @Validation.Required
    def getFormat: ValueProvider[String]

    def setFormat(format: ValueProvider[String]): Unit
  }
}
