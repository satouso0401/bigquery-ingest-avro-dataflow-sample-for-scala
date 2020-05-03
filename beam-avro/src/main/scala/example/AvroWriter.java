package example;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.POutput;
import org.joda.time.Duration;

public class AvroWriter<T extends GenericRecord> extends PTransform<PCollection<GenericRecord>, POutput> {
  private ValueProvider<String> outputPath;
  private Schema schema;

  public AvroWriter<T> withOutputPath(ValueProvider<String> outputPath) {
    this.outputPath = outputPath;
    return this;
  }

  public AvroWriter<T> withRecordSchema(Schema schema) {
    this.schema = schema;
    return this;
  }

  @Override
  public POutput expand(PCollection<GenericRecord> inputRecords) {
    // Write to GCS
    return inputRecords
        .apply("Window for 10 seconds", Window.into(FixedWindows.of(Duration.standardSeconds(10))))
        .apply(
            "Write Avro file",
            AvroIO.writeGenericRecords(schema).to(outputPath).withWindowedWrites().withNumShards(5)
        );
  }
}
