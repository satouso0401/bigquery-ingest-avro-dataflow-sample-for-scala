package example;

import com.google.api.services.bigquery.model.TableRow;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.transforms.SerializableFunction;

public class TableRowParser {
    public static final SerializableFunction<GenericRecord, TableRow> TABLE_ROW_PARSER =
            new SerializableFunction<GenericRecord, TableRow>() {
                @Override
                public TableRow apply(GenericRecord specificRecord) {
                    return BigQueryAvroUtils.convertSpecificRecordToTableRow(
                            specificRecord, BigQueryAvroUtils.getTableSchema(specificRecord.getSchema()));
                }
            };

}
