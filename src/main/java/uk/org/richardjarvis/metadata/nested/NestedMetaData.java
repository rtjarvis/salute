package uk.org.richardjarvis.metadata.nested;

import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import uk.org.richardjarvis.metadata.MetaData;

/**
 * Created by rjarvis on 25/04/16.
 */
public class NestedMetaData implements MetaData {
    public NestedMetaData(Row[] headRows) {

    }

    @Override
    public void generateReport(String inputPath, DataFrame inputData, DataFrame derivedData, String outputPath) {

    }
}
