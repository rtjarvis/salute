package uk.org.richardjarvis.writer;

import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import uk.org.richardjarvis.metadata.MetaData;

/**
 * Created by rjarvis on 26/02/16.
 */
public class CSVWriter implements WriterInterface {

    @Override
    public void write(DataFrame output, MetaData metadata, String outputPath) {

        output.write()
                .format("com.databricks.spark.csv")
                .option("header", "true")
                .save(outputPath);

    }
}
