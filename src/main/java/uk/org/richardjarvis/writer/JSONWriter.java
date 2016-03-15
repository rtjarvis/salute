package uk.org.richardjarvis.writer;

import org.apache.spark.sql.DataFrame;
import uk.org.richardjarvis.metadata.MetaData;

/**
 * Created by rjarvis on 26/02/16.
 */
public class JSONWriter implements WriterInterface{

    @Override
    public void write(DataFrame output, MetaData metadata, String outputPath) {
        output.write().format("json").save(outputPath);
    }
}
