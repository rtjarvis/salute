package uk.org.richardjarvis.writer;

import org.apache.spark.sql.DataFrame;
import uk.org.richardjarvis.metadata.MetaData;

/**
 * Created by rjarvis on 26/02/16.
 */
public interface WriterInterface {

    void write(DataFrame output, MetaData metadata, String outputPath);
}
