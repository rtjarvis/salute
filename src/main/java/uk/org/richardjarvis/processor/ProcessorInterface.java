package uk.org.richardjarvis.processor;

import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import uk.org.richardjarvis.metadata.MetaData;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * Created by rjarvis on 24/02/16.
 */
public interface ProcessorInterface {

    MetaData extractMetaData(String path) throws IOException;

    DataFrame extractData(String path, MetaData metaData, SQLContext sqlContext) throws IOException;

}
