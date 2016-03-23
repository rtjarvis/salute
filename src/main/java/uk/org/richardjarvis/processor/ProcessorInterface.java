package uk.org.richardjarvis.processor;

import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import uk.org.richardjarvis.metadata.MetaData;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * The processsor for handling different file types
 */
public interface ProcessorInterface {

    /**
     *
     * @param path the input path for the file
     * @return metadata detailing properties of the file
     * @throws IOException
     */
    MetaData extractMetaData(String path) throws IOException;

    /**
     *
     * @param path the input path for the file
     * @param metaData metadata detailing properties of the file
     * @param sqlContext SparkSQLContext used to create returned DataFrame
     * @return DataFrame containing the data for the file in @path
     * @throws IOException
     */
    DataFrame extractData(String path, MetaData metaData, SQLContext sqlContext) throws IOException;

}
