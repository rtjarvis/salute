package uk.org.richardjarvis.processor.audio;

import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import uk.org.richardjarvis.metadata.MetaData;
import uk.org.richardjarvis.processor.ProcessorInterface;

import java.io.IOException;

/**
 * Created by rjarvis on 24/02/16.
 */
public class AudioProcessor implements ProcessorInterface {


    @Override
    public MetaData extractMetaData(String path) throws IOException {
        return null;
    }

    @Override
    public DataFrame extractData(String path, MetaData metaData, SQLContext sqlContext) throws IOException {
        return null;
    }
}
