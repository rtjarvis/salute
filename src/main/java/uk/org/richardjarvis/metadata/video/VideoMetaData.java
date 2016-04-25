package uk.org.richardjarvis.metadata.video;

import org.apache.spark.sql.DataFrame;
import uk.org.richardjarvis.metadata.MetaData;

/**
 * Created by rjarvis on 26/02/16.
 */
public class VideoMetaData implements MetaData {
    @Override
    public void generateReport(String inputPath, DataFrame inputData, DataFrame derivedData, String outputPath) {

    }
}
