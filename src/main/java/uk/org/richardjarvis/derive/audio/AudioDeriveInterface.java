package uk.org.richardjarvis.derive.audio;

import org.apache.spark.sql.DataFrame;
import uk.org.richardjarvis.metadata.AudioMetaData;
import uk.org.richardjarvis.metadata.TabularMetaData;

/**
 * Created by rjarvis on 29/02/16.
 */
public interface AudioDeriveInterface {

    DataFrame derive(DataFrame input, AudioMetaData metaData);
}
