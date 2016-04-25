package uk.org.richardjarvis.derive.audio;

import org.apache.spark.sql.DataFrame;
import uk.org.richardjarvis.metadata.audio.AudioMetaData;

/**
 * Created by rjarvis on 29/02/16.
 */
public interface AudioDeriveInterface {

    DataFrame derive(DataFrame input, AudioMetaData metaData);
}
