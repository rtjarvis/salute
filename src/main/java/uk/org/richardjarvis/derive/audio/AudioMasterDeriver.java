package uk.org.richardjarvis.derive.audio;

import org.apache.spark.sql.DataFrame;
import uk.org.richardjarvis.metadata.audio.AudioMetaData;

/**
 * Created by rjarvis on 29/02/16.
 */
public class AudioMasterDeriver implements AudioDeriveInterface {

    @Override
    public DataFrame derive(DataFrame input, AudioMetaData metaData) {

        DataFrame output = new SpectralDensityDeriver().derive(input, metaData);
        output = new VolumeDeriver().derive(output, metaData);
        output = new RawDataRemover().derive(output, metaData);

        return output;
    }
}
