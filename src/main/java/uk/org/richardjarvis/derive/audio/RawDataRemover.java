package uk.org.richardjarvis.derive.audio;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.DataFrame;
import uk.org.richardjarvis.metadata.AudioMetaData;
import uk.org.richardjarvis.utils.DataFrameUtils;

import java.util.List;

/**
 * Created by rjarvis on 14/03/16.
 */
public class RawDataRemover implements AudioDeriveInterface {


    @Override
    public DataFrame derive(DataFrame input, AudioMetaData metaData) {

        List<Column> audioColumns = DataFrameUtils.getAudioWaveformColumns(input);

        DataFrame output = input;

        for (Column column : audioColumns) {
            output = output.drop(column);
        }

        return output;

    }
}
