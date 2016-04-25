package uk.org.richardjarvis.derive.audio;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import uk.org.richardjarvis.metadata.audio.AudioMetaData;
import uk.org.richardjarvis.metadata.text.FieldMeaning;
import uk.org.richardjarvis.utils.DataFrameUtils;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by rjarvis on 14/03/16.
 */
public class VolumeDeriver implements AudioDeriveInterface, Serializable {

    @Override
    public DataFrame derive(DataFrame input, AudioMetaData metaData) {

        List<String> audioColumnNames = DataFrameUtils.getColumnsNames(DataFrameUtils.getColumnsOfMeaning(input, FieldMeaning.MeaningType.AUDIO_WAVEFORM));

        if (audioColumnNames.size()==0)
            return input;

        StructType newSchema = getUpdatedSchema(input,metaData);
        int originalFieldCount = input.schema().fieldNames().length;
        int newFieldCount = originalFieldCount + audioColumnNames.size();

        JavaRDD<Row> rows = input.javaRDD().map(row -> {

                Object[] outputRow = new Object[newFieldCount];

                int fieldIndex = 0;
                while (fieldIndex < originalFieldCount) {
                    outputRow[fieldIndex] = row.get(fieldIndex);
                    fieldIndex++;
                }

                for (String column : audioColumnNames) {

                    List<Double> data = row.getList(row.fieldIndex(column));

                    outputRow[fieldIndex++] = getRMS(data);
                }
                return RowFactory.create(outputRow);

            }

        );

        return input.sqlContext().

                createDataFrame(rows, newSchema);

    }

    private List<StructField> getVolumeSchema(int channelNumber) {

        List<StructField> fields = new ArrayList<>();

        fields.add(new StructField("Channel_" + channelNumber + "_Volume", DataTypes.DoubleType, false, Metadata.empty()));

        return fields;
    }

    private double getRMS(List<Double> buffer) {

        double sumSquares = 0l;
        for (Double v : buffer) {
            sumSquares+=v*v;
        }
        return Math.sqrt(sumSquares/buffer.size());
    }

    private StructType getUpdatedSchema(DataFrame input, AudioMetaData metaData) {
        List<StructField> newColumns = new ArrayList<>();

        newColumns.addAll(Arrays.asList(input.schema().fields()));

        List<Column> audioColumns = DataFrameUtils.getColumnsOfMeaning(input, FieldMeaning.MeaningType.AUDIO_WAVEFORM);

        for (Column column : audioColumns) {
            int channelNumber = (int) column.named().metadata().getLong(DataFrameUtils.CHANNEL_METADATA_KEY);
            newColumns.addAll(getVolumeSchema(channelNumber));
        }

        List<String> audioColumnNames = DataFrameUtils.getColumnsNames(audioColumns);

        return new StructType(newColumns.toArray(new StructField[0]));

    }

}
