package uk.org.richardjarvis.derive.audio;

import org.apache.commons.math3.complex.Complex;
import org.apache.commons.math3.transform.DftNormalization;
import org.apache.commons.math3.transform.FastFourierTransformer;
import org.apache.commons.math3.transform.TransformType;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import uk.org.richardjarvis.metadata.AudioMetaData;
import uk.org.richardjarvis.metadata.FieldMeaning;
import uk.org.richardjarvis.processor.audio.AudioProcessor;
import uk.org.richardjarvis.utils.DataFrameUtils;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by rjarvis on 14/03/16.
 */
public class SpectralDensityDeriver implements AudioDeriveInterface, Serializable {

    private static final int NUMBER_OF_FREQ_OUTPUT = 300;
    private static final double FREQUENCY_BIN_SIZE = 5d;
    private static String[] notes = {"A", "A#", "B", "C", "C#", "D", "D#", "E", "F", "F#", "G", "G#"};

    @Override
    public DataFrame derive(DataFrame input, AudioMetaData metaData) {

        List<String> audioColumnNames = DataFrameUtils.getColumnsNames(DataFrameUtils.getColumnsOfMeaning(input, FieldMeaning.MeaningType.AUDIO_WAVEFORM));

        if (audioColumnNames.size()==0)
            return input;

        StructType newSchema = getUpdatedSchema(input, metaData);

        int originalFieldCount = input.schema().fieldNames().length;
        int newFieldCount = audioColumnNames.size();
        float frameRate = metaData.getAudioFileFormat().getFormat().getFrameRate();

        JavaRDD<Row> rows = input.javaRDD().map(row -> {

            Object[] outputRow = new Object[newFieldCount];

            int fieldIndex = 0;
            while (fieldIndex < originalFieldCount) {
                outputRow[fieldIndex] = row.get(fieldIndex);
                fieldIndex++;
            }

            for (String column : audioColumnNames) {

                List<Double> data = row.getList(row.fieldIndex(column));
                double[] buffer = applyHanningWindow(data);

                FastFourierTransformer fft = new FastFourierTransformer(DftNormalization.STANDARD);
                Complex spectrum[] = fft.transform(buffer, TransformType.FORWARD);

                List<Double> frequencies = getPowerSpectralDensity(spectrum, frameRate, FREQUENCY_BIN_SIZE);

                for (int i = 0; i < NUMBER_OF_FREQ_OUTPUT; i++) {
                    if (i < frequencies.size()) {
                        outputRow[fieldIndex++] = frequencies.get(i);
                    } else {
                        outputRow[fieldIndex++] = null;
                    }
                }
                outputRow[fieldIndex++] = getSpectrumCentroid(frequencies, frameRate);
            }
            return RowFactory.create(outputRow);

        });

        return input.sqlContext().createDataFrame(rows, newSchema);

    }

    private List<StructField> getSpectralSchema(int channelNumber, int numberOfBins, double binWidth) {

        List<StructField> fields = new ArrayList<>();

        for (int j = 0; j < numberOfBins; j++) {
            fields.add(new StructField("Channel_" + channelNumber + "_Frequency_" + j * binWidth + "_Hz", DataTypes.DoubleType, false, Metadata.empty()));
        }

        fields.add(new StructField("Channel_" + channelNumber + "_Spectral_Centroid_Hz", DataTypes.DoubleType, false, Metadata.empty()));
        return fields;
    }

    private List<Double> getPowerSpectralDensity(Complex[] frequncyDomainBuffer, float sampleFrequency, double frequencyBinSize) {

        List<Double> result = new ArrayList<>();

        int samplesPerBin = (int) Math.round(frequencyBinSize / sampleFrequency * frequncyDomainBuffer.length);

        for (int i = 0; i < frequncyDomainBuffer.length - samplesPerBin; i += samplesPerBin) {

            double powerDensity = 0;
            for (int binIndex = i; binIndex < i + samplesPerBin; binIndex++) {
                double real = frequncyDomainBuffer[binIndex].getReal();
                double imaginary = frequncyDomainBuffer[binIndex].getImaginary();
                powerDensity += ((real * real + imaginary * imaginary) / sampleFrequency);
            }
            result.add(powerDensity);
        }

        return result;
    }

    private Double getSpectrumCentroid(List<Double> fft, double framerate) {

        Double frequencyInterval = framerate / fft.size();
        Double sumFreq = 0d;
        Double sum = 0d;
        for (int i = 0; i < fft.size() / 2; i++) {
            double mag = fft.get(i);
            sumFreq += mag * (i + 0.5);
            sum += mag;
        }

        Double spectrumCentroid = sumFreq / sum * frequencyInterval;
        return spectrumCentroid;
    }

    private double getLength(Complex complex) {
        return Math.sqrt(complex.getReal() * complex.getReal() + complex.getImaginary() * complex.getImaginary());
    }

    public double[] applyHanningWindow(List<Double> buffer) {

        double[] buf = new double[buffer.size()];
        for (int n = 1; n < buffer.size(); n++) {
            buf[n] = buffer.get(n) * 0.5 * (1 - Math.cos((2 * Math.PI * n) / (buffer.size() - 1)));
        }
        return buf;
    }

    public static String closestKey(double freq) {
        int key = closestKeyIndex(freq);
        if (key <= 0) {
            return null;
        }
        int range = 1 + (key - 1) / notes.length;
        return notes[(key - 1) % notes.length] + range;
    }

    public static int closestKeyIndex(double freq) {
        return 1 + (int) ((12 * Math.log(freq / 440) / Math.log(2) + 49) - 0.5);
    }

    public StructType getUpdatedSchema(DataFrame input, AudioMetaData metaData) {

        List<StructField> newColumns = new ArrayList<>();

        newColumns.addAll(Arrays.asList(input.schema().fields()));

        List<Column> audioColumns = DataFrameUtils.getColumnsOfMeaning(input, FieldMeaning.MeaningType.AUDIO_WAVEFORM);

        for (Column column : audioColumns) {
            int channelNumber = (int) column.named().metadata().getLong(DataFrameUtils.CHANNEL_METADATA_KEY);
            newColumns.addAll(getSpectralSchema(channelNumber, NUMBER_OF_FREQ_OUTPUT, FREQUENCY_BIN_SIZE));
        }

        return new StructType(newColumns.toArray(new StructField[0]));
    }

}
