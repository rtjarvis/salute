package uk.org.richardjarvis.processor.audio;

import org.apache.commons.math3.complex.Complex;
import org.apache.commons.math3.transform.FastFourierTransformer;
import org.apache.commons.math3.transform.DftNormalization;
import org.apache.commons.math3.transform.TransformType;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.*;
import uk.org.richardjarvis.metadata.AudioMetaData;
import uk.org.richardjarvis.metadata.MetaData;
import uk.org.richardjarvis.processor.ProcessorInterface;
import uk.org.richardjarvis.utils.DataFrameUtils;

import javax.sound.sampled.*;
import java.io.*;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by rjarvis on 24/02/16.
 */
public class AudioProcessor implements ProcessorInterface {

    private static final double TIME_WINDOW_LENGTH_SECONDS = 1.0d;

    @Override
    public MetaData extractMetaData(String path) throws IOException {

        AudioMetaData metaData = new AudioMetaData();
        try {
            AudioFileFormat audioFileFormat = AudioSystem.getAudioFileFormat(new File(path));

            metaData.setAudioFileFormat(audioFileFormat);

        } catch (UnsupportedAudioFileException e) {
            throw new IOException("Cannot read that type of Audio File");
        }
        return metaData;
    }

    @Override
    public DataFrame extractData(String path, MetaData metaData, SQLContext sqlContext) throws IOException {

        AudioMetaData audioMetaData = (AudioMetaData) metaData;

        try {

            AudioInputStream audioInputStream = AudioSystem.getAudioInputStream(new File(path));
            AudioFormat audioFormat = audioMetaData.getAudioFileFormat().getFormat();

            int size = audioInputStream.available();
            int numberOfChannels = audioFormat.getChannels();
            int offset = 0;
            double startTime = 0;

            List<Row> rows = new ArrayList<>();

            while (offset < size) {

                Object[] row = new Object[3 + numberOfChannels];

                int rowIndex = 0;
                row[rowIndex++] = startTime;
                byte[] timeBuffer = readBlock(audioInputStream, audioFormat, TIME_WINDOW_LENGTH_SECONDS);
                offset += timeBuffer.length;


                for (int channel = 0; channel < numberOfChannels; channel++) {

                    List<Double> buf = getBuffer(timeBuffer, channel, audioFormat);
                    double length = getTimeLength(timeBuffer.length, audioFormat);

                    if (channel == 0) {
                        row[rowIndex++] = startTime + length;
                        row[rowIndex++] = length;
                        startTime += length;
                    }
                    row[rowIndex++] = buf;
                }
                rows.add(RowFactory.create(row));
            }
            return sqlContext.createDataFrame(rows, getSchema(numberOfChannels));

        } catch (UnsupportedAudioFileException e) {
            throw new IOException("Cannot read that type of Audio File");
        }

    }

    private StructType getSchema(int numberOfChannels) {

        StructField[] fields = new StructField[3 + numberOfChannels];

        int fieldIndex = 0;
        fields[fieldIndex++] = new StructField("Start Time", DataTypes.DoubleType, false, Metadata.empty());
        fields[fieldIndex++] = new StructField("End Time", DataTypes.DoubleType, false, Metadata.empty());
        fields[fieldIndex++] = new StructField("Window Length", DataTypes.DoubleType, false, Metadata.empty());
        for (int i = 0; i < numberOfChannels; i++) {
            fields[fieldIndex++] = new StructField("Channel_" + i, DataTypes.createArrayType(DataTypes.DoubleType), false, new MetadataBuilder().putLong(DataFrameUtils.CHANNEL_METADATA_KEY, (long) i).build());
        }
        return new StructType(fields);
    }

    private List<Double> getBuffer(byte[] bytesIn, int channelNumber, AudioFormat audioFormat) {

        int numChannels = audioFormat.getChannels();
        int sampleSizeBytes = audioFormat.getSampleSizeInBits() / 8;
        int step = sampleSizeBytes * numChannels;
        int bufferSize = Math.round(bytesIn.length / step);
        int bufferSizePower2 = (int) Math.pow(2, Math.round(Math.log(bufferSize) / Math.log(2) + .5));
        int startOffset = channelNumber * sampleSizeBytes;
        int sampleSize = (int) Math.pow(2, sampleSizeBytes * 8) / 2;

        List<Double> buffer = new ArrayList<>(bufferSizePower2);
        for (int i = startOffset; i < bytesIn.length; i += step) {
            byte blow = bytesIn[i];
            byte bhigh = bytesIn[i + 1];
            buffer.add((double) (blow & 0xFF | bhigh << 8) / sampleSize);
        }
        return buffer;
    }

    /*
     Method returns buffers that are a power of 2 long (for FFT) that
     are longer than the requested Length. There are interleaved channels
     accoridng to the original file
     */
    private byte[] readBlock(AudioInputStream audioInputStream, AudioFormat audioFormat, double lengthSeconds) throws IOException {

        double bitsPerSecondPerChannel = audioFormat.getSampleSizeInBits() * audioFormat.getSampleRate();

        int bufferSizePower2 = (int) Math.pow(2, Math.round(Math.log(lengthSeconds * bitsPerSecondPerChannel / 8) / Math.log(2) + .5));

        byte[] buffer = new byte[bufferSizePower2 * audioFormat.getChannels()];

        audioInputStream.read(buffer);

        return buffer;

    }

    private double getTimeLength(int length, AudioFormat audioFormat) {
        return (double) length / (audioFormat.getFrameRate() * audioFormat.getFrameSize());
    }

}
