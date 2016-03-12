package uk.org.richardjarvis.processor.audio;

import org.apache.commons.math3.complex.Complex;
import org.apache.commons.math3.transform.FastFourierTransformer;
import org.apache.commons.math3.transform.DftNormalization;
import org.apache.commons.math3.transform.TransformType;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import uk.org.richardjarvis.metadata.AudioMetaData;
import uk.org.richardjarvis.metadata.MetaData;
import uk.org.richardjarvis.processor.ProcessorInterface;

import javax.sound.sampled.*;
import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * Created by rjarvis on 24/02/16.
 */
public class AudioProcessor implements ProcessorInterface {

    private static final int TIME_WINDOW_LENGTH_SECONDS = 5;
    private static final int NUMBER_OF_FREQ_OUTPUT = 10;

    static String[] notes = {"A", "A#", "B", "C", "C#", "D", "D#", "E", "F", "F#", "G", "G#"};

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
            double time = 0;

            List<Row> rows = new ArrayList<>();


            while (offset < size) {

                Object[] row = new Object[1 + numberOfChannels * NUMBER_OF_FREQ_OUTPUT];
                int rowIndex = 0;
                row[rowIndex++] = time;
                byte[] timeBuffer = readBlock(audioInputStream, audioFormat, TIME_WINDOW_LENGTH_SECONDS);
                offset += timeBuffer.length;
                time += getTimeLength(timeBuffer.length, audioFormat);

                for (int channel = 0; channel < numberOfChannels; channel++) {

                    double[] channelBuffer = getBuffer(timeBuffer, channel, audioFormat);
                    channelBuffer = hanningWindow(channelBuffer);

                    List<Double> frequencies = getFFT(channelBuffer, audioFormat.getFrameRate());

                    for (int i = 0; i < NUMBER_OF_FREQ_OUTPUT; i++) {
                        if (i < frequencies.size()) {
                            row[rowIndex++] = frequencies.get(i);
                        } else {
                            row[rowIndex++] = null;
                        }
                    }

                }

                rows.add(RowFactory.create(row));

            }

            return sqlContext.createDataFrame(rows, getSchema(numberOfChannels));


        } catch (UnsupportedAudioFileException e) {
            throw new IOException("Cannot read that type of Audio File");
        }

    }

    private StructType getSchema(int numberOfChannels) {

        StructField[] fields = new StructField[1 + numberOfChannels * NUMBER_OF_FREQ_OUTPUT];

        int fieldIndex = 0;
        fields[fieldIndex++] = new StructField("Time", DataTypes.DoubleType, false, Metadata.empty());
        for (int i = 0; i < numberOfChannels; i++) {
            for (int j = 0; j < NUMBER_OF_FREQ_OUTPUT; j++) {
                fields[fieldIndex++] = new StructField("Channel_" + i + "_frequency_" + j, DataTypes.DoubleType, false, Metadata.empty());
            }
        }
        return new StructType(fields);
    }

    private double getTimeLength(int length, AudioFormat audioFormat) {
        return (double) length / (audioFormat.getFrameRate() * audioFormat.getFrameSize());
    }

    private List<Double> getFFT(double[] buffer, float rate) {
        FastFourierTransformer fft = new FastFourierTransformer(DftNormalization.STANDARD);
        Complex resultC[] = fft.transform(buffer, TransformType.FORWARD);

        double[] results = new double[resultC.length];
        for (int i = 0; i < resultC.length; i++) {
            double real = resultC[i].getReal();
            double imaginary = resultC[i].getImaginary();
            results[i] = Math.sqrt(real * real + imaginary * imaginary);
        }

        return DiscreteFourierTransform.process(results, rate, resultC.length, 7);
    }


    private double[] getBuffer(byte[] bytesIn, int channelNumber, AudioFormat audioFormat) {

        int numChannels = audioFormat.getChannels();
        int sampleSizeBytes = audioFormat.getSampleSizeInBits() / 8;
        int step = sampleSizeBytes * numChannels;
        int bufferSize = Math.round(bytesIn.length / step);
        int bufferSizePower2 = (int) Math.pow(2, Math.round(Math.log(bufferSize) / Math.log(2) + .5));
        int startOffset = channelNumber * sampleSizeBytes;
        int sampleSize = (int) Math.pow(2, sampleSizeBytes * 8) / 2;

        double[] buffer = new double[bufferSizePower2];
        int idx = 0;
        for (int i = startOffset; i < bytesIn.length && idx < bufferSize; i += step) {
            byte blow = bytesIn[i];
            byte bhigh = bytesIn[i + 1];
            buffer[idx++] = (double) (blow & 0xFF | bhigh << 8) / sampleSize;
        }
        return buffer;
    }

    public double[] hanningWindow(double[] buffer) {
        for (int n = 1; n < buffer.length; n++) {
            buffer[n] *= 0.5 * (1 - Math.cos((2 * Math.PI * n) / (buffer.length - 1)));
        }
        return buffer;
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

    /*
     Method returns buffers that are a power of 2 long (for FFT) that
     are longer than the requested Length. There are interleaved channels
     accoridng to the original file
     */
    private byte[] readBlock(AudioInputStream audioInputStream, AudioFormat audioFormat, int lengthSeconds) throws IOException {

        double bitsPerSecondPerChannel = audioFormat.getSampleSizeInBits() * audioFormat.getSampleRate();

        int bufferSizePower2 = (int) Math.pow(2, Math.round(Math.log(lengthSeconds * bitsPerSecondPerChannel / 8) / Math.log(2) + .5));

        byte[] buffer = new byte[bufferSizePower2 * audioFormat.getChannels()];

        audioInputStream.read(buffer);

        return buffer;

    }
}
