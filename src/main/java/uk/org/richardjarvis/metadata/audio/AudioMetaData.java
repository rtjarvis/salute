package uk.org.richardjarvis.metadata.audio;

import org.apache.spark.sql.DataFrame;
import org.apache.tika.metadata.Metadata;
import uk.org.richardjarvis.metadata.MetaData;
import uk.org.richardjarvis.utils.file.FileUtils;
import uk.org.richardjarvis.utils.report.ReportUtil;

import javax.sound.sampled.AudioFileFormat;
import java.io.File;
import java.io.IOException;

/**
 * Created by rjarvis on 26/02/16.
 */
public class AudioMetaData implements MetaData {
    private AudioFileFormat audioFileFormat;
    private Metadata audioFileMetadata;
    private double timeWindowLength;

    public void setAudioFileFormat(AudioFileFormat audioFileFormat) {
        this.audioFileFormat = audioFileFormat;
    }

    public AudioFileFormat getAudioFileFormat() {
        return audioFileFormat;
    }

    public void setTimeWindowLength(double timeWindowLength) {
        this.timeWindowLength = timeWindowLength;
    }

    public double getTimeWindowLength() {
        return timeWindowLength;
    }

    @Override
    public void generateReport(String inputPath, DataFrame inputData, DataFrame derivedData, String outputPath) {
        StringBuilder sb = new StringBuilder();

        ReportUtil.addHTMLHeader(sb, "Audio File Report");
        ReportUtil.addBodyStart(sb);
        ReportUtil.addLinkToFile(sb, inputPath);
        ReportUtil.addHTMLTable(sb, "Audio MetaData Properties", audioFileMetadata);
        ReportUtil.addHTMLTable(sb, "Sample Output Data", derivedData, 10);
        ReportUtil.addBodyEnd(sb);
        ReportUtil.addHTMLFooter(sb);

        try {
            FileUtils.writeBufferToFile(sb, outputPath);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void setMetadata(Metadata audioFileMetadata) {
        this.audioFileMetadata = audioFileMetadata;
    }
}
