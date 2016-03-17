package uk.org.richardjarvis.metadata;

import javax.sound.sampled.AudioFileFormat;

/**
 * Created by rjarvis on 26/02/16.
 */
public class AudioMetaData implements MetaData {
    private AudioFileFormat audioFileFormat;
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
}
