package uk.org.richardjarvis.metadata;

import javax.sound.sampled.AudioFileFormat;

/**
 * Created by rjarvis on 26/02/16.
 */
public class AudioMetaData implements MetaData {
    private AudioFileFormat audioFileFormat;

    public void setAudioFileFormat(AudioFileFormat audioFileFormat) {
        this.audioFileFormat = audioFileFormat;
    }

    public AudioFileFormat getAudioFileFormat() {
        return audioFileFormat;
    }
}
