package uk.org.richardjarvis.loader.audio;

import org.apache.commons.io.FileUtils;
import org.junit.Test;
import uk.org.richardjarvis.loader.FileLoader;

import java.io.File;
import java.net.URL;

import static org.junit.Assert.assertTrue;

/**
 * Created by rjarvis on 26/02/16.
 */
public class AudioFullTest {


    // Test audio file fom http://freemusicarchive.org/music/sawsquarenoise/RottenMage_SpaceJacked/RottenMage_SpaceJacked_OST_01#

    @Test
    public void testProcess() throws Exception {

        String outputDir = "/tmp/test_audio.json";
        File output = new File(outputDir);
        FileUtils.deleteDirectory(output);

        URL testFile = getClass().getResource("/test.wav");

        FileLoader fl = new FileLoader();

        fl.process(testFile.getPath(), outputDir);

    }


    // Test audio file fom http://freemusicarchive.org/music/sawsquarenoise/RottenMage_SpaceJacked/RottenMage_SpaceJacked_OST_01#

    @Test
    public void testProcess2() throws Exception {

        String outputDir = "/tmp/test_audio.json";
        File output = new File(outputDir);
        FileUtils.deleteDirectory(output);

        URL testFile = getClass().getResource("/tone_2_channel_fade.wav");

        FileLoader fl = new FileLoader();

        fl.process(testFile.getPath(), outputDir);

    }
}