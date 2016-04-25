package uk.org.richardjarvis.loader.nested;

import org.apache.commons.io.FileUtils;
import org.junit.Test;
import uk.org.richardjarvis.loader.FileLoader;

import java.io.File;
import java.net.URL;

/**
 * Created by rjarvis on 26/02/16.
 */
public class JsonFullIT {


    // Test audio file fom http://freemusicarchive.org/music/sawsquarenoise/RottenMage_SpaceJacked/RottenMage_SpaceJacked_OST_01#

    @Test
    public void testProcess() throws Exception {

        String outputDir = "/tmp/test_json.json";
        File output = new File(outputDir);
        FileUtils.deleteDirectory(output);

        URL testFile = getClass().getResource("/json/test.json");

        FileLoader fl = new FileLoader();

        fl.process(testFile.getPath(), outputDir);

    }
}