package uk.org.richardjarvis.loader.image;

import org.apache.commons.io.FileUtils;
import org.junit.Test;
import uk.org.richardjarvis.loader.FileLoader;

import java.io.File;
import java.net.URL;

/**
 * Created by rjarvis on 20/04/16.
 */
public class ImageFullIT {

    @Test
    public void testProcess() throws Exception {

        String outputDir = "/tmp/test_image.json";
        File output = new File(outputDir);
        FileUtils.deleteDirectory(output);

        URL testFile = getClass().getResource("/image/warning.png");

        FileLoader fl = new FileLoader();

        fl.process(testFile.getPath(), outputDir);

    }

    @Test
    public void testProcess2() throws Exception {

        String outputDir = "/tmp/test_image.json";
        File output = new File(outputDir);
        FileUtils.deleteDirectory(output);

        URL testFile = getClass().getResource("/image/5x5.png");

        FileLoader fl = new FileLoader();

        fl.process(testFile.getPath(), outputDir);

    }

    @Test
    public void testProcess3() throws Exception {

        String outputDir = "/tmp/test_image.json";
        File output = new File(outputDir);
        FileUtils.deleteDirectory(output);

        URL testFile = getClass().getResource("/image/O.png");

        FileLoader fl = new FileLoader();

        fl.process(testFile.getPath(), outputDir);

    }

}
