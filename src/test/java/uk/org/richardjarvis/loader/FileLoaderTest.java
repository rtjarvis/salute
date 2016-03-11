package uk.org.richardjarvis.loader;

import org.apache.commons.io.FileUtils;
import org.junit.Test;
import uk.org.richardjarvis.loader.FileLoader;

import java.io.File;
import java.io.InputStream;
import java.net.URL;
import java.util.Collection;
import java.util.Collections;

import static org.junit.Assert.*;

/**
 * Created by rjarvis on 22/02/16.
 */
public class FileLoaderTest {

    @Test
    public void testProcess() throws Exception {

        String outputDir = "/tmp/test.json";
        File output = new File(outputDir);
        FileUtils.deleteDirectory(output);

        URL testFile = getClass().getResource("/uk-500.csv");

        FileLoader fl = new FileLoader();

        fl.process(testFile.getPath(), outputDir);

    }

    @Test
    public void testProcess2() throws Exception {

        String outputDir = "/tmp/test.json";
        File output = new File(outputDir);
        FileUtils.deleteDirectory(output);

        URL testFile = getClass().getResource("/csv_no_string_escape.csv");

        FileLoader fl = new FileLoader();

        fl.process(testFile.getPath(), outputDir);

    }

    @Test
    public void testProcess3() throws Exception {

        String outputDir = "/tmp/test2.json";
        File output = new File(outputDir);
        FileUtils.deleteDirectory(output);

        URL testFile = getClass().getResource("/csv_with_date_and_header.csv");

        FileLoader fl = new FileLoader();

        fl.process(testFile.getPath(), outputDir);

    }
}