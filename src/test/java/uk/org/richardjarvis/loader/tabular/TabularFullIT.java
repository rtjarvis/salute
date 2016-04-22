package uk.org.richardjarvis.loader.tabular;

import org.apache.commons.io.FileUtils;
import org.junit.Test;
import uk.org.richardjarvis.loader.FileLoader;

import java.io.File;
import java.net.URL;

/**
 * Created by rjarvis on 22/02/16.
 */
public class TabularFullIT {

    @Test
    public void testProcess() throws Exception {

        String outputDir = "/tmp/test.json";
        File output = new File(outputDir);
        FileUtils.deleteDirectory(output);

        URL testFile = getClass().getResource("/text/uk-500.csv");

        FileLoader fl = new FileLoader();

        fl.process(testFile.getPath(), outputDir);

    }

    @Test
    public void testProcess2() throws Exception {

        String outputDir = "/tmp/test.json";
        File output = new File(outputDir);
        FileUtils.deleteDirectory(output);

        URL testFile = getClass().getResource("/text/csv_no_string_escape.csv");

        FileLoader fl = new FileLoader();

        fl.process(testFile.getPath(), outputDir);

    }

    @Test
    public void testProcess3() throws Exception {

        String outputDir = "/tmp/test2.json";
        File output = new File(outputDir);
        FileUtils.deleteDirectory(output);

        URL testFile = getClass().getResource("/text/csv_with_date_and_header.csv");

        FileLoader fl = new FileLoader();

        fl.process(testFile.getPath(), outputDir);

    }

    @Test
    public void testProcess4() throws Exception {

        String outputDir = "/tmp/test3.json";
        File output = new File(outputDir);
        FileUtils.deleteDirectory(output);

        URL testFile = getClass().getResource("/text/csv_with_ip.csv");

        FileLoader fl = new FileLoader();

        fl.process(testFile.getPath(), outputDir);

    }

    @Test
    public void testProcess5() throws Exception {

        String outputDir = "/tmp/test4.json";
        File output = new File(outputDir);
        FileUtils.deleteDirectory(output);

        URL testFile = getClass().getResource("/text/csvs.tar");

        FileLoader fl = new FileLoader();

        fl.process(testFile.getPath(), outputDir);

    }

    @Test
    public void testProcess6() throws Exception {

        String outputDir = "/tmp/test5.json";
        File output = new File(outputDir);
        FileUtils.deleteDirectory(output);

        URL testFile = getClass().getResource("/text/csv_dirs.tar");

        FileLoader fl = new FileLoader();

        fl.process(testFile.getPath(), outputDir);

    }

}