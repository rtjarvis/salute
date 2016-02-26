package uk.org.richardjarvis.loader;

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


        URL testFile = getClass().getResource("/uk-500.csv");

        InputStream inputStream = testFile.openStream();

        FileLoader fl = new FileLoader();

        fl.process(inputStream, System.out, testFile.getFile());

    }

    @Test
    public void testProcess2() throws Exception {


        URL testFile = getClass().getResource("/csv_no_string_escape.csv");

        InputStream inputStream = testFile.openStream();

        FileLoader fl = new FileLoader();

        fl.process(inputStream, System.out, testFile.getFile());

    }
}