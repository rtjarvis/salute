package uk.org.richardjarvis.processor.text;

import org.junit.Test;
import uk.org.richardjarvis.metadata.text.TabularMetaData;

import java.io.File;
import java.io.FileWriter;

import static org.junit.Assert.assertTrue;

/**
 * Created by rjarvis on 26/02/16.
 */
public class TextProcessorTest {

    @Test
    public void testProcess() throws Exception {

        String testString = "1,2,3\n4,5,6\n";

        TabularProcessor textProcessor = new TabularProcessor();

        File outputFile = File.createTempFile("testFile", ".csv");

        FileWriter fw = new FileWriter(outputFile);
        fw.write(testString);
        fw.close();

        TabularMetaData result = textProcessor.extractMetaData(outputFile.getPath());

        outputFile.delete();

        assertTrue(',' == result.getCsvProperties().getDelimiter());
        assertTrue(null == result.getCsvProperties().getStringEnclosure());

    }
}