package uk.org.richardjarvis.processor.text;

import javafx.scene.control.Tab;
import org.apache.tika.io.IOUtils;
import org.junit.Test;
import uk.org.richardjarvis.metadata.TabularMetaData;

import java.io.BufferedInputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.StringBufferInputStream;

import static org.junit.Assert.*;

/**
 * Created by rjarvis on 26/02/16.
 */
public class TextProcessorTest {

    @Test
    public void testProcess() throws Exception {

        String testString = "1,2,3\n4,5,6\n";

        TextProcessor textProcessor = new TextProcessor();

        InputStream in = IOUtils.toInputStream(testString, "UTF-8");

        OutputStream out = null;

        TabularMetaData result = textProcessor.process(in, out);

        assertTrue(',' == result.getCsvProperties().getDelimiter());
        assertTrue(null == result.getCsvProperties().getStringEnclosure());

    }
}