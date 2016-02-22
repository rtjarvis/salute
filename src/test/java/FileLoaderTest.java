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


        URL testFile = getClass().getResource("/uk-500.zip");

        InputStream inputStream = testFile.openStream();

        Collection<InputStream> inputStreams = Collections.singletonList(inputStream);

        FileLoader fl = new FileLoader();

        fl.process(inputStreams, System.out);

    }
}