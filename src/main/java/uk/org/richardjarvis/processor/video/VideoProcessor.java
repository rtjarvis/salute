package uk.org.richardjarvis.processor.video;

import uk.org.richardjarvis.metadata.MetaData;
import uk.org.richardjarvis.processor.ProcessorInterface;

import java.io.InputStream;
import java.io.OutputStream;

/**
 * Created by rjarvis on 24/02/16.
 */
public class VideoProcessor implements ProcessorInterface {

    @Override
    public MetaData process(InputStream inputStream, OutputStream outputStream) {
        return null;
    }
}
