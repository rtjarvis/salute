package uk.org.richardjarvis.processor;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * Created by rjarvis on 24/02/16.
 */
public interface ProcessorInterface {

    public boolean process(InputStream inputStream, OutputStream outputStream) throws IOException;

}
