package uk.org.richardjarvis.writer;

import uk.org.richardjarvis.metadata.MetaData;

import java.io.InputStream;
import java.io.OutputStream;

/**
 * Created by rjarvis on 26/02/16.
 */
public interface WriterInterface {

    boolean write(MetaData metadata, InputStream inputStream, OutputStream outputStream);
}
