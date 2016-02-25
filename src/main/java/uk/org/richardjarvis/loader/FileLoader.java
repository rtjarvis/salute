package uk.org.richardjarvis.loader;

import org.apache.commons.compress.archivers.ArchiveEntry;
import org.apache.commons.compress.archivers.ArchiveException;
import org.apache.commons.compress.archivers.ArchiveInputStream;
import org.apache.commons.compress.archivers.ArchiveStreamFactory;
import org.apache.commons.compress.archivers.zip.ZipArchiveEntry;
import org.apache.commons.compress.compressors.CompressorException;
import org.apache.commons.compress.compressors.CompressorStreamFactory;
import org.apache.tika.Tika;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.org.richardjarvis.processor.image.ImageProcessor;
import uk.org.richardjarvis.processor.ProcessorInterface;
import uk.org.richardjarvis.processor.text.TextProcessor;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by rjarvis on 22/02/16.
 */
public class FileLoader {

    private static Logger LOGGER = LoggerFactory.getLogger(FileLoader.class);

    Tika tika;

    public FileLoader() {
        this.tika = new Tika();
    }

    public boolean process(InputStream inputFileStream, OutputStream outputStream, String inputName) throws IOException, CompressorException, ArchiveException {

        LOGGER.info("Processing file : " + inputName);

        String streamType = getType(inputFileStream);

        LOGGER.info("Detected stream as: " + streamType);

        if (FileAssessor.isArchive(streamType)) {
            LOGGER.info("Stream is archive. Processing archive");
            List<ArchiveEntry> unpackedEntries = unpack(inputFileStream);
            LOGGER.info("Packed File contained " + unpackedEntries.size() + " child streams.");
            return process(unpackedEntries, outputStream);

        } else {

            if (FileAssessor.isCompressed(streamType)) {
                LOGGER.info("Stream compressed. Removing compression");
                inputFileStream = removeCompression(inputFileStream);
            }

            ProcessorInterface processor = null;

            switch (FileAssessor.getType(streamType)) {
                case TEXT:
                    LOGGER.info("Processing file as text");
                    processor = new TextProcessor();
                    break;
                case IMAGE:
                    LOGGER.info("Processing file as an image");
                    processor = new ImageProcessor();
                    break;
                case AUDIO:
                    LOGGER.info("Processing file as audio");
                    processor = new ImageProcessor();
                    break;
            }

            if (processor != null) {
                boolean result = processor.process(inputFileStream, outputStream);
                if (result) {
                    LOGGER.info("Processing file SUCCESS");
                    return true;
                } else {
                    LOGGER.info("Processing file FAILURE");
                }
            }

        }
        // process file here
        return false;
    }


    public boolean process(List<ArchiveEntry> entries, OutputStream outputStream) throws IOException, CompressorException, ArchiveException {

        for (ArchiveEntry archiveEntry : entries) {
            ZipArchiveEntry zar = (ZipArchiveEntry) archiveEntry;

            //archiveEntry.get
            //process(archiveEntry.)
        }
        return true;
    }


    private String getType(InputStream inputStream) throws CompressorException, IOException {

        return tika.detect(inputStream);

    }


    private InputStream removeCompression(InputStream inputStream) throws CompressorException {

        return new CompressorStreamFactory().createCompressorInputStream(inputStream);

    }

    private List<ArchiveEntry> unpack(InputStream inputStream) throws ArchiveException, IOException {

        ArchiveInputStream archiveInputStream = new ArchiveStreamFactory().createArchiveInputStream(inputStream);

        List<ArchiveEntry> entries = new ArrayList<>();

        ArchiveEntry entry = archiveInputStream.getNextEntry();

        while (entry != null) {
            entries.add(entry);
            entry = archiveInputStream.getNextEntry();
        }

        return entries;

    }


}
