package uk.org.richardjarvis.loader;

import org.apache.commons.compress.archivers.ArchiveEntry;
import org.apache.commons.compress.archivers.ArchiveException;
import org.apache.commons.compress.archivers.ArchiveInputStream;
import org.apache.commons.compress.archivers.ArchiveStreamFactory;
import org.apache.commons.compress.archivers.zip.ZipArchiveEntry;
import org.apache.commons.compress.compressors.CompressorException;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.apache.tika.Tika;
import org.json.JSONException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.org.richardjarvis.derive.tabular.MasterDeriver;
import uk.org.richardjarvis.metadata.Statistics;
import uk.org.richardjarvis.metadata.MetaData;
import uk.org.richardjarvis.metadata.TabularMetaData;
import uk.org.richardjarvis.processor.ProcessorInterface;
import uk.org.richardjarvis.processor.audio.AudioProcessor;
import uk.org.richardjarvis.processor.image.ImageProcessor;
import uk.org.richardjarvis.processor.text.TabularProcessor;
import uk.org.richardjarvis.utils.SparkProvider;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by rjarvis on 22/02/16.
 */
public class FileLoader {

    private static Logger LOGGER = LoggerFactory.getLogger(FileLoader.class);

    Tika tika;
    SQLContext sqlContext;

    public static void main(String[] args) throws CompressorException, ArchiveException, JSONException, IOException {

        FileLoader fl = new FileLoader();

        fl.process(args[0], args[1]);

    }

    public FileLoader() {
        this.tika = new Tika();
    }

    public boolean process(String inputPath, String outputPath) throws IOException, CompressorException, ArchiveException, JSONException {

        LOGGER.info("Processing file : " + inputPath);

        String streamType = getType(inputPath);

        LOGGER.info("Detected stream as: " + streamType);

        if (FileAssessor.isArchive(streamType)) {
            LOGGER.info("Stream is archive. Processing archive");
            List<ArchiveEntry> unpackedEntries = unpack(inputPath);
            LOGGER.info("Packed File contained " + unpackedEntries.size() + " child streams.");
            return process(unpackedEntries, outputPath);

        } else {

            if (FileAssessor.isCompressed(streamType)) {
                LOGGER.info("Stream compressed. Removing compression");
                inputPath = removeCompression(inputPath);
            }

            ProcessorInterface processor = null;

            switch (FileAssessor.getType(streamType)) {
                case TEXT:
                    LOGGER.info("Processing file as text");
                    processor = new TabularProcessor();
                    break;
                case IMAGE:
                    LOGGER.info("Processing file as an image");
                    processor = new ImageProcessor();
                    break;
                case AUDIO:
                    LOGGER.info("Processing file as audio");
                    processor = new AudioProcessor();
                    break;
            }

            if (processor != null) {

                MetaData metaData = processor.extractMetaData(inputPath);
                if (metaData != null) {
                    LOGGER.info("MetaData Extraction SUCCESS");
                } else {
                    LOGGER.info("MetaData Extraction FAILURE");
                    return false;
                }

                DataFrame data = processor.extractData(inputPath, metaData, getSqlContext());
                if (data != null) {
                    LOGGER.info("Data Extraction SUCCESS");
                } else {
                    LOGGER.info("Data Extraction FAILURE");
                    return false;
                }

                DataFrame derivedData = data;

                if (metaData instanceof TabularMetaData) {

                    MasterDeriver masterDeriver = new MasterDeriver();
                    derivedData = masterDeriver.derive(data, (TabularMetaData) metaData);
                }

                derivedData.write().format("json").save(outputPath);

                return true;
            }

        }


        return false;
    }


    public boolean process(List<ArchiveEntry> entries, String outputPath) throws IOException, CompressorException, ArchiveException {

        for (ArchiveEntry archiveEntry : entries) {
            ZipArchiveEntry zar = (ZipArchiveEntry) archiveEntry;

            //archiveEntry.get
            //extractMetaData(archiveEntry.)
        }
        return true;
    }


    private String getType(String path) throws CompressorException, IOException {

        return tika.detect(path);

    }


    private String removeCompression(String inputPath) throws CompressorException {

        return inputPath; // TODO

        //return new CompressorStreamFactory().createCompressorInputStream(inputStream);

    }

    private List<ArchiveEntry> unpack(String path) throws ArchiveException, IOException {

        ArchiveInputStream archiveInputStream = new ArchiveStreamFactory().createArchiveInputStream(new FileInputStream(path));

        List<ArchiveEntry> entries = new ArrayList<>();

        ArchiveEntry entry = archiveInputStream.getNextEntry();

        while (entry != null) {
            entries.add(entry);
            entry = archiveInputStream.getNextEntry();
        }

        return entries;

    }


    public SQLContext getSqlContext() {
        if (this.sqlContext==null)
            this.sqlContext = SparkProvider.getSQLContext();
        return sqlContext;
    }
}
