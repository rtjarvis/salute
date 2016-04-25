package uk.org.richardjarvis.loader;

import com.google.common.io.Files;
import org.apache.commons.compress.archivers.ArchiveEntry;
import org.apache.commons.compress.archivers.ArchiveException;
import org.apache.commons.compress.archivers.ArchiveInputStream;
import org.apache.commons.compress.archivers.ArchiveStreamFactory;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.compressors.CompressorException;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.apache.tika.Tika;
import org.json.JSONException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.org.richardjarvis.derive.audio.AudioMasterDeriver;
import uk.org.richardjarvis.derive.image.ImageMasterDeriver;
import uk.org.richardjarvis.derive.nested.NestedMasterDeriver;
import uk.org.richardjarvis.derive.tabular.TabularMasterDeriver;
import uk.org.richardjarvis.metadata.audio.AudioMetaData;
import uk.org.richardjarvis.metadata.image.ImageMetaData;
import uk.org.richardjarvis.metadata.MetaData;
import uk.org.richardjarvis.metadata.nested.NestedMetaData;
import uk.org.richardjarvis.metadata.text.TabularMetaData;
import uk.org.richardjarvis.processor.ProcessorInterface;
import uk.org.richardjarvis.processor.audio.AudioProcessor;
import uk.org.richardjarvis.processor.image.ImageProcessor;
import uk.org.richardjarvis.processor.nested.MultiLineJSONProcessor;
import uk.org.richardjarvis.processor.text.TabularProcessor;
import uk.org.richardjarvis.utils.SparkProvider;
import uk.org.richardjarvis.writer.CSVWriter;
import uk.org.richardjarvis.writer.WriterInterface;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

/**
 * The main class that loads, processes and saves the input data
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

    /**
     * @param inputPath  the path of the input file that is to be processed
     * @param outputPath the path of the output file that will be saved
     * @return success of processing
     * @throws IOException
     * @throws CompressorException
     * @throws ArchiveException
     * @throws JSONException
     */
    public boolean process(String inputPath, String outputPath) throws IOException, CompressorException, ArchiveException, JSONException {

        LOGGER.info("Processing file : " + inputPath);

        String streamType = getType(inputPath);

        LOGGER.info("Detected stream as: " + streamType);

        if (FileAssessor.isArchive(streamType)) {
            LOGGER.info("Stream is archive. Processing archive");
            List<String> unpackedEntries = uncompressTar(inputPath);
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
                case JSON:
                    LOGGER.info("Processing file as JSON");
                    processor = new MultiLineJSONProcessor();
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

                    TabularMasterDeriver masterDeriver = new TabularMasterDeriver();
                    derivedData = masterDeriver.derive(data, (TabularMetaData) metaData);

                } else if (metaData instanceof AudioMetaData) {

                    AudioMasterDeriver masterDeriver = new AudioMasterDeriver();
                    derivedData = masterDeriver.derive(data, (AudioMetaData) metaData);

                } else if (metaData instanceof ImageMetaData) {

                    ImageMasterDeriver masterDeriver = new ImageMasterDeriver();
                    derivedData = masterDeriver.derive(data, (ImageMetaData) metaData);

                } else if (metaData instanceof NestedMetaData) {

                    NestedMasterDeriver masterDeriver = new NestedMasterDeriver();
                    derivedData = masterDeriver.derive(data, (NestedMetaData) metaData);
                }
                WriterInterface writer = new CSVWriter();

                writer.write(derivedData, metaData, outputPath);

                metaData.generateReport(inputPath, data, derivedData, outputPath + "metadataReport.html");

                return true;
            }

        }

        return false;
    }


    public boolean process(List<String> inputPaths, String outputPath) throws IOException, CompressorException, ArchiveException, JSONException {

        boolean success = true;
        for (String inputPath : inputPaths) {
            success &= process(inputPath, outputPath + "_" + inputPath);
        }
        return success;
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

    public static List<String> uncompressTar(String tarFilePath) throws IOException {

        File tempDir = Files.createTempDir();

        LOGGER.info("Extracting taar file contents from " + tarFilePath);

        List<String> contents = new ArrayList<>();

        TarArchiveInputStream tarIn = new TarArchiveInputStream(new BufferedInputStream(new FileInputStream(tarFilePath)));

        TarArchiveEntry tarEntry = tarIn.getNextTarEntry();

        while (tarEntry != null) {

            File dest = new File(tempDir, tarEntry.getName());
            LOGGER.info("Creating " + dest.getPath() + " from entry " + tarEntry.getName());

            if (tarEntry.isDirectory()) {
                dest.mkdirs();
            } else {
                dest.createNewFile();
                //byte [] btoRead = new byte[(int)tarEntry.getSize()];
                byte[] btoRead = new byte[1024];
                //FileInputStream fin
                //  = new FileInputStream(destPath.getCanonicalPath());
                BufferedOutputStream bout =
                        new BufferedOutputStream(new FileOutputStream(dest));
                int len = 0;

                while ((len = tarIn.read(btoRead)) != -1) {
                    bout.write(btoRead, 0, len);
                }

                bout.close();
                btoRead = null;

            }
            contents.add(dest.getPath());
            tarEntry = tarIn.getNextTarEntry();
        }
        tarIn.close();

        return contents;
    }

    public SQLContext getSqlContext() {
        if (this.sqlContext == null)
            this.sqlContext = SparkProvider.getSQLContext();
        return sqlContext;
    }
}
