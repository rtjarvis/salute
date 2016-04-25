package uk.org.richardjarvis.utils.file;

import org.apache.tika.exception.TikaException;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.AutoDetectParser;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.Parser;
import org.apache.tika.sax.BodyContentHandler;
import org.xml.sax.SAXException;

import java.io.*;
import java.net.URL;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

/**
 * Created by rjarvis on 21/03/16.
 */
public class FileUtils {

    public static boolean downloadZipFileAsResource(URL url) {
        /*

        This product includes GeoLite2 data created by MaxMind, available from
                <a href="http://www.maxmind.com">http://www.maxmind.com</a>.
        */

        String downloadPath = getRootDataPath();
        try {
            ZipInputStream zipIn = new ZipInputStream(url.openStream());

            ZipEntry entry = zipIn.getNextEntry();
            // iterates over entries in the zip file
            while (entry != null) {
                File entryFile = new File(entry.getName());
                String filePath = downloadPath + entryFile.getName();
                if (!entry.isDirectory()) {
                    // if the entry is a file, extracts it
                    extractFile(zipIn, filePath);
                }
                zipIn.closeEntry();
                entry = zipIn.getNextEntry();
            }
            zipIn.close();

        } catch (java.io.IOException e) {
            return false;
        }
        return true;
    }

    public static void extractFile(ZipInputStream zipIn, String filePath) throws IOException {
        BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream(filePath));
        byte[] bytesIn = new byte[4096];
        int read = 0;
        while ((read = zipIn.read(bytesIn)) != -1) {
            bos.write(bytesIn, 0, read);
        }
        bos.close();
    }

    public static String getRootDataPath() {
        return FileUtils.class.getResource("/").getPath();
    }

    public static void writeBufferToFile(StringBuilder sb, String path) throws IOException {

        FileWriter fw = new FileWriter(path);
        fw.write(sb.toString());
        fw.close();

    }

    public static Metadata getMetadata(String path) throws IOException, TikaException, SAXException {

        Parser parser = new AutoDetectParser();
        BodyContentHandler handler = new BodyContentHandler();
        Metadata metadata = new Metadata();
        FileInputStream inputstream = new FileInputStream(path);
        ParseContext context = new ParseContext();

        parser.parse(inputstream, handler, metadata, context);

        return metadata;

    }

}
