package uk.org.richardjarvis.processor.text;

import au.com.bytecode.opencsv.CSVReader;
import org.slf4j.LoggerFactory;
import uk.org.richardjarvis.processor.ProcessorInterface;

import java.io.*;
import java.util.*;

/**
 * Created by rjarvis on 24/02/16.
 */
public class TextProcessor implements ProcessorInterface {

    private static org.slf4j.Logger LOGGER = LoggerFactory.getLogger(TextProcessor.class);
    public static int MAX_ROWS_TO_PROCESS=100;

    @Override
    public boolean process(InputStream inputStream, OutputStream outputStream) throws IOException {

        CSVProperties csvProperties = new CSVProperties(inputStream);

        LOGGER.info(csvProperties.toString());

        BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));

        CSVReader csvReader = getCSVReader(reader, csvProperties);

        DataProperties dataProperties = new DataProperties(csvReader);

        LOGGER.info(dataProperties.toString());


        // detect header


        // guess field type for each column

        // check column distribution

        // derive additional columns (J-Hot, EventCube)

        // check column correlations (RDC)


        return true;


    }

    private CSVReader getCSVReader(Reader reader, CSVProperties csvProperties) {

        if (csvProperties.getDelimiter() == null)
            return null;


        if (csvProperties.getStringEnclosure() != null) {
            return new CSVReader(reader, csvProperties.getDelimiter(), csvProperties.getStringEnclosure());
        } else {
            return new CSVReader(reader, csvProperties.getDelimiter());
        }

    }


}