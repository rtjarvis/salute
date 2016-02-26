package uk.org.richardjarvis.processor.text;

import au.com.bytecode.opencsv.CSVReader;
import org.slf4j.LoggerFactory;
import uk.org.richardjarvis.metadata.CSVProperties;
import uk.org.richardjarvis.metadata.FieldProperties;
import uk.org.richardjarvis.metadata.TabularMetaData;
import uk.org.richardjarvis.processor.ProcessorInterface;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by rjarvis on 24/02/16.
 */
public class TextProcessor implements ProcessorInterface {

    private static org.slf4j.Logger LOGGER = LoggerFactory.getLogger(TextProcessor.class);
    public static int MAX_ROWS_TO_PROCESS = 100;

    @Override
    public TabularMetaData process(InputStream inputStream, OutputStream outputStream) throws IOException {


        CSVProperties csvProperties = new CSVProperties(inputStream);
        LOGGER.info(csvProperties.toString());


        BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));

        CSVReader csvReader = getCSVReader(reader, csvProperties);

        List<FieldProperties> fieldPropertiesList = getFieldList(csvReader);

        // detect header


        // guess field type for each column

        // check column distribution

        // derive additional columns (J-Hot, EventCube)

        // check column correlations (RDC)
        TabularMetaData tabularMetaData = new TabularMetaData(csvProperties, fieldPropertiesList);


        return tabularMetaData;


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

    public List<FieldProperties> getFieldList(CSVReader csvReader) throws IOException {

        String[] firstRowFields = csvReader.readNext();
        List<FieldProperties> firstRowFieldPropertiesList = getRow(firstRowFields);   // store the first row separately
        List<FieldProperties> bulkFieldPropertiesList = getRow(csvReader.readNext());           // initialise from the second row onward

        buildColumnProperties(bulkFieldPropertiesList, csvReader);

        List<FieldProperties> totalFields;

        if (detectHeaderRow(firstRowFieldPropertiesList, bulkFieldPropertiesList)) {

            totalFields = mergeHeader(bulkFieldPropertiesList, firstRowFields);

        } else {

            // No header row so join first row into rest of table
            totalFields = updateProperties(bulkFieldPropertiesList, firstRowFields);

        }

        return totalFields;
    }

    private List<FieldProperties> mergeHeader(List<FieldProperties> propertiesList, String[] firstRowFields) {

        for (int fieldIndex = 0; fieldIndex < propertiesList.size(); fieldIndex++) {

            propertiesList.get(fieldIndex).setName(firstRowFields[fieldIndex]);

        }
        return propertiesList;
    }

    private boolean detectHeaderRow(List<FieldProperties> firstRowFieldPropertiesList, List<FieldProperties> bulkFieldPropertiesList) {

        boolean hasHeaderRow = false;

        for (int fieldIndex = 0; fieldIndex < firstRowFieldPropertiesList.size(); fieldIndex++) {
            FieldProperties firstRow = firstRowFieldPropertiesList.get(fieldIndex);
            FieldProperties remainingRows = bulkFieldPropertiesList.get(fieldIndex);

            if (firstRow.isString() && !remainingRows.isString())
                hasHeaderRow = true;

            if (firstRow.getMaxLength() > remainingRows.getMaxLength())
                hasHeaderRow = true;

            if (firstRow.getMinLength() < remainingRows.getMinLength())
                hasHeaderRow = true;

        }

        return hasHeaderRow;
    }

    private boolean buildColumnProperties(List<FieldProperties> propertiesList, CSVReader reader) throws IOException {

        for (int row = 0; row < TextProcessor.MAX_ROWS_TO_PROCESS; row++) {

            String[] fields = reader.readNext();

            if (fields == null)
                break;

            updateProperties(propertiesList, fields);
        }

        return true;
    }

    private List<FieldProperties> updateProperties(List<FieldProperties> propertiesList, String[] fields) {

        for (int fieldIndex = 0; fieldIndex < fields.length; fieldIndex++) {
            propertiesList.get(fieldIndex).update(fields[fieldIndex]);
        }

        return propertiesList;
    }

    private List<FieldProperties> getRow(String[] fields) {

        List<FieldProperties> fieldProperties = new ArrayList<>();

        for (int fieldIndex = 0; fieldIndex < fields.length; fieldIndex++) {

            fieldProperties.add(new FieldProperties(fields[fieldIndex]));

        }

        return fieldProperties;

    }

}