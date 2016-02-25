package uk.org.richardjarvis.processor.text;

import au.com.bytecode.opencsv.CSVReader;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.*;

/**
 * Created by rjarvis on 24/02/16.
 */
public class DataProperties {

    private static org.slf4j.Logger LOGGER = LoggerFactory.getLogger(DataProperties.class);

    private List<FieldProperties> bulkFieldPropertiesList;
    private List<FieldProperties> firstRowFieldPropertiesList;

    private String[] firstRowFields;
    private boolean hasHeaderRow = false;

    @Override
    public String toString() {

        StringBuilder sb = new StringBuilder();

        for (FieldProperties fieldProperties : getTotalFields()) {

            sb.append(fieldProperties.toString() + "\n");
        }

        return sb.toString();
    }

    public boolean hasHeaderRow() {
        return hasHeaderRow;
    }

    public List<FieldProperties> getTotalFields() {

        List<FieldProperties> totalFields;

        if (hasHeaderRow()) {

            totalFields = mergeHeader(this.bulkFieldPropertiesList, firstRowFields);

        } else {

            // No header row so join first row into rest of table
            totalFields = updateProperties(this.bulkFieldPropertiesList, firstRowFields);

        }

        return totalFields;
    }

    public DataProperties(CSVReader csvReader) throws IOException {

        firstRowFields = csvReader.readNext();
        firstRowFieldPropertiesList = getRow(firstRowFields);   // store the first row separately
        bulkFieldPropertiesList = getRow(csvReader.readNext());           // initialise from the second row onward

        buildColumnProperties(bulkFieldPropertiesList, csvReader);

        detectHeaderRow();

    }

    private List<FieldProperties> mergeHeader(List<FieldProperties> propertiesList, String[] firstRowFields) {

        for (int fieldIndex = 0; fieldIndex < propertiesList.size(); fieldIndex++) {

            propertiesList.get(fieldIndex).setName(firstRowFields[fieldIndex]);

        }
        return propertiesList;
    }

    private boolean detectHeaderRow() {

        for (int fieldIndex = 0; fieldIndex < getNumberOfFields(); fieldIndex++) {
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

    public int getNumberOfFields() {
        return firstRowFieldPropertiesList.size();
    }

}
