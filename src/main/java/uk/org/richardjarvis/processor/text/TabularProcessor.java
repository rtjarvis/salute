package uk.org.richardjarvis.processor.text;

import au.com.bytecode.opencsv.CSVReader;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.slf4j.LoggerFactory;
import uk.org.richardjarvis.loader.LoaderConfig;
import uk.org.richardjarvis.metadata.text.CSVProperties;
import uk.org.richardjarvis.metadata.text.FieldMeaning;
import uk.org.richardjarvis.metadata.text.FieldProperties;
import uk.org.richardjarvis.metadata.MetaData;
import uk.org.richardjarvis.metadata.text.TabularMetaData;
import uk.org.richardjarvis.processor.ProcessorInterface;
import uk.org.richardjarvis.utils.SparkProvider;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by rjarvis on 24/02/16.
 */
public class TabularProcessor implements ProcessorInterface {

    public static int MAX_ROWS_TO_PROCESS = 100;
    private static org.slf4j.Logger LOGGER = LoggerFactory.getLogger(TabularProcessor.class);
    private LoaderConfig config;

    public TabularProcessor(LoaderConfig config) {
        this.config = config;
    }

    public TabularProcessor() {
    }

    @Override
    public TabularMetaData extractMetaData(String path) throws IOException {

        List<String> headRows = SparkProvider.getSparkContext().textFile(path).take(TabularProcessor.MAX_ROWS_TO_PROCESS);

        CSVProperties csvProperties = new CSVProperties(headRows, (config == null) ? null : config.getDelimiter(), (config == null) ? null : config.getStringEnclosure());

        List<CSVRecord> tableData = parseTable(headRows, csvProperties);

        TabularMetaData tabularMetaData = getTabularMetaData(tableData, csvProperties, (config == null) ? null : config.getHasHeader());

        return tabularMetaData;

    }

    @Override
    public DataFrame extractData(String path, MetaData metaData, SQLContext sqlContext) throws IOException {

        if (!(metaData instanceof TabularMetaData))
            return null;

        TabularMetaData tabularMetaData = (TabularMetaData) metaData;

        DataFrame df;
        if (tabularMetaData.getCsvProperties().getStringEnclosure() != null) {
            df = sqlContext.read()
                    .format("com.databricks.spark.csv")
                    .schema(tabularMetaData.getStructType())
                    .option("header", "" + tabularMetaData.hasHeader())
                    .option("delimiter", tabularMetaData.getCsvProperties().getDelimiter().toString())
                    .option("quote", tabularMetaData.getCsvProperties().getStringEnclosure().toString())
                    .load(path);
        } else {
            df = sqlContext.read()
                    .format("com.databricks.spark.csv")
                    .schema(tabularMetaData.getStructType())
                    .option("header", "" + tabularMetaData.hasHeader())
                    .option("delimiter", tabularMetaData.getCsvProperties().getDelimiter().toString())
                    .load(path);
        }

        return df;
    }

    private List<CSVRecord> parseTable(List<String> rows, CSVProperties csvProperties) throws IOException {

        StringBuilder sb = new StringBuilder();
        for (String row : rows) {
            sb.append(row);
            sb.append("\n");
        }

        return CSVParser.parse(sb.toString(), csvProperties.getCsvFormat()).getRecords();
    }

    private CSVReader getCSVReader(Reader reader, CSVProperties csvProperties) {

        if (csvProperties.getDelimiter() == null)
            return null;

        if (csvProperties.getStringEnclosure() != null) {
            return new CSVReader(reader, csvProperties.getDelimiter(), csvProperties.getStringEnclosure(), true);
        } else {
            return new CSVReader(reader, csvProperties.getDelimiter());
        }

    }

    private TabularMetaData getTabularMetaData(List<CSVRecord> rows, CSVProperties csvProperties, Boolean hasHeader) throws IOException {

        List<FieldProperties> firstRowFieldPropertiesList = getRow(rows.get(0));       // store the first row separately
        List<FieldProperties> bulkFieldPropertiesList = getRow(rows.get(1));           // initialise from the second row onward

        if (rows.size() > 2) {
            buildColumnProperties(bulkFieldPropertiesList, rows.subList(2, rows.size() - 1));
        }

        List<FieldProperties> totalFields;

        boolean hasHeaderRow;
        if (hasHeader != null) {
            hasHeaderRow = hasHeader;
        } else {
            hasHeaderRow = detectHeaderRow(firstRowFieldPropertiesList, bulkFieldPropertiesList);
        }
        if (hasHeaderRow) {

            totalFields = mergeHeader(bulkFieldPropertiesList, rows.get(0));

        } else {

            // No header row so join first row into rest of table
            totalFields = updateProperties(bulkFieldPropertiesList, rows.get(0));
            totalFields = addPseudoHeader(totalFields);

        }

        TabularMetaData tabularMetaData = new TabularMetaData(csvProperties, totalFields);
        tabularMetaData.setHasHeader(hasHeaderRow);

        return tabularMetaData;
    }

    private List<FieldProperties> mergeHeader(List<FieldProperties> propertiesList, CSVRecord firstRowFields) {

        for (int fieldIndex = 0; fieldIndex < propertiesList.size(); fieldIndex++) {

            propertiesList.get(fieldIndex).setName(firstRowFields.get(fieldIndex));

        }
        return propertiesList;
    }

    private List<FieldProperties> addPseudoHeader(List<FieldProperties> propertiesList) {

        for (int fieldIndex = 0; fieldIndex < propertiesList.size(); fieldIndex++) {

            propertiesList.get(fieldIndex).setName("Col_" + fieldIndex);

        }
        return propertiesList;
    }

    private boolean detectHeaderRow(List<FieldProperties> firstRowFieldPropertiesList, List<FieldProperties> bulkFieldPropertiesList) {

        boolean hasHeaderRow = false;

        List<String> namesSeen = new ArrayList<>();

        for (int fieldIndex = 0; fieldIndex < firstRowFieldPropertiesList.size(); fieldIndex++) {
            FieldProperties firstRow = firstRowFieldPropertiesList.get(fieldIndex);
            FieldProperties remainingRows = bulkFieldPropertiesList.get(fieldIndex);

            // if there content is repeated, unlikely to be a header
            if (namesSeen.contains(firstRow.getName())) return false;

            if (firstRow.getMeaning().getType() == DataTypes.StringType && (remainingRows.getMeaning().getType() != DataTypes.StringType || firstRow.equals(remainingRows)))
                hasHeaderRow = true;

            namesSeen.add(firstRow.getName());
        }

        return hasHeaderRow;
    }

    private boolean buildColumnProperties(List<FieldProperties> propertiesList, List<CSVRecord> tableData) throws IOException {

        for (CSVRecord csvRecord : tableData) {

            updateProperties(propertiesList, csvRecord);

        }

        return true;
    }

    private List<FieldProperties> updateProperties(List<FieldProperties> propertiesList, CSVRecord fields) {


        for (int fieldIndex = 0; fieldIndex < fields.size(); fieldIndex++) {
            propertiesList.get(fieldIndex).update(fields.get(fieldIndex));
        }

        return propertiesList;
    }

    private List<FieldProperties> getRow(CSVRecord row) throws IOException {

        List<FieldProperties> fieldProperties = new ArrayList<>();

        for (int fieldIndex = 0; fieldIndex < row.size(); fieldIndex++) {
            fieldProperties.add(new FieldProperties(row.get(fieldIndex)));
        }

        return fieldProperties;

    }

}