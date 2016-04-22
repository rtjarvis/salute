package uk.org.richardjarvis.metadata;

import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import uk.org.richardjarvis.utils.DataFrameUtils;
import uk.org.richardjarvis.utils.file.FileUtils;
import uk.org.richardjarvis.utils.report.ReportUtil;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Created by rjarvis on 26/02/16.
 */
public class TabularMetaData implements MetaData {

    private List<FieldProperties> fieldPropertiesList;
    private CSVProperties csvProperties;
    private String name;
    private boolean hasHeader;
    private Statistics statistics = new Statistics();
    private String primaryTimeStampFieldName;

    public Statistics getStatistics() {
        return statistics;
    }

    public void setStatistics(Statistics statistics) {
        this.statistics = statistics;
    }

    public boolean hasHeader() {
        return hasHeader;
    }

    public void setHasHeader(boolean hasHeader) {
        this.hasHeader = hasHeader;
    }

    public TabularMetaData(CSVProperties csvProperties, List<FieldProperties> fieldPropertiesList) {
        this.csvProperties = csvProperties;
        this.fieldPropertiesList = fieldPropertiesList;
    }

    public CSVProperties getCsvProperties() {
        return csvProperties;
    }

    public void setCsvProperties(CSVProperties csvProperties) {
        this.csvProperties = csvProperties;
    }

    public List<FieldProperties> getFieldPropertiesList() {
        return fieldPropertiesList;
    }

    public void setFieldPropertiesList(List<FieldProperties> fieldPropertiesList) {
        this.fieldPropertiesList = fieldPropertiesList;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public FieldProperties getFieldProperty(int index) {
        if (fieldPropertiesList == null)
            return null;

        return fieldPropertiesList.get(index);
    }

    @Override
    public String toString() {

        StringBuilder sb = new StringBuilder();

        for (FieldProperties fieldProperties : fieldPropertiesList) {

            sb.append(fieldProperties.toString() + "\n");
        }

        return sb.toString();
    }

    public StructType getStructType() {

        StructField[] fields = new StructField[fieldPropertiesList.size()];

        for (int fieldIndex = 0; fieldIndex < fields.length; fieldIndex++) {
            fields[fieldIndex] = fieldPropertiesList.get(fieldIndex).getStructField();
        }

        return new StructType(fields);
    }

    public List<FieldProperties> getRawDateFields() {

        List<FieldProperties> result = new ArrayList<>();

        for (FieldProperties fieldProperties : this.fieldPropertiesList) {
            if (fieldProperties.getMeaning().getType() == DataTypes.DateType)
                result.add(fieldProperties);

        }
        return result;

    }

    public List<FieldProperties> getNumericFields() {

        List<FieldProperties> result = new ArrayList<>();

        for (FieldProperties fieldProperties : this.fieldPropertiesList) {
            if (fieldProperties.getMeaning().getMeaningType() == FieldMeaning.MeaningType.NUMERIC)
                result.add(fieldProperties);

        }
        return result;

    }

    public String getPrimaryTimeStampFieldName() {
        return primaryTimeStampFieldName;
    }

    public void setPrimaryTimeStampFieldName(String primaryTimeStampFieldName) {
        this.primaryTimeStampFieldName = primaryTimeStampFieldName;
    }

    @Override
    public void generateReport(String inputPath, DataFrame dataFrame, String outputPath) {

        Map<String, FieldMeaning> fieldProperties = new HashMap<>();

        this.fieldPropertiesList.stream().forEach(fp -> fieldProperties.put(fp.getName(), fp.getMeaning()));

        StringBuilder sb = new StringBuilder();

        ReportUtil.addHTMLHeader(sb, "Text File Report");
        ReportUtil.addBodyStart(sb);
        ReportUtil.addLinkToFile(sb, inputPath);
        ReportUtil.addHTMLTable(sb, "Field Properties", fieldProperties);
        ReportUtil.addHTMLTable(sb, "Sample Data", dataFrame, 10);
        ReportUtil.addBodyEnd(sb);
        ReportUtil.addHTMLFooter(sb);

        try {
            FileUtils.writeBufferToFile(sb, outputPath);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


}
