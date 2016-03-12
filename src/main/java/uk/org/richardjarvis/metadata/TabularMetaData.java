package uk.org.richardjarvis.metadata;

import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import uk.org.richardjarvis.utils.DataFrameUtils;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by rjarvis on 26/02/16.
 */
public class TabularMetaData  implements MetaData{

    private List<FieldProperties> fieldPropertiesList;
    private CSVProperties csvProperties;
    private String name;
    private boolean hasHeader;
    private Statistics statistics=new Statistics();
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
        this.csvProperties=csvProperties;
        this.fieldPropertiesList=fieldPropertiesList;
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
        if (fieldPropertiesList==null)
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

        for (int fieldIndex=0; fieldIndex< fields.length; fieldIndex++) {
            fields[fieldIndex] = fieldPropertiesList.get(fieldIndex).getStructField();
        }

        return new StructType(fields);
    }

    public List<FieldProperties> getRawDateFields() {

        List<FieldProperties> result = new ArrayList<>();

        for (FieldProperties fieldProperties : this.fieldPropertiesList) {
            if (fieldProperties.getType()== DataTypes.DateType)
                result.add(fieldProperties);

        }
        return result;

    }

    public List<FieldProperties> getNumericFields() {

        List<FieldProperties> result = new ArrayList<>();

        for (FieldProperties fieldProperties : this.fieldPropertiesList) {
            if (DataFrameUtils.isNumericType(fieldProperties.getType()))
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
}
