package uk.org.richardjarvis.metadata;

import java.util.List;

/**
 * Created by rjarvis on 26/02/16.
 */
public class TabularMetaData  implements MetaData{

    private List<FieldProperties> fieldPropertiesList;
    private CSVProperties csvProperties;
    private String name;

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

    @Override
    public String toString() {

        StringBuilder sb = new StringBuilder();

        for (FieldProperties fieldProperties : fieldPropertiesList) {

            sb.append(fieldProperties.toString() + "\n");
        }

        return sb.toString();
    }

}
