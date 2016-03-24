package uk.org.richardjarvis.derive.tabular;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.*;
import uk.org.richardjarvis.metadata.FieldMeaning;
import uk.org.richardjarvis.metadata.TabularMetaData;
import uk.org.richardjarvis.utils.DataFrameUtils;
import uk.org.richardjarvis.utils.nlp.OpenNLPEntityExtractor;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Extracts entites from text fields
 */
public class EntityDeriver implements TabularDeriveInterface {

    private static final int NUMBER_OF_ENTITY_CATEGORIES = 5;

    /**
     *
     * @param input the input dataframe
     * @param metaData the metadata that describes the input dataframe
     * @return an enriched DataFrame with the top n (default:5) entites extracted from String columns
     */
    @Override
    public DataFrame derive(DataFrame input, TabularMetaData metaData) {

        List<String> stringColumns = DataFrameUtils.getColumnsNames(DataFrameUtils.getColumnsOfMeaning(input, FieldMeaning.MeaningType.TEXT));

        if (stringColumns.size()==0)
            return input;

        StructType newSchema = getUpdatedSchema(input, metaData);

        int originalFieldCount = input.schema().fieldNames().length;
        int newFieldCount = originalFieldCount + stringColumns.size() * OpenNLPEntityExtractor.MODELS.keySet().size() * NUMBER_OF_ENTITY_CATEGORIES;

        JavaRDD<Row> rows = input.javaRDD().map(row -> {

            Object[] outputRow = new Object[newFieldCount];

            int fieldIndex = 0;
            while (fieldIndex < originalFieldCount) {
                outputRow[fieldIndex] = row.get(fieldIndex);
                fieldIndex++;
            }

            for (String stringColumn : stringColumns) {

                String value = row.getString(row.fieldIndex(stringColumn));

                Map<String, List<String>> entities = OpenNLPEntityExtractor.getResults(value);

                for (String modelName : OpenNLPEntityExtractor.MODELS.keySet()) {

                    List<String> modelEntities = entities.get(modelName);

                    for (int i = 0; i < NUMBER_OF_ENTITY_CATEGORIES; i++) {

                        if (modelEntities == null) {
                            outputRow[fieldIndex++] = null;
                        } else {

                            if (modelEntities.size() <= i) {
                                outputRow[fieldIndex++] = null;
                            } else {
                                String entity = modelEntities.get(i);
                                outputRow[fieldIndex++] = entity;
                            }
                        }
                    }
                }
            }

            return RowFactory.create(outputRow);

        });

        return input.sqlContext().createDataFrame(rows, newSchema);
    }

    private List<StructField> getFields(String fieldName) {

        List<StructField> fields = new ArrayList<>();

        for (String modelName : OpenNLPEntityExtractor.MODELS.keySet()) {
            for (int i = 0; i < NUMBER_OF_ENTITY_CATEGORIES; i++) {
                Metadata metadata = DataFrameUtils.getMetadata(OpenNLPEntityExtractor.MODELS.get(modelName),null);
                fields.add(new StructField(fieldName + "_entity_" + modelName + "_" + i, DataTypes.StringType, false, metadata));
            }
        }
        return fields;

    }

    private StructType getUpdatedSchema(DataFrame input, TabularMetaData metaData) {

        List<StructField> newColumns = new ArrayList<>();

        newColumns.addAll(Arrays.asList(input.schema().fields()));

        List<String> stringColumns = DataFrameUtils.getColumnsNames(DataFrameUtils.getColumnsOfMeaning(input, FieldMeaning.MeaningType.TEXT));

        for (String column : stringColumns) {
            newColumns.addAll(getFields(column));
        }

        return new StructType(newColumns.toArray(new StructField[0]));
    }

}
