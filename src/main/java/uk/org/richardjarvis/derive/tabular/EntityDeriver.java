package uk.org.richardjarvis.derive.tabular;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import uk.org.richardjarvis.metadata.Statistics;
import uk.org.richardjarvis.metadata.TabularMetaData;
import uk.org.richardjarvis.utils.DataFrameUtils;
import uk.org.richardjarvis.utils.nlp.OpenNLPEntityExtractor;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Created by rjarvis on 15/03/16.
 */
public class EntityDeriver implements TabularDeriveInterface {

    private static final int NUMBER_OF_ENTITY_CATEGORIES = 5;

    @Override
    public DataFrame derive(DataFrame input, TabularMetaData metaData) {

        List<StructField> newColumns = new ArrayList<>();

        newColumns.addAll(Arrays.asList(input.schema().fields()));

        List<String> stringColumns = DataFrameUtils.getStringColumnsNames(input);

        for (String column : stringColumns) {
            newColumns.addAll(getFields(column));
        }

        StructType newSchema = new StructType(newColumns.toArray(new StructField[0]));

        int originalFieldCount = input.schema().fieldNames().length;
        int newFieldCount = originalFieldCount + stringColumns.size() * OpenNLPEntityExtractor.MODEL_NAMES.size() * NUMBER_OF_ENTITY_CATEGORIES;

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

                for (String modelName : OpenNLPEntityExtractor.MODEL_NAMES) {

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

        for (String modelName : OpenNLPEntityExtractor.MODEL_NAMES) {
            for (int i = 0; i < NUMBER_OF_ENTITY_CATEGORIES; i++) {
                fields.add(new StructField(fieldName + "_" + modelName + "_" + i, DataTypes.StringType, false, Metadata.empty()));
            }
        }
        return fields;

    }

}