package uk.org.richardjarvis.derive.tabular;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import uk.org.richardjarvis.metadata.FieldMeaning;
import uk.org.richardjarvis.metadata.FieldStatistics;
import uk.org.richardjarvis.metadata.Statistics;
import uk.org.richardjarvis.metadata.TabularMetaData;
import uk.org.richardjarvis.utils.DataFrameUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Add the populatity of individual category values in relation to all category values
 */
public class CategoryPopularityDeriver implements TabularDeriveInterface {
    /**
     * @param input    the input dataframe
     * @param metaData the metadata that describes the input dataframe
     * @return an enriched dataframe with an additional column per categorical variable containing the fraction of all distinct values that match this categorical variable
     */
    @Override
    public DataFrame derive(DataFrame input, TabularMetaData metaData) {

        List<String> stringColumns = DataFrameUtils.getColumnsNames(DataFrameUtils.getColumnsOfMeaning(input, FieldMeaning.MeaningType.TEXT));

        int originalFieldCount = input.schema().fieldNames().length;
        int stringFieldCount = stringColumns.size();
        int newFieldCount = originalFieldCount + stringFieldCount;

        Statistics statistics = metaData.getStatistics();

        JavaRDD<Row> output = input.javaRDD().map(row -> {

            Object[] outputRow = new Object[newFieldCount];

            for (int i = 0; i < originalFieldCount; i++) {
                outputRow[i] = row.get(i);
            }
            for (int i = 0; i < stringFieldCount; i++) {
                String columnName = stringColumns.get(i);
                FieldStatistics fieldStatistics = statistics.get(columnName);

                String value = row.getString(row.fieldIndex(columnName));

                outputRow[originalFieldCount + i] = fieldStatistics.getFrequency(value);
            }
            return RowFactory.create(outputRow);
        });

        return input.sqlContext().createDataFrame(output, getUpdatedSchema(input, metaData));
    }

    private StructType getUpdatedSchema(DataFrame input, TabularMetaData metaData) {

        List<StructField> newColumns = new ArrayList<>();
        newColumns.addAll(Arrays.asList(input.schema().fields()));

        List<String> stringColumns = DataFrameUtils.getColumnsNames(DataFrameUtils.getColumnsOfMeaning(input, FieldMeaning.MeaningType.TEXT));

        for (String name : stringColumns) {
            StructField field = new StructField(name + "_ratio", DataTypes.DoubleType, false, DataFrameUtils.getMetadata(FieldMeaning.MeaningType.NUMERIC, null));
            newColumns.add(field);
        }

        return new StructType(newColumns.toArray(new StructField[0]));
    }


}