package uk.org.richardjarvis.derive.tabular;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.*;
import uk.org.richardjarvis.metadata.*;
import uk.org.richardjarvis.utils.DataFrameUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * One-hot encoder for categorical fields. Only includes top n categories and buckets remainder into _OTHER_ column
 */
public class OneHotDeriver implements TabularDeriveInterface {

    /**
     *
     * @param input the input dataframe
     * @param metaData the metadata that describes the input dataframe
     * @return an enriched DataFrame containing OneHot encoded columns for categorical variables
     */
    @Override
    public DataFrame derive(DataFrame input, TabularMetaData metaData) {

        List<String> stringColumns = DataFrameUtils.getColumnsNames(DataFrameUtils.getColumnsOfMeaning(input, FieldMeaning.MeaningType.TEXT));

        if (stringColumns.size() == 0)
            return input;

        StructType updatedSchema = getUpdatedSchema(input, metaData);

        Statistics statistics = metaData.getStatistics();
        int originalFieldCount = input.schema().fieldNames().length;
        int newFieldCount = updatedSchema.size();

        JavaRDD<Row> rows = input.javaRDD().map(row -> {

            Object[] outputRow = new Object[newFieldCount];

            int fieldIndex = 0;
            while (fieldIndex < originalFieldCount) {
                outputRow[fieldIndex] = row.get(fieldIndex);
                fieldIndex++;
            }

            for (String stringColumn : stringColumns) {

                List<String> oneHotCols = statistics.get(stringColumn).getFrequencyList();

                String value = row.getString(row.fieldIndex(stringColumn));

                int index = oneHotCols.indexOf(value);

                index = (index == -1) ? oneHotCols.size() - 1 : index;

                for (int i = 0; i < oneHotCols.size(); i++) {

                    if (i == index) {
                        outputRow[fieldIndex] = 1;
                    } else {
                        outputRow[fieldIndex] = 0;
                    }
                    fieldIndex++;
                }
            }
            return RowFactory.create(outputRow);

        });

        return input.sqlContext().createDataFrame(rows, updatedSchema);
    }


    private StructType getUpdatedSchema(DataFrame input, TabularMetaData metaData) {

        List<StructField> newColumns = new ArrayList<>();

        newColumns.addAll(Arrays.asList(input.schema().fields()));

        List<String> stringColumns = DataFrameUtils.getColumnsNames(DataFrameUtils.getColumnsOfMeaning(input, FieldMeaning.MeaningType.TEXT));

        for (String column : stringColumns) {
            newColumns.addAll(getFields(column, metaData.getStatistics().get(column)));
        }

        return new StructType(newColumns.toArray(new StructField[0]));
    }

    private List<StructField> getFields(String fieldName, FieldStatistics statistics) {

        List<StructField> fields = new ArrayList<>();

        for (String columnName : statistics.getFrequencyList()) {
            Metadata metadata = DataFrameUtils.getMetadata(FieldMeaning.MeaningType.ONE_HOT, null);
            fields.add(new StructField(fieldName + "=" + columnName, DataTypes.IntegerType, false, metadata));
        }

        return fields;

    }

}
