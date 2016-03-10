package uk.org.richardjarvis.derive;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import uk.org.richardjarvis.utils.DataFrameUtils;

import java.util.List;

/**
 * Created by rjarvis on 08/03/16.
 */
public class CategoryPopularityDeriver implements DeriveInterface {

    @Override
    public DataFrame derive(DataFrame input, Statistics statisticsMap) {

        List<String> stringColumns = DataFrameUtils.getStringColumnsNames(input);

        int originalFieldCount = input.schema().fieldNames().length;
        int stringFieldCount = stringColumns.size();
        int newFieldCount = originalFieldCount + stringFieldCount;

        JavaRDD<Row> output = input.javaRDD().map(row -> {

            Object[] outputRow = new Object[newFieldCount];

            for (int i = 0; i < originalFieldCount; i++) {
                outputRow[i] = row.get(i);
            }
            for (int i = 0; i < stringFieldCount; i++) {
                String columnName = stringColumns.get(i);
                FieldStatistics fieldStatistics = statisticsMap.get(columnName);

                String value = row.getString(row.fieldIndex(columnName));

                outputRow[originalFieldCount + i] = fieldStatistics.getFrequency(value);
            }
            return RowFactory.create(outputRow);
        });


        StructType structType = input.schema();
        for (String name : stringColumns) {
            structType = structType.add(name+"_ratio", DataTypes.DoubleType);
        }

        return input.sqlContext().createDataFrame(output, structType);
    }
}