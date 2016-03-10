package uk.org.richardjarvis.derive;

/**
 * Created by rjarvis on 29/02/16.
 */

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import uk.org.richardjarvis.utils.DataFrameUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

public class OneHotDeriver implements DeriveInterface {

    @Override
    public DataFrame derive(DataFrame input, Statistics statisticsMap) {

        List<StructField> newColumns = new ArrayList<>();

        newColumns.addAll(Arrays.asList(input.schema().fields()));

        List<String> stringColumns = DataFrameUtils.getStringColumnsNames(input);

        for (String column : stringColumns) {
            newColumns.addAll(getFields(column, statisticsMap.get(column)));
        }

        StructType newSchema = new StructType(newColumns.toArray(new StructField[0]));


        int originalFieldCount = input.schema().fieldNames().length;
        int newFieldCount = newColumns.size();

        JavaRDD<Row> rows = input.javaRDD().map(row -> {

            Object[] outputRow = new Object[newFieldCount];

            int fieldIndex = 0;
            while (fieldIndex < originalFieldCount) {
                outputRow[fieldIndex] = row.get(fieldIndex);
                fieldIndex++;
            }

            for (String stringColumn : stringColumns) {

                List<String> oneHotCols = statisticsMap.get(stringColumn).getFrequencyList();

                String value = row.getString(row.fieldIndex(stringColumn));

                int index = oneHotCols.indexOf(value);

                index = (index == -1) ? oneHotCols.size()-1 : index;

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

        return input.sqlContext().createDataFrame(rows, newSchema);
    }

    private List<StructField> getFields(String fieldName, FieldStatistics statistics) {

        List<StructField> fields = new ArrayList<>();

        for (String columnName : statistics.getFrequencyList()) {
            fields.add(new StructField(fieldName + "=" + columnName, DataTypes.IntegerType, false, Metadata.empty()));
        }

        return fields;

    }

//    }
}
