package uk.org.richardjarvis.utils;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by rjarvis on 29/02/16.
 */
public class DataFrameUtils {
    public static List<Column> getNumericColumns(DataFrame input) {

        List<Column> numericColumns = new ArrayList<>();

        StructType schema = input.schema();

        for (StructField field : schema.fields()) {

            if (isNumericType(field.dataType())) {
                numericColumns.add(input.col(field.name()));
            }
        }

        return numericColumns;
    }

    private static boolean isNumericType(DataType dataType) {

        return (dataType.sameType(DataTypes.DoubleType) ||
                dataType.sameType(DataTypes.IntegerType) ||
                dataType.sameType(DataTypes.FloatType) ||
                dataType.sameType(DataTypes.ShortType) ||
                dataType.sameType(DataTypes.LongType));

    }
}
