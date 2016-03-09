package uk.org.richardjarvis.utils;

import org.apache.hadoop.yarn.webapp.hamlet.HamletSpec;
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

        List<Column> matchingColumns = new ArrayList<>();

        StructType schema = input.schema();

        for (StructField field : schema.fields()) {

            if (isNumericType(field.dataType())) {
                matchingColumns.add(input.col(field.name()));
            }
        }

        return matchingColumns;
    }

    public static List<String> getColumnsNames(List<Column> columns) {
        List<String> columnNames = new ArrayList<>(columns.size());

        for (Column column : columns) {
            columnNames.add(column.expr().prettyString());
        }

        return columnNames;
    }

    public static List<String> getNumericColumnsNames(DataFrame input) {

        return getColumnsNames(getNumericColumns(input));

    }

    public static List<Column> getStringColumns(DataFrame input) {

        List<Column> matchingColumns = new ArrayList<>();

        StructType schema = input.schema();

        for (StructField field : schema.fields()) {

            if (isStringType(field.dataType())) {
                matchingColumns.add(input.col(field.name()));
            }
        }

        return matchingColumns;
    }

    public static List<String> getStringColumnsNames(DataFrame input) {

        return getColumnsNames(getStringColumns(input));

    }

    public static List<Integer> getColumnIndexes(DataFrame input, List<Column> columns) {

        List<Integer> indexes = new ArrayList<>(columns.size());

        for (Column column : columns) {
            indexes.add(input.schema().fieldIndex(column.expr().prettyString()));
        }

        return indexes;

    }

    private static boolean isNumericType(DataType dataType) {

        return (dataType.sameType(DataTypes.DoubleType) ||
                dataType.sameType(DataTypes.IntegerType) ||
                dataType.sameType(DataTypes.FloatType) ||
                dataType.sameType(DataTypes.ShortType) ||
                dataType.sameType(DataTypes.LongType));

    }

    private static boolean isStringType(DataType dataType) {

        return (dataType.sameType(DataTypes.StringType));

    }
}
