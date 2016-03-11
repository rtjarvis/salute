package uk.org.richardjarvis.utils;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import uk.org.richardjarvis.metadata.FieldProperties;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by rjarvis on 29/02/16.
 */
public class DataFrameUtils {

    private static List<Column> getTypeOfColumns(DataFrame input, Type type) {

        List<Column> matchingColumns = new ArrayList<>();

        StructType schema = input.schema();

        for (StructField field : schema.fields()) {

            if (getType(field) == type) {
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

    public static List<Column> getNumericColumns(DataFrame input) {
        return getTypeOfColumns(input, Type.NUMERIC);
    }

    public static List<String> getNumericColumnsNames(DataFrame input) {

        return getColumnsNames(getNumericColumns(input));

    }

    public static List<Column> getStringColumns(DataFrame input) {

        return getTypeOfColumns(input, Type.STRING);
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

    private static boolean isNumericType(StructField dataType) {

        return (dataType.dataType().sameType(DataTypes.DoubleType) ||
                dataType.dataType().sameType(DataTypes.IntegerType) ||
                dataType.dataType().sameType(DataTypes.FloatType) ||
                dataType.dataType().sameType(DataTypes.ShortType) ||
                dataType.dataType().sameType(DataTypes.LongType));

    }

    private static boolean isStringType(StructField dataType) {

        return (dataType.dataType().sameType(DataTypes.StringType) &&
                !dataType.metadata().contains(FieldProperties.DATE_FORMAT_METADATA));

    }

    private static Type getType(StructField dataType) {

        if (isNumericType(dataType))
            return Type.NUMERIC;
        if (isDateType(dataType))
            return Type.DATE;
        if (isStringType(dataType))
            return Type.STRING;

        return Type.UNKNOWN;
    }

    private static boolean isDateType(StructField dataType) {
        return (dataType.dataType().sameType(DataTypes.DateType));
    }

    private enum Type {NUMERIC, STRING, UNKNOWN, DATE}

}
