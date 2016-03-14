package uk.org.richardjarvis.utils;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.types.DataType;
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

    public static final String CHANNEL_METADATA_KEY = "Channel";

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

    public static boolean isNumericType(DataType dataType) {
        return (dataType.sameType(DataTypes.DoubleType) ||
                dataType.sameType(DataTypes.IntegerType) ||
                dataType.sameType(DataTypes.FloatType) ||
                dataType.sameType(DataTypes.ShortType) ||
                dataType.sameType(DataTypes.LongType));

    }

    public static boolean isNumericType(StructField dataType) {

        return isNumericType(dataType.dataType());
    }

    public static boolean isStringType(StructField dataType) {

        return (dataType.dataType().sameType(DataTypes.StringType) &&
                !dataType.metadata().contains(FieldProperties.DATE_FORMAT_METADATA));

    }

    public static Type getType(StructField dataType) {

        if (isNumericType(dataType))
            return Type.NUMERIC;
        if (isDateType(dataType))
            return Type.DATE;
        if (isStringType(dataType))
            return Type.STRING;
        if (isAudioWaveForm(dataType))
            return Type.AUDIO_WAVEFORM;

        return Type.UNKNOWN;
    }

    private static boolean isAudioWaveForm(StructField dataType) {
        return dataType.metadata().contains(CHANNEL_METADATA_KEY);
    }

    public static boolean isDateType(StructField dataType) {
        return (dataType.dataType().sameType(DataTypes.DateType));
    }

    public static List<String> getAudioColumnsNames(DataFrame input) {
        return getColumnsNames(getTypeOfColumns(input, Type.AUDIO_WAVEFORM));
    }

    public static List<Column> getAudioWaveformColumns(DataFrame input) {
        return getTypeOfColumns(input, Type.AUDIO_WAVEFORM);
    }

    public enum Type {AUDIO_WAVEFORM, NUMERIC, STRING, UNKNOWN, DATE}

}
