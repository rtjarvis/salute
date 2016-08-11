package uk.org.richardjarvis.utils;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.*;
import uk.org.richardjarvis.metadata.text.FieldMeaning;

import java.util.ArrayList;
import java.util.List;

/**
 * Utils to return information about a DataFrame
 */
public class DataFrameUtils {

    public static final String CHANNEL_METADATA_KEY = "Channel";
    public static final String FORMAT_METADATA = "format";
    public static final String MEANING_METADATA = "meaning";

    /**
     * @param input the DataFrame to process
     * @param type  the type of Meaning of the columns to be returned
     * @return list of columns that match the meaning requested
     */
    public static List<Column> getColumnsOfMeaning(DataFrame input, FieldMeaning.MeaningType type) {

        List<Column> matchingColumns = new ArrayList<>();

        for (StructField field : input.schema().fields()) {
            FieldMeaning.MeaningType fieldType = getFieldMeaning(field).getMeaningType();
            if (fieldType.equals(type)) {
                String f = field.name();
                Column col = input.col(f);
                matchingColumns.add(col);
            }
        }

        return matchingColumns;
    }

    private static FieldMeaning getFieldMeaning(StructField field) {
        if (!field.metadata().contains(MEANING_METADATA))
            return new FieldMeaning(FieldMeaning.MeaningType.TEXT, null, field.dataType());

        String format = (field.metadata().contains(FORMAT_METADATA)) ? field.metadata().getString(FORMAT_METADATA) : null;

        return new FieldMeaning(FieldMeaning.MeaningType.valueOf(field.metadata().getString(MEANING_METADATA)), format, field.dataType());
    }

    public static List<String> getColumnsNames(List<Column> columns) {
        List<String> columnNames = new ArrayList<>(columns.size());

        for (Column column : columns) {
            columnNames.add(column.expr().prettyString());
        }

        return columnNames;
    }

    public static Metadata getMetadata(FieldMeaning.MeaningType meaningType, String format) {

        MetadataBuilder metadataBuilder = new MetadataBuilder();
        if (format != null)
            metadataBuilder.putString(FORMAT_METADATA, format);

        if (meaningType != null)
            metadataBuilder.putString(MEANING_METADATA, meaningType.name());
        return metadataBuilder.build();
    }

    public static StructType unNestColumns(StructType schema) {
        return new StructType(unNestColumns("", schema).toArray(new StructField[0]));
    }

    private static List<StructField> unNestColumns(String prefix, StructType schema) {

        List<StructField> colNames = new ArrayList<>();

        for (StructField field : schema.fields()) {
            String name = ((prefix.length() > 0) ? prefix + "." : "") + field.name();

            if (field.dataType().typeName().equals("struct")) {
                colNames.addAll(unNestColumns(name, (StructType) field.dataType()));
            } else {
                colNames.add(new StructField(name, field.dataType(), field.nullable(), field.metadata()));
            }

        }
        return colNames;

    }

    public static Row unNestData(Row row) {
        return RowFactory.create(unNestDataToList(row).toArray(new Object[0]));
    }

    private static List<Object> unNestDataToList(Row row) {

        List<Object> data = new ArrayList<>();

        for (int i = 0; i < row.size(); i++) {

            Object cell = row.get(i);
            if (cell instanceof Row) {
                data.addAll(unNestDataToList((Row) cell));
            } else {
                data.add(cell);
            }

        }

        return data;
    }

    public static List<Row> getSampleHead(DataFrame df, int rowCount) {

        final int maxLength = 50;
        final String ellipses = "...";

        List<Row> rows = df.limit(rowCount).javaRDD().map(row -> {
                    Object[] o = new Object[row.size()];

                    for (int i = 0; i < row.size(); i++) {
                        Object obj = row.get(i);
                        if (obj == null) {
                            o[i] = null;
                        } else {
                            String value = obj.toString();

                            if (value.length() > maxLength) {
                                value = value.substring(0, maxLength - ellipses.length() - 1) + ellipses;
                            }
                            o[i] = value;
                        }
                    }
                    return RowFactory.create(o);

                }).collect();

        return rows;

    }
}