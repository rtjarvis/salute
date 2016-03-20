package uk.org.richardjarvis.utils;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.types.*;
import uk.org.richardjarvis.metadata.FieldMeaning;
import uk.org.richardjarvis.utils.field.Recogniser;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by rjarvis on 29/02/16.
 */
public class DataFrameUtils {

    public static final String CHANNEL_METADATA_KEY = "Channel";
    public static final String FORMAT_METADATA = "format";
    public static final String MEANING_METADATA = "meaning";

    public static List<Column> getColumnsOfMeaning(DataFrame input, FieldMeaning.MeaningType type) {

        List<Column> matchingColumns = new ArrayList<>();

        for (StructField field : input.schema().fields()) {
            FieldMeaning.MeaningType fieldType = getType(field);
            if (fieldType.equals(type)) {
                String f = field.name();
                Column col = input.col(f);
                matchingColumns.add(col);
            }
        }

        return matchingColumns;
    }

    private static FieldMeaning.MeaningType getType(StructField field) {
        if (!field.metadata().contains(MEANING_METADATA))
            return Recogniser.getDataTypeMeaning(field.dataType());
        return FieldMeaning.MeaningType.valueOf(field.metadata().getString(MEANING_METADATA));
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


}
