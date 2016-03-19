package uk.org.richardjarvis.derive.tabular;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.*;
import uk.org.richardjarvis.metadata.FieldMeaning;
import uk.org.richardjarvis.metadata.FieldProperties;
import uk.org.richardjarvis.metadata.TabularMetaData;
import uk.org.richardjarvis.utils.field.Recogniser;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

/**
 * Created by rjarvis on 29/02/16.
 */
public class DateFormatDeriver implements TabularDeriveInterface {

    private HashMap<String, DataType> timePeriods;

    public DateFormatDeriver() {

        timePeriods = new LinkedHashMap<>();
        timePeriods.put("Second", DataTypes.IntegerType);
        timePeriods.put("Minute", DataTypes.IntegerType);
        timePeriods.put("Hour", DataTypes.IntegerType);
        timePeriods.put("DayOfWeek", DataTypes.StringType);
        timePeriods.put("DayOfMonth", DataTypes.IntegerType);
        timePeriods.put("DayOfYear", DataTypes.IntegerType);
        timePeriods.put("Month", DataTypes.StringType);
        timePeriods.put("Year", DataTypes.IntegerType);
        timePeriods.put("EpochTime", DataTypes.LongType);

    }

    @Override
    public DataFrame derive(DataFrame input, TabularMetaData metaData) {

        List<FieldProperties> columns = metaData.getRawDateFields();

        StructType updatedSchema = getUpdatedSchema(input, metaData);

        if (columns.size() == 0)
            return input;

        int originalFieldCount = input.schema().fieldNames().length;
        int additionalFieldCount = columns.size();
        int newFieldCount = updatedSchema.size();

        JavaRDD<Row> output = input.javaRDD().map(row -> {

            Object[] outputRow = new Object[newFieldCount];

            int fieldIndex = 0;
            for (int i = 0; i < originalFieldCount; i++) {
                outputRow[fieldIndex++] = row.get(i);
            }

            for (int i = 0; i < additionalFieldCount; i++) {

                int columnIndex = row.fieldIndex(columns.get(i).getName());

                String value = row.getString(columnIndex);

                DateTimeFormatter formatter = columns.get(i).getMeaning().getDateFormattter();
                ZonedDateTime date = ZonedDateTime.parse(value, formatter);

                outputRow[fieldIndex++] = date.getSecond();
                outputRow[fieldIndex++] = date.getMinute();
                outputRow[fieldIndex++] = date.getHour();
                outputRow[fieldIndex++] = date.getDayOfWeek().toString();
                outputRow[fieldIndex++] = date.getDayOfMonth();
                outputRow[fieldIndex++] = date.getDayOfYear();
                outputRow[fieldIndex++] = date.getMonth().toString();
                outputRow[fieldIndex++] = date.getYear();
                outputRow[fieldIndex++] = date.toEpochSecond();

            }
            return RowFactory.create(outputRow);
        });

        metaData.setPrimaryTimeStampFieldName(getName(columns.get(0), "EpochTime"));

        return input.sqlContext().createDataFrame(output, updatedSchema);

    }

    private StructType getUpdatedSchema(DataFrame input, TabularMetaData metaData) {

        List<FieldProperties> columns = metaData.getRawDateFields();

        StructType structType = input.schema();
        for (FieldProperties fieldProperties : columns) {
            for (String timePeriod : timePeriods.keySet()) {

                MetadataBuilder metadataBuilder = new MetadataBuilder();
                metadataBuilder.putString(FieldProperties.MEANING_METADATA, FieldMeaning.MeaningType.DATE_PART.name());
                StructField field= new StructField(getName(fieldProperties, timePeriod), timePeriods.get(timePeriod), false, metadataBuilder.build());

                structType = structType.add(field);
            }
        }
        return structType;
    }

    private String getName(FieldProperties fieldProperties, String timePeriod) {
        return fieldProperties.getName() + "_" + timePeriod;
    }


}

