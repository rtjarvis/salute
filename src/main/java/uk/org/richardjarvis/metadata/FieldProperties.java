package uk.org.richardjarvis.metadata;

import org.apache.spark.sql.types.*;

import java.io.Serializable;
import java.text.Format;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * Created by rjarvis on 24/02/16.
 */
public class FieldProperties implements MetaData, Serializable {

    public static final String DATE_FORMAT_METADATA = "dataFormat";
    private String name;
    private int maxLength = Integer.MIN_VALUE;
    private int minLength = Integer.MAX_VALUE;
    private int count = 0;
    private long totalChars;
    private Boolean isNullable = true;
    private List<String> possibleDateTimeFormats;

    public Boolean getNullable() {
        return isNullable;
    }

    private DataType type;

    public FieldProperties() {

        possibleDateTimeFormats = new ArrayList<>();
        possibleDateTimeFormats.add("yyyy.MM.dd G 'at' HH:mm:ss z");    // 	2001.07.04 AD at 12:08:56 PDT
        possibleDateTimeFormats.add("EEE, MMM d, ''yy");                // 	Wed, Jul 4, '01
        possibleDateTimeFormats.add("h:mm a");                          // 	12:08 PM
        possibleDateTimeFormats.add("hh 'o''clock' a, zzzz");           // 	12 o'clock PM, Pacific Daylight Time
        possibleDateTimeFormats.add("K:mm a, z");                       // 	0:08 PM, PDT
        possibleDateTimeFormats.add("yyyyy.MMMMM.dd GGG hh:mm a");      // 	02001.July.04 AD 12:08 PM
        possibleDateTimeFormats.add("EEE, d MMM yyyy HH:mm:ss Z");      // 	Wed, 4 Jul 2001 12:08:56 -0700
        possibleDateTimeFormats.add("yyMMddHHmmssZ");                   // 	010704120856-0700
        possibleDateTimeFormats.add("yyyy-MM-dd'T'HH:mm:ss.SSSZ");      // 	2001-07-04T12:08:56.235-0700
        possibleDateTimeFormats.add("yyyy-MM-dd'T'HH:mm:ss.SSSXXX");    // 	2001-07-04T12:08:56.235-07:00
        possibleDateTimeFormats.add("YYYY-'W'ww-u");                    //  2001-W27-3
        possibleDateTimeFormats.add("dd/MM/yy HH:mm");                  //  28/04/16 15:30
        possibleDateTimeFormats.add("dd/MM/yy HH:mm:ss");               //  28/04/16 15:30:45
    }

    public FieldProperties(String value) {
        this();
        update(value);
    }

    public DataType getType() {

        return type;
    }

    @Override
    public String toString() {
        return "FieldProperties{" +
                "name='" + name + '\'' +
                ", type=" + type.toString() +
                ", minLength=" + minLength +
                ", maxLength=" + maxLength +
                '}';
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getMaxLength() {
        return maxLength;
    }

    public double getMeanLength() {
        return (double) totalChars / count;
    }

    public int getMinLength() {
        return minLength;
    }

    public void update(String value) {

        count++;
        totalChars += value.length();

        if (value.length() > maxLength)
            maxLength = value.length();

        if (value.length() < minLength)
            minLength = value.length();

        if (type == null) {

            if (value.matches("^[0-9]*$")) {
                type = DataTypes.IntegerType;
            } else if (value.matches("^[-+]*[0-9]*\\.[0-9]+$")) {
                type = DataTypes.DoubleType;
            } else if (couldBeDate(value)) {
                type = DataTypes.DateType;
            } else {
                type = DataTypes.StringType;
            }
        }

    }

    public Object convertToType(String value) {
//        switch (type) {
//            case DataTypes.DoubleType:
//                return Double.parseDouble(value);
//            case INTEGER:
//                return Integer.parseInt(value);
//            case STRING:
//                return value;
//            default:
        return value;
//        }
    }

    public StructField getStructField() {

        DataType type = getType();
        Metadata metadata = Metadata.empty();
        if (type == DataTypes.DateType) {// treat dates as Strings for now. Use deriver to format into well formatted dates
            metadata = new MetadataBuilder().putString(DATE_FORMAT_METADATA, getDateFormat()).build();
            type = DataTypes.StringType;
        }

        return new StructField(getName(), type, getNullable(), metadata);

    }

    private boolean couldBeDate(String possibleDate) {

        if (this.possibleDateTimeFormats.size() == 0)
            return false;

        LocalDate date = null;
        List<String> updatedPossibleDateTimeFormats = new ArrayList<>();

        for (String dateTimeFormatter : this.possibleDateTimeFormats) {
            boolean isParseable = true;
            try {
                date = LocalDate.parse(possibleDate, DateTimeFormatter.ofPattern(dateTimeFormatter));
            } catch (DateTimeParseException exc) {
                isParseable = false;
            }
            if (isParseable)
                updatedPossibleDateTimeFormats.add(dateTimeFormatter);

        }

        this.possibleDateTimeFormats = updatedPossibleDateTimeFormats;

        return this.possibleDateTimeFormats.size() > 0;

    }

    public String getDateFormat() {
        return possibleDateTimeFormats.get(0);
    }

    public DateTimeFormatter getDateFormattter() {

        DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern(getDateFormat()).withZone(getZone());
        return dateTimeFormatter;
    }

    public ZoneId getZone() {
        return ZoneId.systemDefault();
    }
}
