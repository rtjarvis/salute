package uk.org.richardjarvis.utils.field;

import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import scala.Tuple2;
import uk.org.richardjarvis.metadata.FieldMeaning;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.List;

import static uk.org.richardjarvis.metadata.FieldMeaning.MeaningType;

/**
 * Created by rjarvis on 18/03/16.
 */
public class Recogniser {

    private static List<String> possibleDateTimeFormats;
    private static List<Tuple2<String, DataType>> dataTypeRegex;
    private static List<Tuple2<String, FieldMeaning.MeaningType>> meaningTypeRegex;


    static {

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

        dataTypeRegex = new ArrayList<>();
        dataTypeRegex.add(new Tuple2<>("^[-+]*[0-9]*\\.[0-9]+$", DataTypes.DoubleType));
        dataTypeRegex.add(new Tuple2<>("^[0-9]*$", DataTypes.IntegerType));
        dataTypeRegex.add(new Tuple2<>("^(true|false|True|False|TRUE|FALSE|Yes|No|YES|NO|yes|no|Y|N)$", DataTypes.BooleanType));

        meaningTypeRegex = new ArrayList<>();
        meaningTypeRegex.add(new Tuple2<>("^(([0-9A-Fa-f]{2}[-:]){5}[0-9A-Fa-f]{2})|(([0-9A-Fa-f]{4}\\.){2}[0-9A-Fa-f]{4})$", MeaningType.MAC_ADDRESS));
        meaningTypeRegex.add(new Tuple2<>("^([a-zA-Z0-9_\\-\\.]+)@((\\[[0-9]{1,3}\\.[0-9]{1,3}\\.[0-9]{1,3}\\.)|(([a-zA-Z0-9\\-]+\\.)+))([a-zA-Z]{2,4}|[0-9]{1,3})(\\]?)$", MeaningType.EMAIL_ADDRESS));
        meaningTypeRegex.add(new Tuple2<>("^(25[0-5]|2[0-4][0-9]|[0-1]{1}[0-9]{2}|[1-9]{1}[0-9]{1}|[1-9])\\.(25[0-5]|2[0-4][0-9]|[0-1]{1}[0-9]{2}|[1-9]{1}[0-9]{1}|[1-9]|0)\\.(25[0-5]|2[0-4][0-9]|[0-1]{1}[0-9]{2}|[1-9]{1}[0-9]{1}|[1-9]|0)\\.(25[0-5]|2)$", MeaningType.IPv4));
        meaningTypeRegex.add(new Tuple2<>("^(^(([0-9A-F]{1,4}(((:[0-9A-F]{1,4}){5}::[0-9A-F]{1,4})|((:[0-9A-F]{1,4}){4}::[0-9A-F]{1,4}(:[0-9A-F]{1,4}){0,1})|((:[0-9A-F]{1,4}){3}::[0-9A-F]{1,4}(:[0-9A-F]{1,4}){0,2})|((:[0-9A-F]{1,4}){2}::[0-9A-F]{1,4}(:[0-9A-F]{1,4}){0,3})|(:[0-9A-F]{1,4}::[0-9A-F]{1,4}(:[0-9A-F]{1,4}){0,4})|(::[0-9A-F]{1,4}(:[0-9A-F]{1,4}){0,5})|(:[0-9A-F]{1,4}){7}))$|^(::[0-9A-F]{1,4}(:[0-9A-F]{1,4}){0,6})$)|^::$)|^((([0-9A-F]{1,4}(((:[0-9A-F]{1,4}){3}::([0-9A-F]{1,4}){1})|((:[0-9A-F]{1,4}){2}::[0-9A-F]{1,4}(:[0-9A-F]{1,4}){0,1})|((:[0-9A-F]{1,4}){1}::[0-9A-F]{1,4}(:[0-9A-F]{1,4}){0,2})|(::[0-9A-F]{1,4}(:[0-9A-F]{1,4}){0,3})|((:[0-9A-F]{1,4}){0,5})))|([:]{2}[0-9A-F]{1,4}(:[0-9A-F]{1,4}){0,4})):|::)((25[0-5]|2[0-4][0-9]|[0-1]?[0-9]{0,2})\\.){3}(25[0-5]|2[0-4][0-9]|[0-1]?[0-9]{0,2})$", MeaningType.IPv6));
        meaningTypeRegex.add(new Tuple2<>("^(http|ftp|https):\\/\\/[\\w\\-_]+(\\.[\\w\\-_]+)+([\\w\\-\\.,@?^=%&amp;:/~\\+#]*[\\w\\-\\@?^=%&amp;/~\\+#])?$", MeaningType.URL));

    }

    public static List<FieldMeaning> getPossibleMeanings(String value) {

        List<FieldMeaning> fieldMeanings = new ArrayList<>();

        List<DataType> possibleDataTypes = getPossibleDataTypes(value);
        List<String> possibleFormats = getPossibleDateFormats(value);

        for (DataType type : possibleDataTypes) {

            for (MeaningType meaningType : getPossibleFieldMeaningTypes(value)) {

                if (possibleFormats != null && possibleFormats.size() > 0) {
                    for (String format : possibleFormats) {
                        fieldMeanings.add(new FieldMeaning(meaningType, format, type));
                    }
                } else {
                    fieldMeanings.add(new FieldMeaning(meaningType, null, type));
                }

            }

            fieldMeanings.add(new FieldMeaning(getDataTypeMeaning(type), null, type));

        }

        return fieldMeanings;

    }

    public static MeaningType getDataTypeMeaning(DataType type) {
        if (type.sameType(DataTypes.ShortType) ||
                type.sameType(DataTypes.LongType) ||
                type.sameType(DataTypes.FloatType) ||
                type.sameType(DataTypes.DoubleType) ||
                type.sameType(DataTypes.IntegerType)) {

            return MeaningType.NUMERIC;
        }

        if (type.sameType(DataTypes.DateType))
            return MeaningType.DATE;

        if (type.sameType(DataTypes.BooleanType))
            return MeaningType.BOOLEAN;

        return MeaningType.TEXT;

    }
        private static List<DataType> getPossibleDataTypes (String value){

            List<DataType> types = new ArrayList<>();

            for (Tuple2<String, DataType> regex : dataTypeRegex) {
                if (value.matches(regex._1)) {
                    types.add(regex._2);
                }
            }

            if (!types.contains(DataTypes.StringType))
                types.add(DataTypes.StringType);

            return types;
        }

        public static List<MeaningType> getPossibleFieldMeaningTypes (String value){

            List<MeaningType> types = new ArrayList<>();

            for (Tuple2<String, MeaningType> regex : meaningTypeRegex) {
                if (value.matches(regex._1)) {
                    types.add(regex._2);
                }
            }

            if (isDate(value))
                types.add(MeaningType.DATE);

            if (types.size() == 0)
                types.add(MeaningType.TEXT);

            return types;

        }

    public static DataType getDataType(String value) {

        DataType type = null;

        for (Tuple2<String, DataType> regex : dataTypeRegex) {
            if (value.matches(regex._1)) {
                type = regex._2;
            }
        }

        if (type == null) {
            type = DataTypes.StringType;
        }
        return type;

    }

    public static List<String> getPossibleDateFormats(String value) { // returns null if not a date

        LocalDate date = null;

        List<String> possibleDateTimeFormats = new ArrayList<>();

        for (String dateTimeFormatter : Recogniser.possibleDateTimeFormats) {
            boolean isParseable = true;
            try {
                date = LocalDate.parse(value, DateTimeFormatter.ofPattern(dateTimeFormatter));
            } catch (DateTimeParseException exc) {
                isParseable = false;
            }
            if (isParseable)
                possibleDateTimeFormats.add(dateTimeFormatter);

        }

        return possibleDateTimeFormats;
    }

    public static boolean isDate(String value) {
        return getPossibleDateFormats(value).size() > 0;
    }

    public static List<FieldMeaning> getPossibleFieldMeaningTypes(List<FieldMeaning> possibleMeanings, String value) {

        List<FieldMeaning> refinedMeanings = new ArrayList<>();


        for (FieldMeaning possibleMeaning : getPossibleMeanings(value)) {
            if (possibleMeanings == null || possibleMeanings.contains(possibleMeaning)) {
                refinedMeanings.add(possibleMeaning);
            }

        }
        return refinedMeanings;
    }

}
