package uk.org.richardjarvis.utils.field;

import com.google.i18n.phonenumbers.CountryCodeToRegionCodeMap;
import com.google.i18n.phonenumbers.NumberParseException;
import com.google.i18n.phonenumbers.PhoneNumberUtil;
import com.google.i18n.phonenumbers.Phonenumber;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import scala.Tuple2;
import uk.org.richardjarvis.metadata.FieldMeaning;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static uk.org.richardjarvis.metadata.FieldMeaning.MeaningType;

/**
 * Class for recognising the semantic of a String
 */
public class Recogniser {

    private static final java.lang.String COUNTRY_CODE_SEPARATOR = "|";
    private static Set<FieldMeaning> typeSet;

    static {

        typeSet = new HashSet<>();
        typeSet.add(new FieldMeaning(MeaningType.NUMERIC, null, DataTypes.DoubleType, "^[-+]*[0-9]*\\.[0-9]+$"));
        typeSet.add(new FieldMeaning(MeaningType.NUMERIC, null, DataTypes.IntegerType, "^[0-9]+$"));
        typeSet.add(new FieldMeaning(MeaningType.BOOLEAN, null, DataTypes.BooleanType, "^(true|false|True|False|TRUE|FALSE|Yes|No|YES|NO|yes|no|Y|N)$"));
        typeSet.add(new FieldMeaning(MeaningType.TEXT, null, DataTypes.StringType, "^.*$"));
        typeSet.add(new FieldMeaning(MeaningType.MAC_ADDRESS, null, DataTypes.StringType, "^(([0-9A-Fa-f]{2}[-:]){5}[0-9A-Fa-f]{2})|(([0-9A-Fa-f]{4}\\.){2}[0-9A-Fa-f]{4})$"));
        typeSet.add(new FieldMeaning(MeaningType.EMAIL_ADDRESS, null, DataTypes.StringType, "^([a-zA-Z0-9_\\-\\.]+)@((\\[[0-9]{1,3}\\.[0-9]{1,3}\\.[0-9]{1,3}\\.)|(([a-zA-Z0-9\\-]+\\.)+))([a-zA-Z]{2,4}|[0-9]{1,3})(\\]?)$"));
        typeSet.add(new FieldMeaning(MeaningType.IPv4, null, DataTypes.StringType, "^([01]?\\d\\d?|2[0-4]\\d|25[0-5])\\.([01]?\\d\\d?|2[0-4]\\d|25[0-5])\\.([01]?\\d\\d?|2[0-4]\\d|25[0-5])\\.([01]?\\d\\d?|2[0-4]\\d|25[0-5])$"));
        typeSet.add(new FieldMeaning(MeaningType.IPv6, null, DataTypes.StringType, "^(^(([0-9A-F]{1,4}(((:[0-9A-F]{1,4}){5}::[0-9A-F]{1,4})|((:[0-9A-F]{1,4}){4}::[0-9A-F]{1,4}(:[0-9A-F]{1,4}){0,1})|((:[0-9A-F]{1,4}){3}::[0-9A-F]{1,4}(:[0-9A-F]{1,4}){0,2})|((:[0-9A-F]{1,4}){2}::[0-9A-F]{1,4}(:[0-9A-F]{1,4}){0,3})|(:[0-9A-F]{1,4}::[0-9A-F]{1,4}(:[0-9A-F]{1,4}){0,4})|(::[0-9A-F]{1,4}(:[0-9A-F]{1,4}){0,5})|(:[0-9A-F]{1,4}){7}))$|^(::[0-9A-F]{1,4}(:[0-9A-F]{1,4}){0,6})$)|^::$)|^((([0-9A-F]{1,4}(((:[0-9A-F]{1,4}){3}::([0-9A-F]{1,4}){1})|((:[0-9A-F]{1,4}){2}::[0-9A-F]{1,4}(:[0-9A-F]{1,4}){0,1})|((:[0-9A-F]{1,4}){1}::[0-9A-F]{1,4}(:[0-9A-F]{1,4}){0,2})|(::[0-9A-F]{1,4}(:[0-9A-F]{1,4}){0,3})|((:[0-9A-F]{1,4}){0,5})))|([:]{2}[0-9A-F]{1,4}(:[0-9A-F]{1,4}){0,4})):|::)((25[0-5]|2[0-4][0-9]|[0-1]?[0-9]{0,2})\\.){3}(25[0-5]|2[0-4][0-9]|[0-1]?[0-9]{0,2})$"));
        typeSet.add(new FieldMeaning(MeaningType.URL, null, DataTypes.StringType, "^(http|ftp|https):\\/\\/[\\w\\-_]+(\\.[\\w\\-_]+)+([\\w\\-\\.,@?^=%&amp;:/~\\+#]*[\\w\\-\\@?^=%&amp;/~\\+#])?$"));
        typeSet.add(new FieldMeaning(MeaningType.URL, null, DataTypes.StringType, "^(http|ftp|https):\\/\\/[\\w\\-_]+(\\.[\\w\\-_]+)+([\\w\\-\\.,@?^=%&amp;:/~\\+#]*[\\w\\-\\@?^=%&amp;/~\\+#])?$"));
        typeSet.add(new FieldMeaning(MeaningType.DATE, "yyyy.MM.dd G 'at' HH:mm:ss z", DataTypes.StringType, null));
        typeSet.add(new FieldMeaning(MeaningType.DATE, "EEE, MMM d, ''yy", DataTypes.StringType, null));
        typeSet.add(new FieldMeaning(MeaningType.DATE, "h:mm a", DataTypes.StringType, null));
        typeSet.add(new FieldMeaning(MeaningType.DATE, "hh 'o''clock' a, zzzz", DataTypes.StringType, null));
        typeSet.add(new FieldMeaning(MeaningType.DATE, "K:mm a, z", DataTypes.StringType, null));
        typeSet.add(new FieldMeaning(MeaningType.DATE, "yyyyy.MMMMM.dd GGG hh:mm a", DataTypes.StringType, null));
        typeSet.add(new FieldMeaning(MeaningType.DATE, "EEE, d MMM yyyy HH:mm:ss Z", DataTypes.StringType, null));
        typeSet.add(new FieldMeaning(MeaningType.DATE, "yyMMddHHmmssZ", DataTypes.StringType, null));
        typeSet.add(new FieldMeaning(MeaningType.DATE, "yyyy-MM-dd'T'HH:mm:ss.SSSZ", DataTypes.StringType, null));
        typeSet.add(new FieldMeaning(MeaningType.DATE, "yyyy-MM-dd HH:mm:ss", DataTypes.StringType, null));
        typeSet.add(new FieldMeaning(MeaningType.DATE, "yyyy-MM-dd'T'HH:mm:ss.SSSXXX", DataTypes.StringType, null));
        typeSet.add(new FieldMeaning(MeaningType.DATE, "YYYY-'W'ww-u", DataTypes.StringType, null));
        typeSet.add(new FieldMeaning(MeaningType.DATE, "dd/MM/yy HH:mm", DataTypes.StringType, null));
        typeSet.add(new FieldMeaning(MeaningType.DATE, "dd/MM/yy HH:mm:ss", DataTypes.StringType, null));
        typeSet.add(new FieldMeaning(MeaningType.DATE, "dd/MM/yyyy HH:mm:ss", DataTypes.StringType, null));
        typeSet.add(new FieldMeaning(MeaningType.DATE, "MM/dd/yy HH:mm", DataTypes.StringType, null));
        typeSet.add(new FieldMeaning(MeaningType.DATE, "MM/dd/yy HH:mm:ss", DataTypes.StringType, null));
        typeSet.add(new FieldMeaning(MeaningType.DATE, "MM/dd/yyyy HH:mm:ss", DataTypes.StringType, null));
        typeSet.add(new FieldMeaning(MeaningType.PHONE_NUMBER, null, DataTypes.StringType, null));

    }

    /**
     * @param value the String to evaluate
     * @return a list of possible meanings that this String could have. This includes semantic and DataType combinations
     */
    public static Set<FieldMeaning> getPossibleMeanings(String value) {
        Set<FieldMeaning> types = new HashSet<>();

        for (FieldMeaning meaning : typeSet) {
            String regex = meaning.getMatchingRegex();
            if (regex == null) {
                switch (meaning.getMeaningType()) {
                    case DATE:
                        if (matchesDateFormat(value, meaning.getFormat()))
                            types.add(meaning);
                        break;
                    case PHONE_NUMBER:
                        String format = countriesForPhoneNumber(value, meaning.getFormat());
                        if (format.length() > 0) {
                            meaning.setFormat(format);
                            types.add(meaning);
                        }
                        break;
                }
            } else if (value.matches(regex)) {
                types.add(meaning);
            }
        }

        return types;
    }

    public static String countriesForPhoneNumber(String value, String format) {

        PhoneNumberUtil phoneUtil = PhoneNumberUtil.getInstance();

        String[] possibleCountryCodes;
        if (format != null) {
            possibleCountryCodes = format.split(COUNTRY_CODE_SEPARATOR);
        } else {
            possibleCountryCodes = phoneUtil.getSupportedRegions().toArray(new String[0]);
        }

        StringBuilder matchingCountries = new StringBuilder();
        boolean first = true;
        for (String countryCode : possibleCountryCodes) {

            try {
                Phonenumber.PhoneNumber test = phoneUtil.parse(value, countryCode);
                if (phoneUtil.isValidNumberForRegion(test, countryCode)) {
                    if (!first) {
                        matchingCountries.append("|");
                    }
                    matchingCountries.append(countryCode);
                    first = false;
                }
            } catch (NumberParseException e) {
            }

        }

        return matchingCountries.toString();
    }

    /**
     * @param value the String to be evaulated
     * @return list of possible date formats that could be used to decode this date or null if not a date.
     */
    public static boolean matchesDateFormat(String value, String format) {

        LocalDate date = null;

        boolean isParseable = true;
        try {
            date = LocalDate.parse(value, DateTimeFormatter.ofPattern(format));
        } catch (DateTimeParseException exc) {
            isParseable = false;
        }
        return isParseable;
    }

    /**
     * @param possibleMeanings list of meanings that are currently possible for this value
     * @param value            the String to be evaulated
     * @return an updated list of meanings (a subset of possibleMeanings) that also match value
     */
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
