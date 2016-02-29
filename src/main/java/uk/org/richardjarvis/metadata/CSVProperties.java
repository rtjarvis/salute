package uk.org.richardjarvis.metadata;

import org.apache.commons.csv.CSVFormat;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.LoggerFactory;
import uk.org.richardjarvis.utils.SparkProvider;

import java.io.IOException;
import java.util.*;

/**
 * Created by rjarvis on 24/02/16.
 */
public class CSVProperties {

    private static org.slf4j.Logger LOGGER = LoggerFactory.getLogger(CSVProperties.class);
    private Character delimiter;
    private Character stringEnclosure;
    private JavaSparkContext javaSparkContext;
    private CSVFormat csvFormat;

    public CSVProperties(List<String> rows) throws IOException {
        this.javaSparkContext = SparkProvider.getSparkContext();
        getCSVProperties(rows);
    }

    public Character getDelimiter() {
        return delimiter;
    }

    public Character getStringEnclosure() {
        return stringEnclosure;
    }

    @Override
    public String toString() {
        return "Using " + (stringEnclosure == null ? "nothing" : "'" + stringEnclosure + "'") + " as string enclosure and '" + delimiter + "' as the delimiter";
    }

    private void getCSVProperties(List<String> rows) throws IOException {
        // detect delimiter
        LOGGER.info("Looking for string enclosure character");
        stringEnclosure = getStringEscape(rows);
        LOGGER.info("Found candidate string enclosure: '" + stringEnclosure + "' but this could be the delimiter");


        LOGGER.info("Search for delimiter assuming that '" + stringEnclosure + "' is the string enclosure character");
        delimiter = getDelimiter(rows, stringEnclosure);

        if (delimiter == null) {
            LOGGER.info("Unable to detect delimiter using that string escape. Assume that '" + stringEnclosure + "' is actually a delimiter");
            delimiter = stringEnclosure;
            stringEnclosure = null;
        } else {
            LOGGER.info("Found candidate delimiter: '" + delimiter + "'");
        }

    }

    private Character getCommonFrequencyCharacter(List<String> rows, Character stringEscape) throws IOException {

        Map<Character, Integer> firstLineFrequencyTable = getFrequencyTable(rows.get(0), stringEscape);

        Set<Character> candidates = firstLineFrequencyTable.keySet();

        Iterator<String> reader = rows.iterator();
        int i = 0;
        while (reader.hasNext()) {

            String nextLine = reader.next();
            if (nextLine == null)
                break;

            Map<Character, Integer> frequencyTable = getFrequencyTable(nextLine, stringEscape);

            if (frequencyTable != null) {
                Set<Character> newCandidates = new HashSet<>();

                for (Character character : candidates) {
                    if (frequencyTable.get(character) == firstLineFrequencyTable.get(character)) {
                        newCandidates.add(character);
                    }
                }

                candidates = newCandidates;
            }

            i++;
        }
        if (candidates.size() == 0)
            return null;

        return candidates.iterator().next();
    }


    private Character getStringEscape(List<String> rows) throws IOException {

        return getCommonFrequencyCharacter(rows, null);

    }

    private Character getDelimiter(List<String> rows, Character stringEscape) throws IOException {

        return getCommonFrequencyCharacter(rows, stringEscape);
    }

    private Map<Character, Integer> getFrequencyTable(String text, Character stringEscape) {

        Map<Character, Integer> frequencyTable = null;

        if (text != null) {

            frequencyTable = new HashMap<>();
            boolean insideString = false;

            for (int i = 0; i < text.length(); i++) {

                Character c = text.charAt(i);
                if (c == stringEscape) {
                    insideString = !insideString;
                } else {

                    if (!insideString) {
                        Integer count = frequencyTable.get(c);
                        if (count == null) {
                            count = 1;
                        } else {
                            count++;
                        }
                        frequencyTable.put(c, count);
                    }
                }
            }
        }
        return frequencyTable;

    }

    public CSVFormat getCsvFormat() {

        if (csvFormat !=null )
            return csvFormat;

        csvFormat = CSVFormat.newFormat(getDelimiter());

        if (getStringEnclosure() != null) {
            csvFormat = csvFormat.withQuote(getStringEnclosure());
        }

        return csvFormat;
    }
}
