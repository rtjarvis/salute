package uk.org.richardjarvis.metadata.text;

import org.apache.commons.csv.CSVFormat;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.LoggerFactory;
import uk.org.richardjarvis.loader.LoaderConfig;
import uk.org.richardjarvis.utils.SparkProvider;

import java.io.IOException;
import java.util.*;

/**
 * Created by rjarvis on 24/02/16.
 */
public class CSVProperties {

    private static final List<Character> MOST_LIKELY_STRING_ESCAPES = Arrays.asList('"', '\'');
    private static final List<Character> MOST_LIKELY_DELIMITERS = Arrays.asList(',', ' ', '\t');
    private static org.slf4j.Logger LOGGER = LoggerFactory.getLogger(CSVProperties.class);
    private Character delimiter;
    private Character stringEnclosure;
    private JavaSparkContext javaSparkContext;
    private CSVFormat csvFormat;

    public CSVProperties(List<String> rows, Character delimiter, String stringEnclosure) throws IOException {
        this.javaSparkContext = SparkProvider.getSparkContext();
        getCSVProperties(rows, delimiter, stringEnclosure);
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

    private void getCSVProperties(List<String> rows, Character suggestedDelimiter, String suggestedStringEnclosure) throws IOException {
        // detect delimiter
        LOGGER.info("Looking for string enclosure character");
        if (suggestedStringEnclosure == null) {
            this.stringEnclosure = getStringEscape(rows);
            LOGGER.info("Found candidate string enclosure: '" + suggestedStringEnclosure + "' but this could be the delimiter");
        } else if (suggestedStringEnclosure.equals(LoaderConfig.NO_STRING_ESCAPE)) {
            this.stringEnclosure = null;
        } else {
            this.stringEnclosure = suggestedStringEnclosure.charAt(0);
        }

        if (suggestedDelimiter == null) {
            LOGGER.info("Search for delimiter assuming that '" + suggestedStringEnclosure + "' is the string enclosure character");
            suggestedDelimiter = getDelimiter(rows, this.stringEnclosure);
            if (suggestedDelimiter == null) {
                LOGGER.info("Unable to detect delimiter using that string escape. Assume that '" + suggestedStringEnclosure + "' is actually a delimiter");
                suggestedDelimiter = this.stringEnclosure;
                this.stringEnclosure = null;
            } else {
                LOGGER.info("Found candidate delimiter: '" + suggestedDelimiter + "'");
            }
        }
        this.delimiter = suggestedDelimiter;

    }

    private Set<Character> getCommonFrequencyCharacter(List<String> rows, Character stringEscape) throws IOException {

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
            return new HashSet<>();

        return candidates;
    }

    private Character getProbableCommonFrequencyCharacter(List<String> rows, List<Character> probableChars, List<Character> unlikelyChars, Character stringEscape) throws IOException {

        Set<Character> commonFrequencyCharacters = getCommonFrequencyCharacter(rows, stringEscape);

        List<Character> candidates = new ArrayList<>();

        candidates.addAll(probableChars);
        candidates.removeAll(unlikelyChars);

        if (commonFrequencyCharacters.size() == 1) {
            return commonFrequencyCharacters.iterator().next();
        } else if (commonFrequencyCharacters.size() > 1) {
            for (Character c : probableChars) {
                for (Character pse : candidates) {
                    if (pse.equals(c))
                        return c;
                }
            }
            return commonFrequencyCharacters.iterator().next();
        } else {
            return null;
        }

    }

    private Character getStringEscape(List<String> rows) throws IOException {

        return getProbableCommonFrequencyCharacter(rows, MOST_LIKELY_STRING_ESCAPES, MOST_LIKELY_DELIMITERS, null);

    }

    private Character getDelimiter(List<String> rows, Character stringEscape) throws IOException {

        return getProbableCommonFrequencyCharacter(rows, MOST_LIKELY_DELIMITERS, MOST_LIKELY_STRING_ESCAPES, stringEscape);

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

        if (csvFormat != null)
            return csvFormat;

        csvFormat = CSVFormat.newFormat(getDelimiter());

        if (getStringEnclosure() != null) {
            csvFormat = csvFormat.withQuote(getStringEnclosure());
        }

        return csvFormat;
    }
}
