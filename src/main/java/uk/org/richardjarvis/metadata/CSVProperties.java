package uk.org.richardjarvis.metadata;

import org.slf4j.LoggerFactory;
import uk.org.richardjarvis.processor.text.TextProcessor;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Created by rjarvis on 24/02/16.
 */
public class CSVProperties {

    private static org.slf4j.Logger LOGGER = LoggerFactory.getLogger(CSVProperties.class);

    public Character getDelimiter() {
        return delimiter;
    }

    public Character getStringEnclosure() {
        return stringEnclosure;
    }

    private Character delimiter;
    private Character stringEnclosure;


    public CSVProperties(InputStream inputStream) throws IOException {
        getCSVProperties(inputStream);
        inputStream.reset();
    }

    @Override
    public String toString() {
        return "Using " + (stringEnclosure == null ? "nothing" : "'" + stringEnclosure + "'") + " as string enclosure and '" + delimiter + "' as the delimiter";
    }

    private void getCSVProperties(InputStream inputStream) throws IOException {
        // detect delimiter
        LOGGER.info("Looking for string enclosure character");
        stringEnclosure = getStringEscape(inputStream);
        LOGGER.info("Found candidate string enclosure: '" + stringEnclosure + "' but this could be the delimiter");


        LOGGER.info("Search for delimiter assuming that '" + stringEnclosure + "' is the string enclosure character");
        delimiter = getDelimiter(inputStream, stringEnclosure);

        if (delimiter == null) {
            LOGGER.info("Unable to detect delimiter using that string escape. Assume that '" + stringEnclosure + "' is actually a delimiter");
            delimiter = stringEnclosure;
            stringEnclosure = null;
        } else {
            LOGGER.info("Found candidate delimiter: '" + delimiter + "'");
        }

    }

    private Character getCommonFrequencyCharacter(InputStream inputStream, Character stringEscape) throws IOException {

        BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));

        Map<Character, Integer> firstLineFrequencyTable = getFrequencyTable(reader.readLine(), stringEscape);

        Set<Character> candidates = firstLineFrequencyTable.keySet();

        int i = 0;
        while (i < TextProcessor.MAX_ROWS_TO_PROCESS) {

            String nextLine = reader.readLine();
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
        inputStream.reset();
        if (candidates.size() == 0)
            return null;

        return candidates.iterator().next();
    }


    private Character getStringEscape(InputStream inputStream) throws IOException {

        return getCommonFrequencyCharacter(inputStream, null);

    }

    private Character getDelimiter(InputStream inputStream, Character stringEscape) throws IOException {

        return getCommonFrequencyCharacter(inputStream, stringEscape);
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
}
