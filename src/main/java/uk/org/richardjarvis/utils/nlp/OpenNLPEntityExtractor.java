package uk.org.richardjarvis.utils.nlp;

import opennlp.tools.namefind.NameFinderME;
import opennlp.tools.namefind.TokenNameFinder;
import opennlp.tools.sentdetect.SentenceDetectorME;
import opennlp.tools.sentdetect.SentenceModel;
import opennlp.tools.tokenize.TokenizerME;
import opennlp.tools.tokenize.TokenizerModel;
import opennlp.tools.util.Span;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.org.richardjarvis.metadata.FieldMeaning;

import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.util.*;
import java.util.concurrent.Callable;

public class OpenNLPEntityExtractor implements Serializable {

    private static final Logger LOGGER = LoggerFactory.getLogger(OpenNLPEntityExtractor.class);

    public static final String NAME_MODEL = "name";
    public static final String MONEY_MODEL = "money";
    public static final String LOCATIONS_MODEL = "locations";
    public static final String ORGANISATIONS_MODEL = "organisations";
    public static final String DATES_MODEL = "dates";;

    public static final Map<String, FieldMeaning.MeaningType> MODELS = new HashMap<>();

    static PooledTokenNameFinderModel nameModel = null;
    static PooledTokenNameFinderModel orgModel = null;
    static PooledTokenNameFinderModel locModel = null;
    static PooledTokenNameFinderModel monModel = null;
    static SentenceModel sentModel = null;
    static TokenizerModel tm = null;

    static {

        MODELS.put(NAME_MODEL, FieldMeaning.MeaningType.NAME);
        MODELS.put(MONEY_MODEL, FieldMeaning.MeaningType.MONEY);
        MODELS.put(LOCATIONS_MODEL, FieldMeaning.MeaningType.LOCATION);
        MODELS.put(ORGANISATIONS_MODEL, FieldMeaning.MeaningType.ORGANISATION);
        MODELS.put(DATES_MODEL, FieldMeaning.MeaningType.DATE);

        InputStream pis = OpenNLPEntityExtractor.class.getResourceAsStream("/en-ner-person.bin");
        InputStream ois = OpenNLPEntityExtractor.class.getResourceAsStream("/en-ner-organization.bin");
        InputStream lis = OpenNLPEntityExtractor.class.getResourceAsStream("/en-ner-location.bin");
        InputStream mis = OpenNLPEntityExtractor.class.getResourceAsStream("/en-ner-money.bin");
        InputStream tis = OpenNLPEntityExtractor.class.getResourceAsStream("/en-token.bin");
        InputStream sis = OpenNLPEntityExtractor.class.getResourceAsStream("/en-sent.bin");

        try {

            nameModel = new PooledTokenNameFinderModel(pis);
            orgModel = new PooledTokenNameFinderModel(ois);
            locModel = new PooledTokenNameFinderModel(lis);
            monModel = new PooledTokenNameFinderModel(mis);
            sentModel = new SentenceModel(sis);
            tm = new TokenizerModel(tis);

        } catch (IOException ex) {
            LOGGER.error("Couldn't load OpenNLP models", ex);
        }
    }

    public static Map<String, List<String>> getResults(String textToAnalyse) throws Exception {

        LOGGER.info("Extract entitiies using OpenNLP");

        List<String> names = new ArrayList<>();
        List<String> locations = new ArrayList<>();
        List<String> organisations = new ArrayList<>();
        List<String> dates = new ArrayList<>();
        List<String> money = new ArrayList<>();

        Map<String, List<String>> resultsMap = new HashMap<>();
        resultsMap.put(NAME_MODEL, names);
        resultsMap.put(MONEY_MODEL, money);
        resultsMap.put(LOCATIONS_MODEL, locations);
        resultsMap.put(ORGANISATIONS_MODEL, organisations);
        resultsMap.put(DATES_MODEL, dates);

        try {

            TokenNameFinder nameFinder = new NameFinderME(nameModel);
            TokenNameFinder orgFinder = new NameFinderME(orgModel);
            TokenNameFinder locFinder = new NameFinderME(locModel);
            TokenNameFinder monFinder = new NameFinderME(monModel);

            LOGGER.info("Splitting text into sentences");
            List<String> sentences = getSentences(textToAnalyse);

            LOGGER.info("Extracting name, location, organisations and money entities from sentences");

            for (String sentence : sentences) {

                String[] tokens = tokenization(sentence);

                List<String> nameTokens = getTokens(tokens, nameFinder);
                names.addAll(nameTokens);

                nameFinder.clearAdaptiveData();

                List<String> locationTokens = getTokens(tokens, locFinder);
                locations.addAll(locationTokens);
                locFinder.clearAdaptiveData();

                List<String> organisationTokens = getTokens(tokens, orgFinder);
                organisations.addAll(organisationTokens);
                orgFinder.clearAdaptiveData();

                List<String> moneyTokens = getTokens(tokens, monFinder);
                money.addAll(moneyTokens);
                monFinder.clearAdaptiveData();
            }

        } catch (IOException ex) {
            LOGGER.error("Error obtaining name values", ex);
        }

        return resultsMap;
    }

    private static List<String> getSentences(String textToAnalyse) {

        SentenceDetectorME sentenceDetector = new SentenceDetectorME(sentModel);

        String sentences[] = sentenceDetector.sentDetect(textToAnalyse);

        return Arrays.asList(sentences);
    }

    private static List<String> getTokens(String[] tokens, TokenNameFinder finder) throws IOException {

        if (tokens != null) {
            Span nameSpans[] = finder.find(tokens);
            String[] names = Span.spansToStrings(nameSpans, tokens);
            return Arrays.asList(names);
        }

        return new ArrayList<>();
    }

    private static String[] tokenization(String text) throws IOException {

        TokenizerME tz = new TokenizerME(tm);
        String[] tokens = tz.tokenize(text);

        return tokens;
    }

    public List<String> getTokens(String text) {

        StringTokenizer st = new StringTokenizer(text);

        List<String> tokens = new ArrayList<>();

        while (st.hasMoreTokens()) {
            tokens.add(st.nextToken());
        }

        return tokens;
    }
}
