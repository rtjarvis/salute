package uk.org.richardjarvis.loader;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.compress.archivers.ArchiveException;
import org.apache.commons.compress.compressors.CompressorException;
import org.json.JSONException;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

/**
 * Created by richard on 10/06/16.
 */
public class LoaderConfig {

    private static final String HAS_HEADER = "h";
    private static final String NO_HEADER = "b";
    private static final String INPUT_FILE = "i";
    private static final String OUTPUT_FILE = "o";
    private static final String DELIMITER = "d";
    private static final String STRING_ENCLOSURE = "s";
    public static final String NO_STRING_ESCAPE = "none";

    Boolean hasHeader = null;
    Character stringEnclosure = null;
    Character delimiter = null;
    String inputPath = null;
    String outputPath = null;

    public LoaderConfig() {
    }

    public LoaderConfig(String inputPath, String outputPath) {
        this.inputPath = inputPath;
        this.outputPath = outputPath;
    }

    public LoaderConfig(CommandLine cmd) {
        inputPath = cmd.getOptionValue(INPUT_FILE);
        outputPath = cmd.getOptionValue(OUTPUT_FILE);
        if (cmd.hasOption(HAS_HEADER))
            hasHeader = true;
        if (cmd.hasOption(NO_HEADER))
            hasHeader = false;
        if (cmd.hasOption(DELIMITER)) {
            String d = cmd.getOptionValue(DELIMITER);
            if (d.equals("\\t")) {
                delimiter = '\t';
            } else {
                delimiter = d.charAt(0);
            }
        }
        if (cmd.hasOption(STRING_ENCLOSURE))
            delimiter = cmd.getOptionValue(STRING_ENCLOSURE).charAt(0);
    }

    public Boolean getHasHeader() {
        return hasHeader;
    }

    public void setHasHeader(Boolean hasHeader) {
        this.hasHeader = hasHeader;
    }

    public Character getStringEnclosure() {
        return stringEnclosure;
    }

    public void setStringEnclosure(Character stringEnclosure) {
        this.stringEnclosure = stringEnclosure;
    }

    public Character getDelimiter() {
        return delimiter;
    }

    public void setDelimiter(Character delimiter) {
        this.delimiter = delimiter;
    }

    public String getInputPath() {
        return inputPath;
    }

    public void setInputPath(String inputPath) {
        this.inputPath = inputPath;
    }

    public String getOutputPath() {

        String out = outputPath;
        int i = 0;

        while (new File(out).exists()) {
            i++;
            out = outputPath + "_" + i;
        }

        return out;
    }

    public void setOutputPath(String outputPath) {
        this.outputPath = outputPath;
    }

    public LoaderConfig copy() {
        LoaderConfig newConfig = new LoaderConfig();
        newConfig.setDelimiter(this.getDelimiter());
        newConfig.setStringEnclosure(this.getStringEnclosure());
        newConfig.setHasHeader(this.getHasHeader());
        newConfig.setInputPath(this.getInputPath());
        newConfig.setOutputPath(this.getOutputPath());
        return newConfig;
    }

    public static Options getOptions() {
        // create Options object
        Options options = new Options();
        Option input = new Option(INPUT_FILE, true, "input file");
        input.setRequired(true);
        Option output = new Option(OUTPUT_FILE, true, "output file");
        output.setRequired(true);
        options.addOption(input);
        options.addOption(output);
        options.addOption(HAS_HEADER, false, "definitely has header");
        options.addOption(NO_HEADER, false, "definitely does not have header (bare)");
        options.addOption(DELIMITER, true, "the delimtier character");
        options.addOption(STRING_ENCLOSURE, true, "the string enclosure character. For none use -" + STRING_ENCLOSURE +" none");
        return options;
    }

    public boolean isValid() {

        if (inputPath == null || outputPath == null)
            return false;

        return true;
    }

}
