package uk.org.richardjarvis.metadata;

import org.apache.spark.sql.DataFrame;

import java.io.File;

/**
 * Created by rjarvis on 26/02/16.
 */
public interface MetaData {
    void generateReport(String inputPath, DataFrame inputData, DataFrame derivedData, String outputPath);
}
