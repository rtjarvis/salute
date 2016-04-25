package uk.org.richardjarvis.processor.nested;

import au.com.bytecode.opencsv.CSVReader;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.slf4j.LoggerFactory;
import uk.org.richardjarvis.metadata.text.CSVProperties;
import uk.org.richardjarvis.metadata.text.FieldProperties;
import uk.org.richardjarvis.metadata.MetaData;
import uk.org.richardjarvis.metadata.text.TabularMetaData;
import uk.org.richardjarvis.processor.ProcessorInterface;
import uk.org.richardjarvis.metadata.nested.NestedMetaData;
import uk.org.richardjarvis.utils.SparkProvider;

import java.io.IOException;
import java.io.Reader;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by rjarvis on 24/02/16.
 */
public class MultiLineJSONProcessor implements ProcessorInterface {

    public static int MAX_ROWS_TO_PROCESS = 100;
    private static org.slf4j.Logger LOGGER = LoggerFactory.getLogger(MultiLineJSONProcessor.class);

    @Override
    public NestedMetaData extractMetaData(String path) throws IOException {

        Row[] headRows = SparkProvider.getSQLContext().read().json(path).take(MultiLineJSONProcessor.MAX_ROWS_TO_PROCESS);

        NestedMetaData metaData = new NestedMetaData(headRows);

        return metaData;

    }

    @Override
    public DataFrame extractData(String path, MetaData metaData, SQLContext sqlContext) throws IOException {

        if (!(metaData instanceof NestedMetaData))
            return null;

        NestedMetaData nestedMetaData = (NestedMetaData) metaData;

        DataFrame df = sqlContext.read()
                .json(path);

        return df;
    }

}