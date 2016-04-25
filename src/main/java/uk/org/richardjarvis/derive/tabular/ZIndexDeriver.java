package uk.org.richardjarvis.derive.tabular;

import org.apache.spark.sql.*;
import org.apache.spark.sql.types.Metadata;
import uk.org.richardjarvis.metadata.text.FieldMeaning;
import uk.org.richardjarvis.metadata.text.FieldProperties;
import uk.org.richardjarvis.metadata.text.FieldStatistics;
import uk.org.richardjarvis.metadata.text.TabularMetaData;
import uk.org.richardjarvis.utils.DataFrameUtils;


import java.util.*;

/**
 * Calculates z-index values for all numeric columns. z-index is (x - avg(x))/stddev(x)
 */
public class ZIndexDeriver implements TabularDeriveInterface {

    /**
     * @param input    the input DataFrame
     * @param metaData the metadata that describes the input dataframe
     * @return an enriched DataFrame with additional numeric columns that contain the z-score for the original numeric
     */
    @Override
    public DataFrame derive(DataFrame input, TabularMetaData metaData) {

        List<FieldProperties> numericColumns = metaData.getNumericFields();

        if (numericColumns.size() == 0)
            return input;

        int numericColumnCount = numericColumns.size();

        DataFrame output = input;

        for (int fieldIndex = 0; fieldIndex < numericColumnCount; fieldIndex++) {
            Column valueColumn = input.col(numericColumns.get(fieldIndex).getName());
            FieldStatistics stats = metaData.getStatistics().get(valueColumn);
            Metadata metadata = DataFrameUtils.getMetadata(FieldMeaning.MeaningType.NUMERIC, null);
            output = output.withColumn(valueColumn.expr().prettyString() + "_zindex", valueColumn.minus(stats.getMean()).divide(stats.getStandardDeviation()), metadata);
        }

        return output;
    }


}
