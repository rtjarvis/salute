package uk.org.richardjarvis.derive;

import org.apache.spark.sql.*;
import uk.org.richardjarvis.utils.DataFrameUtils;


import java.util.*;

/**
 * Created by rjarvis on 29/02/16.
 */
public class DeviationDeriver implements DeriveInterface {

    @Override
    public DataFrame derive(DataFrame input, Statistics statisticsMap) {

        List<Column> numericColumns = DataFrameUtils.getNumericColumns(input);

        int numericColumnCount = numericColumns.size();

        DataFrame output = input;

        for (int fieldIndex = 0; fieldIndex < numericColumnCount; fieldIndex++) {
            Column valueColumn = numericColumns.get(fieldIndex);
            FieldStatistics stats = statisticsMap.get(valueColumn);
            output = output.withColumn(valueColumn.expr().prettyString() + "_zindex", valueColumn.minus(stats.getMean()).divide(stats.getStandardDeviation()));
        }

        return output;
    }



}
