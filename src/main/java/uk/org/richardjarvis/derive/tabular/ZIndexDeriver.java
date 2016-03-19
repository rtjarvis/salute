package uk.org.richardjarvis.derive.tabular;

import org.apache.spark.sql.*;
import uk.org.richardjarvis.metadata.FieldProperties;
import uk.org.richardjarvis.metadata.FieldStatistics;
import uk.org.richardjarvis.metadata.MetaData;
import uk.org.richardjarvis.metadata.TabularMetaData;
import uk.org.richardjarvis.utils.DataFrameUtils;


import java.util.*;

/**
 * Created by rjarvis on 29/02/16.
 */
public class ZIndexDeriver implements TabularDeriveInterface {

    @Override
    public DataFrame derive(DataFrame input,  TabularMetaData metaData) {

        List<FieldProperties> numericColumns = metaData.getNumericFields();

        if (numericColumns.size()==0)
            return input;

        int numericColumnCount = numericColumns.size();

        DataFrame output = input;

        for (int fieldIndex = 0; fieldIndex < numericColumnCount; fieldIndex++) {
            Column valueColumn = input.col(numericColumns.get(fieldIndex).getName());
            FieldStatistics stats = metaData.getStatistics().get(valueColumn);
            output = output.withColumn(valueColumn.expr().prettyString() + "_zindex", valueColumn.minus(stats.getMean()).divide(stats.getStandardDeviation()));
        }

        return output;
    }



}
