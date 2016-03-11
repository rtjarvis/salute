package uk.org.richardjarvis.derive.tabular;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.DataFrame;
import uk.org.richardjarvis.metadata.MetaData;
import uk.org.richardjarvis.metadata.TabularMetaData;
import uk.org.richardjarvis.utils.DataFrameUtils;

import java.util.List;

/**
 * Created by rjarvis on 29/02/16.
 */
public class RatioDeriver implements TabularDeriveInterface {


    @Override
    public DataFrame derive(DataFrame input, TabularMetaData metaData) {

        List<Column> numericColumns = DataFrameUtils.getNumericColumns(input);

        int numericColumnCount = numericColumns.size();

        for (int numerator = 0; numerator < numericColumnCount; numerator++) {
            for (int denominator = 0; denominator < numerator; denominator++) {
                Column numeratorColumn = numericColumns.get(numerator);
                Column denominatorColumn = numericColumns.get(denominator);
                input = input.withColumn(numeratorColumn.expr().prettyString() + "_over_" + denominatorColumn.expr().prettyString(), numeratorColumn.divide(denominatorColumn));
            }
        }

        return input;
    }


}
