package uk.org.richardjarvis.derive.tabular;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.DataFrame;
import uk.org.richardjarvis.metadata.FieldProperties;
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

        List<FieldProperties> numericColumns = metaData.getNumericFields();

        int numericColumnCount = numericColumns.size();

        for (int numerator = 0; numerator < numericColumnCount; numerator++) {
            for (int denominator = 0; denominator < numerator; denominator++) {
                String numeratorColumnName = numericColumns.get(numerator).getName();
                String denominatorColumnName = numericColumns.get(denominator).getName();
                Column numeratorColumn = input.col(numeratorColumnName);
                Column denominatorColumn = input.col(denominatorColumnName);
                input = input.withColumn(numeratorColumnName + "_over_" + denominatorColumnName, numeratorColumn.divide(denominatorColumn));
            }
        }

        return input;
    }


}
