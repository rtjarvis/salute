package uk.org.richardjarvis.derive.tabular;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.MetadataBuilder;
import uk.org.richardjarvis.metadata.FieldMeaning;
import uk.org.richardjarvis.metadata.FieldProperties;
import uk.org.richardjarvis.metadata.MetaData;
import uk.org.richardjarvis.metadata.TabularMetaData;
import uk.org.richardjarvis.utils.DataFrameUtils;

import java.util.List;

/**
 * Derives ratios between numerical fields
 */
public class RatioDeriver implements TabularDeriveInterface {

    /**
     * @param input    the input dataframe
     * @param metaData the metadata that describes the input dataframe
     * @return an enriched DataFrame with addtional fields that are the ratio between pairs of the original numerical columns
     */
    @Override
    public DataFrame derive(DataFrame input, TabularMetaData metaData) {

        List<FieldProperties> numericColumns = metaData.getNumericFields();

        if (numericColumns.size() == 0)
            return input;

        int numericColumnCount = numericColumns.size();

        for (int numerator = 0; numerator < numericColumnCount; numerator++) {
            for (int denominator = 0; denominator < numerator; denominator++) {
                String numeratorColumnName = numericColumns.get(numerator).getName();
                String denominatorColumnName = numericColumns.get(denominator).getName();
                Column numeratorColumn = input.col(numeratorColumnName);
                Column denominatorColumn = input.col(denominatorColumnName);
                Metadata metadata = DataFrameUtils.getMetadata(FieldMeaning.MeaningType.NUMERIC, null);
                input = input.withColumn(numeratorColumnName + "_over_" + denominatorColumnName, numeratorColumn.divide(denominatorColumn),metadata);
            }
        }

        return input;
    }


}
