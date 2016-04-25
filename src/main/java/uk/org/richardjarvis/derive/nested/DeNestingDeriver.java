package uk.org.richardjarvis.derive.nested;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructType;
import uk.org.richardjarvis.metadata.nested.NestedMetaData;
import uk.org.richardjarvis.utils.DataFrameUtils;

/**
 * Calculates statistics on input columns. Provides no transformation
 */
public class DeNestingDeriver implements NestedDeriveInterface {

    private static final int MAX_CARDINALITY = 10;

    /**
     * @param input    the input dataframe
     * @param metaData the metadata that describes the input dataframe
     * @return the input DataFrame
     */
    @Override
    public DataFrame derive(DataFrame input, NestedMetaData metaData) {


        StructType schema = DataFrameUtils.unNestColumns(input.schema());

        JavaRDD<Row> outputRows = input.javaRDD().map(row -> DataFrameUtils.unNestData(row));

        return input.sqlContext().createDataFrame(outputRows,schema);

    }
}
