package uk.org.richardjarvis.derive.tabular;

import org.apache.spark.sql.DataFrame;
import uk.org.richardjarvis.metadata.MetaData;
import uk.org.richardjarvis.metadata.Statistics;
import uk.org.richardjarvis.metadata.TabularMetaData;

/**
 * Created by rjarvis on 29/02/16.
 */
public interface TabularDeriveInterface {
    /**
     * Takes an input dataframe and derives additonal columns
     *
     * @param input the input DataFrame
     * @param metaData the metadata that describes the input dataframe
     * @return the enriched DataFrame
     */
    DataFrame derive(DataFrame input, TabularMetaData metaData);
}
