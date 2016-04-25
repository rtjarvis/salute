package uk.org.richardjarvis.derive.nested;

import org.apache.spark.sql.DataFrame;
import uk.org.richardjarvis.metadata.nested.NestedMetaData;
import uk.org.richardjarvis.metadata.text.TabularMetaData;

/**
 * Created by rjarvis on 29/02/16.
 */
public interface NestedDeriveInterface {
    /**
     * Takes an input dataframe and derives additonal columns
     *
     * @param input the input DataFrame
     * @param metaData the metadata that describes the input dataframe
     * @return the enriched DataFrame
     */
    DataFrame derive(DataFrame input, NestedMetaData metaData);

}
