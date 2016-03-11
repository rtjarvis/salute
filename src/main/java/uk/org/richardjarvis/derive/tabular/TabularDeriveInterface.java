package uk.org.richardjarvis.derive.tabular;

import org.apache.spark.sql.DataFrame;
import uk.org.richardjarvis.metadata.MetaData;
import uk.org.richardjarvis.metadata.Statistics;
import uk.org.richardjarvis.metadata.TabularMetaData;

/**
 * Created by rjarvis on 29/02/16.
 */
public interface TabularDeriveInterface {

    DataFrame derive(DataFrame input, TabularMetaData metaData);
}
