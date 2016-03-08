package uk.org.richardjarvis.derive;

import org.apache.spark.sql.DataFrame;

/**
 * Created by rjarvis on 29/02/16.
 */
public interface DeriveInterface {

    DataFrame derive(DataFrame input);
}
