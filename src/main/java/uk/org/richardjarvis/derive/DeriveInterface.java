package uk.org.richardjarvis.derive;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.DataFrame;

import java.util.Map;

/**
 * Created by rjarvis on 29/02/16.
 */
public interface DeriveInterface {

    DataFrame derive(DataFrame input, Statistics statisticsMap);
}
