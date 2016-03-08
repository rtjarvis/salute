package uk.org.richardjarvis.derive;

import org.apache.spark.sql.DataFrame;

/**
 * Created by rjarvis on 29/02/16.
 */
public class MasterDeriver implements DeriveInterface {

    @Override
    public DataFrame derive(DataFrame input) {

        DataFrame output = new RatioDeriver().derive(input);
        output = new DeviationDeriver().derive(output);

        return output;
    }
}
