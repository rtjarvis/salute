package uk.org.richardjarvis.derive;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.DataFrame;
import uk.org.richardjarvis.metadata.MetaData;

import java.util.Map;

/**
 * Created by rjarvis on 29/02/16.
 */
public class MasterDeriver implements DeriveInterface {

    MetaData metaData;

    public MasterDeriver(MetaData metaData) {
        this.metaData=metaData;
    }

    @Override
    public DataFrame derive(DataFrame input, Statistics statisticsMap) {

        DataFrame output = new StatisticsDeriver().derive(input, statisticsMap);
        output = new RatioDeriver().derive(output, statisticsMap);
        output = new DeviationDeriver().derive(output, statisticsMap);
        output = new CategoryPopularityDeriver().derive(output, statisticsMap);
        output = new OneHotDeriver().derive(output, statisticsMap);

        return output;
    }
}
