package uk.org.richardjarvis.derive.tabular;

import org.apache.spark.sql.DataFrame;
import uk.org.richardjarvis.metadata.TabularMetaData;

/**
 * Created by rjarvis on 29/02/16.
 */
public class TabularMasterDeriver implements TabularDeriveInterface {

    @Override
    public DataFrame derive(DataFrame input, TabularMetaData metaData) {

        DataFrame output = new DateFormatDeriver().derive(input, metaData);
        output = new StatisticsDeriver().derive(output, metaData);
        output = new RatioDeriver().derive(output, metaData);
        output = new ZIndexDeriver().derive(output, metaData);
        output = new CategoryPopularityDeriver().derive(output, metaData);
        output = new OneHotDeriver().derive(output, metaData);

        return output;
    }
}
