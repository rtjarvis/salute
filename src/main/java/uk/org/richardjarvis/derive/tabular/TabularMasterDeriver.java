package uk.org.richardjarvis.derive.tabular;

import org.apache.spark.sql.DataFrame;
import uk.org.richardjarvis.metadata.TabularMetaData;

/**
 * The Master Dervier. This sets the order and selection of derivations that will be performed on Tabular Data
 */
public class TabularMasterDeriver implements TabularDeriveInterface {

    /**
     *
     * @param input the input DataFrame
     * @param metaData the metadata that describes the input dataframe
     * @return the completely enriched DataFrame
     */
    @Override
    public DataFrame derive(DataFrame input, TabularMetaData metaData) {

        DataFrame output = new DateFormatDeriver().derive(input, metaData);
        output = new StatisticsDeriver().derive(output, metaData);
        output = new RatioDeriver().derive(output, metaData);
        output = new ZIndexDeriver().derive(output, metaData);
        output = new CategoryPopularityDeriver().derive(output, metaData);
        output = new OneHotDeriver().derive(output, metaData);
        output = new EntityDeriver().derive(output, metaData);
        output = new GeoIPDeriver().derive(output, metaData);

        return output;
    }
}
