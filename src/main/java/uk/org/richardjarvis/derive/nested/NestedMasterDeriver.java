package uk.org.richardjarvis.derive.nested;

import javafx.scene.control.Tab;
import org.apache.spark.sql.DataFrame;
import uk.org.richardjarvis.derive.tabular.*;
import uk.org.richardjarvis.derive.tabular.StatisticsDeriver;
import uk.org.richardjarvis.metadata.nested.NestedMetaData;
import uk.org.richardjarvis.metadata.text.TabularMetaData;

/**
 * The Master Dervier. This sets the order and selection of derivations that will be performed on Tabular Data
 */
public class NestedMasterDeriver implements NestedDeriveInterface {

    /**
     * @param input    the input DataFrame
     * @param metaData the metadata that describes the input dataframe
     * @return the completely enriched DataFrame
     */
    @Override
    public DataFrame derive(DataFrame input, NestedMetaData metaData) {

        DataFrame output = new DeNestingDeriver().derive(input, metaData);

// TODO descend into tabular deriver here.
        return output;
    }
}
