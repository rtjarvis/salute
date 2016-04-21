package uk.org.richardjarvis.derive.image;

import org.apache.spark.sql.DataFrame;
import uk.org.richardjarvis.metadata.ImageMetaData;

/**
 * Created by rjarvis on 29/02/16.
 */
public interface ImageDeriveInterface {

    DataFrame derive(DataFrame input, ImageMetaData metaData);
}
