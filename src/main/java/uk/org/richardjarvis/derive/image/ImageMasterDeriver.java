package uk.org.richardjarvis.derive.image;

import org.apache.spark.sql.DataFrame;
import uk.org.richardjarvis.metadata.image.ImageMetaData;

/**
 * Created by rjarvis on 29/02/16.
 */
public class ImageMasterDeriver implements ImageDeriveInterface {

    @Override
    public DataFrame derive(DataFrame input, ImageMetaData metaData) {

        DataFrame output = new HueSaturationLuminanceDeriver().derive(input, metaData);
        output = new ColourNameDeriver().derive(output, metaData);
        output = new BlackAndWhiteDeriver().derive(output, metaData);
        output = new EdgeDetectorDeriver().derive(output, metaData);
        return output;
    }
}
