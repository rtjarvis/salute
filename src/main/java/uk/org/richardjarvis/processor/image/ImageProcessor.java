package uk.org.richardjarvis.processor.image;

import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import uk.org.richardjarvis.metadata.ImageMetaData;
import uk.org.richardjarvis.metadata.MetaData;
import uk.org.richardjarvis.processor.ProcessorInterface;

import javax.imageio.stream.ImageInputStream;
import java.io.IOException;

/**
 * Created by rjarvis on 24/02/16.
 */
public class ImageProcessor implements ProcessorInterface {

    @Override
    public MetaData extractMetaData(String path) throws IOException {

        ImageMetaData imageMetaData = new ImageMetaData();



        javax.imageio.metadata.IIOMetadataFormat iioMetadata = null;

        //imageMetaData.setIIOMetaData(iioMetadata);
//        iioMetadata.
//        ImageInputStream iis;


        return imageMetaData;
    }

    @Override
    public DataFrame extractData(String path, MetaData metaData, SQLContext sqlContext) throws IOException {
        return null;
    }
}