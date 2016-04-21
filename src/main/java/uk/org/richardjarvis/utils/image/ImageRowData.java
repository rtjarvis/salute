package uk.org.richardjarvis.utils.image;

import scala.Tuple3;
import uk.org.richardjarvis.metadata.ImageMetaData;

import java.io.Serializable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by rjarvis on 20/04/16.
 */
public class ImageRowData implements Serializable {

    Map<String,String> metaData;
    List<List<Tuple3<Double,Double,Double>>> imageData = null;

    public ImageRowData(ImageMetaData imageMetaData, List<List<Tuple3<Double, Double, Double>>> imageData) {
        this.metaData = new HashMap<>();
        for (String name : imageMetaData.getMetadata().names()) {
            metaData.put(name, imageMetaData.getMetadata().get(name));
        }
        this.imageData = imageData;
    }

    public Map<String, String> getMetaData() {
        return metaData;
    }

    public void setMetaData(Map<String, String> metaData) {
        this.metaData = metaData;
    }

    public List<List<Tuple3<Double, Double, Double>>> getImageData() {
        return imageData;
    }

    public void setImageData(List<List<Tuple3<Double, Double, Double>>> imageData) {
        this.imageData = imageData;
    }
}
