package uk.org.richardjarvis.metadata;

import org.apache.tika.metadata.Metadata;
import org.apache.tika.metadata.Property;

import java.io.Serializable;

/**
 * Created by rjarvis on 26/02/16.
 */
public class ImageMetaData implements MetaData, Serializable {

    Metadata metadata;
    Integer width = null;
    Integer height = null;
    Integer maxColourValue = null;

    public Integer getMaxColourValue() {
        return maxColourValue;
    }

    public void setMaxColourValue(Integer maxColourValue) {
        this.maxColourValue = maxColourValue;
    }

    public void setHeight(Integer height) {
        this.height = height;
    }

    public void setWidth(Integer width) {
        this.width = width;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("ImageMetaData{");

        if (metadata != null) {
            String[] metadataNames = metadata.names();

            for (String name : metadataNames) {
                sb.append(name).append(": ").append(metadata.get(name)).append(",\n");
            }
        }
        sb.append("dimensions: ").append(width).append("x").append(height);
        sb.append('}');
        return sb.toString();
    }

    public Metadata getMetadata() {
        return metadata;
    }

    public void setMetadata(Metadata metadata) {
        this.metadata = metadata;
        if (this.width == null) {
            setWidth(Integer.parseInt(metadata.get("width")));
        }
        if (this.height == null) {
            setHeight(Integer.parseInt(metadata.get("height")));
        }
        if (this.getMaxColourValue() == null) {
            String bps = metadata.get("Data BitsPerSample").split(" ")[0];
            setMaxColourValue((int)Math.round(Math.pow(2,Double.parseDouble(bps)))-1);
        }
    }

    public Integer getWidth() {
        return width;
    }

    public Integer getHeight() {
        return height;
    }
}
