package uk.org.richardjarvis.processor.image;

import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.tika.exception.TikaException;
import org.apache.tika.metadata.Metadata;
import org.xml.sax.SAXException;
import uk.org.richardjarvis.metadata.text.FieldMeaning;
import uk.org.richardjarvis.metadata.image.ImageMetaData;
import uk.org.richardjarvis.metadata.MetaData;
import uk.org.richardjarvis.processor.ProcessorInterface;
import uk.org.richardjarvis.utils.DataFrameUtils;
import uk.org.richardjarvis.utils.file.FileUtils;

import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.awt.image.Raster;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by rjarvis on 24/02/16.
 */
public class ImageProcessor implements ProcessorInterface {

    @Override
    public MetaData extractMetaData(String path) throws IOException {

        ImageMetaData imageMetaData = new ImageMetaData();

        try {
            Metadata metadata = FileUtils.getMetadata(path);
            imageMetaData.setMetadata(metadata);
        } catch (SAXException e) {
            throw new IOException(e);
        } catch (TikaException e) {
            throw new IOException(e);
        }

        return imageMetaData;
    }

    @Override
    public DataFrame extractData(String path, MetaData metaData, SQLContext sqlContext) throws IOException {

        ImageMetaData imageMetaData = (ImageMetaData) metaData;
        BufferedImage imageData = ImageIO.read(new File(path));
        Raster raster = imageData.getData();

        List<Row> rows = new ArrayList<>();

        StructType schema = getSchema(imageMetaData);

        Object[] row = new Object[schema.size()];

        Integer width = imageMetaData.getWidth();
        Integer height = imageMetaData.getHeight();
        Integer maxColourValue = imageMetaData.getMaxColourValue();
        int numDataElements = raster.getNumDataElements();

        double[] rawData = new double[width * height * numDataElements];
        raster.getPixels(0, 0, width, height, rawData);
        Double[][][] imageDataArray = new Double[height][width][numDataElements];

        int x = 0, y = 0, e = 0;
        for (int i = 0; i < rawData.length; i++) {
            imageDataArray[y][x][e] = rawData[i]/maxColourValue;
            e = (e + 1) % numDataElements;
            if (e % numDataElements == 0) {
                x = (x + 1) % width;
                if (x == 0) {
                    y++;
                }
            }
        }

        int fieldIndex = 0;
        row[fieldIndex++] = width;
        row[fieldIndex++] = height;
        row[fieldIndex++] = imageDataArray;

        String[] metadataNames = imageMetaData.getMetadata().names();
        for (String name : metadataNames) {
            row[fieldIndex++] = imageMetaData.getMetadata().get(name);
        }

        rows.add(RowFactory.create(row));

        DataFrame output = sqlContext.createDataFrame(rows, schema);
        return output;
    }

    private StructType getSchema(ImageMetaData metaData) {

        String[] metadataNames = metaData.getMetadata().names();

        StructField[] fields = new StructField[metadataNames.length + 3];

        int fieldIndex = 0;
        fields[fieldIndex++] = new StructField("Width", DataTypes.IntegerType, false, org.apache.spark.sql.types.Metadata.empty());
        fields[fieldIndex++] = new StructField("Height", DataTypes.IntegerType, false, org.apache.spark.sql.types.Metadata.empty());
        fields[fieldIndex++] = new StructField("RawImageData", DataTypes.createArrayType(DataTypes.createArrayType(DataTypes.createArrayType(DataTypes.DoubleType))), false, DataFrameUtils.getMetadata(FieldMeaning.MeaningType.RAW_IMAGE_DATA, null));
        for (String name : metadataNames) {
            fields[fieldIndex++] = new StructField("ImageData_" + name, DataTypes.StringType, true, DataFrameUtils.getMetadata(FieldMeaning.MeaningType.IMAGE_METADATA, null));
        }
        return new StructType(fields);
    }
}