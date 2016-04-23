package uk.org.richardjarvis.derive.image;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.collection.Iterator;
import scala.collection.mutable.WrappedArray;
import uk.org.richardjarvis.metadata.FieldMeaning;
import uk.org.richardjarvis.metadata.ImageMetaData;
import uk.org.richardjarvis.utils.DataFrameUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by rjarvis on 20/04/16.
 */
public class BlackAndWhiteDeriver implements ImageDeriveInterface {
    @Override
    public DataFrame derive(DataFrame input, ImageMetaData metaData) {

        List<String> imageDataColumnNames = DataFrameUtils.getColumnsNames(DataFrameUtils.getColumnsOfMeaning(input, FieldMeaning.MeaningType.RAW_IMAGE_DATA));

        if (imageDataColumnNames.size() == 0)
            return input;

        StructType newSchema = getUpdatedSchema(input, metaData);

        int originalFieldCount = input.schema().fieldNames().length;
        int newFieldCount = newSchema.size();

        JavaRDD<Row> rows = input.javaRDD().map(row -> {

            Object[] outputRow = new Object[newFieldCount];

            int fieldIndex = 0;
            while (fieldIndex < originalFieldCount) {
                outputRow[fieldIndex] = row.get(fieldIndex);
                fieldIndex++;
            }

            for (String column : imageDataColumnNames) {

                WrappedArray<WrappedArray<WrappedArray<Double>>> data = row.getAs(row.fieldIndex(column));

                Integer width = row.getInt(0);
                Integer height = row.getInt(1);

                // algrothim from http://www.niwa.nu/2013/05/math-behind-colorspace-conversions-rgb-hsl/

                Double[][] imageDataArray = new Double[height][width];

                Iterator<WrappedArray<WrappedArray<Double>>> itr = data.iterator();

                int y = 0;
                while (itr.hasNext()) {
                    Iterator<WrappedArray<Double>> rowItr = itr.next().iterator();
                    int x = 0;
                    while (rowItr.hasNext()) {

                        WrappedArray<Double> pixel = rowItr.next();
                        Double red = pixel.apply(0);
                        Double green = pixel.apply(1);
                        Double blue = pixel.apply(2);
                        imageDataArray[y][x] = 0.21 * red + 0.72 * green + 0.07 * blue; // see http://www.johndcook.com/blog/2009/08/24/algorithms-convert-color-grayscale/
                        x++;
                    }
                    y++;
                }

                outputRow[fieldIndex++] = imageDataArray;
            }
            return RowFactory.create(outputRow);

        });

        return input.sqlContext().createDataFrame(rows, newSchema);

    }

    public StructType getUpdatedSchema(DataFrame input, ImageMetaData metaData) {

        List<StructField> newColumns = new ArrayList<>();

        newColumns.addAll(Arrays.asList(input.schema().fields()));
        newColumns.add(new StructField("BlackAndWhiteImageData", DataTypes.createArrayType(DataTypes.createArrayType(DataTypes.DoubleType)), false, DataFrameUtils.getMetadata(FieldMeaning.MeaningType.BLACK_AND_WHITE_IMAGE_DATA, null)));

        return new StructType(newColumns.toArray(new StructField[0]));
    }
}
