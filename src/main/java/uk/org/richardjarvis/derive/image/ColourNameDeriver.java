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
import uk.org.richardjarvis.utils.image.ColourNamer;

import java.util.*;

/**
 * Created by rjarvis on 20/04/16.
 */
public class ColourNameDeriver implements ImageDeriveInterface {
    @Override
    public DataFrame derive(DataFrame input, ImageMetaData metaData) {

        List<String> hslDataColumnNames = DataFrameUtils.getColumnsNames(DataFrameUtils.getColumnsOfMeaning(input, FieldMeaning.MeaningType.HSL_IMAGE_DATA));

        if (hslDataColumnNames.size() == 0)
            return input;

        StructType newSchema = getUpdatedSchema(input);

        int originalFieldCount = input.schema().fieldNames().length;
        int newFieldCount = newSchema.size();

        JavaRDD<Row> rows = input.javaRDD().map(row -> {

            Object[] outputRow = new Object[newFieldCount];

            int fieldIndex = 0;
            while (fieldIndex < originalFieldCount) {
                outputRow[fieldIndex] = row.get(fieldIndex);
                fieldIndex++;
            }

            for (String column : hslDataColumnNames) {

                Map<String, Integer> colourDist = new HashMap<>();

                WrappedArray<WrappedArray<WrappedArray<Double>>> data = row.getAs(row.fieldIndex(column));

                Iterator<WrappedArray<WrappedArray<Double>>> itr = data.iterator();

                int y = 0;
                while (itr.hasNext()) {
                    Iterator<WrappedArray<Double>> rowItr = itr.next().iterator();
                    int x = 0;
                    while (rowItr.hasNext()) {
                        WrappedArray<Double> pixel = rowItr.next();
                        Double hue = pixel.apply(0);
                        Double saturation = pixel.apply(1);
                        Double luminance = pixel.apply(2);

                        String colourName = ColourNamer.getColourName(hue, saturation, luminance).getName();
                        Integer existingCount = colourDist.get(colourName);
                        if (existingCount == null)
                            existingCount = 0;
                        colourDist.put(colourName, existingCount+1);
                        x++;
                    }
                    y++;
                }

                for (String colourName : ColourNamer.getNames()) {
                    Integer count = colourDist.get(colourName);
                    if (count == null)
                        count = 0;

                    outputRow[fieldIndex++] = count;
                }
            }

            return RowFactory.create(outputRow);

        });

        return input.sqlContext().createDataFrame(rows, newSchema);

    }

    public StructType getUpdatedSchema(DataFrame input) {

        List<StructField> newColumns = new ArrayList<>();

        newColumns.addAll(Arrays.asList(input.schema().fields()));
        String[] names = ColourNamer.getNames();
        for (String colourName : names) {
            newColumns.add(new StructField(colourName, DataTypes.IntegerType, true, DataFrameUtils.getMetadata(FieldMeaning.MeaningType.COLOUR_NAME, null)));
        }

        return new StructType(newColumns.toArray(new StructField[0]));
    }
}
