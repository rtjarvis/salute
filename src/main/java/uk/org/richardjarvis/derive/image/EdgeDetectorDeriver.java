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
public class EdgeDetectorDeriver implements ImageDeriveInterface {


    private static int[][] filter1 = {
            {-1, 0, 1},
            {-2, 0, 2},
            {-1, 0, 1}
    };

    private static int[][] filter2 = {
            {1, 2, 1},
            {0, 0, 0},
            {-1, -2, -1}
    };

    @Override
    public DataFrame derive(DataFrame input, ImageMetaData metaData) {

        List<String> blackAndWhiteImageData = DataFrameUtils.getColumnsNames(DataFrameUtils.getColumnsOfMeaning(input, FieldMeaning.MeaningType.BLACK_AND_WHITE_IMAGE_DATA));

        if (blackAndWhiteImageData.size() == 0)
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

            for (String column : blackAndWhiteImageData) {

                WrappedArray<WrappedArray<Double>> data = row.getAs(row.fieldIndex(column));

                Integer width = row.getInt(0);
                Integer height = row.getInt(1);

                Double[][] edgeArray = new Double[height][width];

                for (int y = 1; y < height - 1; y++) {
                    for (int x = 1; x < width - 1; x++) {
                        // apply filter
                        double grey1 = 0, grey2 = 0;
                        for (int i = 0; i < 3; i++) {
                            for (int j = 0; j < 3; j++) {
                                Double grey = data.apply(y + j - 1).apply(x + i - 1);
                                grey1 += grey * filter1[i][j];
                                grey2 += grey * filter2[i][j];
                            }
                        }
                        edgeArray[y][x] = 1 - truncate(Math.sqrt(grey1 * grey1 + grey2 * grey2));
                    }
                }
                outputRow[fieldIndex++] = edgeArray;
            }
            return RowFactory.create(outputRow);

        });

        return input.sqlContext().createDataFrame(rows, newSchema);

    }

    public StructType getUpdatedSchema(DataFrame input, ImageMetaData metaData) {

        List<StructField> newColumns = new ArrayList<>();

        newColumns.addAll(Arrays.asList(input.schema().fields()));
        newColumns.add(new StructField("EdgeImageData", DataTypes.createArrayType(DataTypes.createArrayType(DataTypes.DoubleType)), false, DataFrameUtils.getMetadata(FieldMeaning.MeaningType.EDGE_IMAGE_DATA, null)));

        return new StructType(newColumns.toArray(new StructField[0]));
    }

    public static double truncate(double a) {
        if (a < 0) return 0;
        else if (a > 1) return 1.0;
        else return a;
    }

}
