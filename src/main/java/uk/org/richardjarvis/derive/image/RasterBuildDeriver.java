package uk.org.richardjarvis.derive.image;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import uk.org.richardjarvis.metadata.FieldMeaning;
import uk.org.richardjarvis.metadata.ImageMetaData;
import uk.org.richardjarvis.utils.DataFrameUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by rjarvis on 20/04/16.
 */
public class RasterBuildDeriver implements ImageDeriveInterface {
    @Override
    public DataFrame derive(DataFrame input, ImageMetaData metaData) {

        List<String> imageDataColumnNames = DataFrameUtils.getColumnsNames(DataFrameUtils.getColumnsOfMeaning(input, FieldMeaning.MeaningType.RAW_IMAGE_DATA));

        if (imageDataColumnNames.size() == 0)
            return input;

        StructType newSchema = getUpdatedSchema(input, metaData);

        int originalFieldCount = input.schema().fieldNames().length;
        int newFieldCount = imageDataColumnNames.size();

//        JavaRDD<Row> rows = input.javaRDD().map(row -> {
//
//            Object[] outputRow = new Object[newFieldCount];
//
//            int fieldIndex = 0;
//            while (fieldIndex < originalFieldCount) {
//                outputRow[fieldIndex] = row.get(fieldIndex);
//                fieldIndex++;
//            }
//
//            int num
//
//            for (String column : imageDataColumnNames) {
//
//                List<Double> data = row.getList(row.fieldIndex(column));
//
//                for (int i=0; i<data.size(); i+=) {
//
//                }
//
//          //      outputRow[fieldIndex++] = data;
//            }
//            return RowFactory.create(outputRow);
//
//        });
//
//        return input.sqlContext().createDataFrame(rows, newSchema);
        return input;
    }

    public StructType getUpdatedSchema(DataFrame input, ImageMetaData metaData) {

        List<StructField> newColumns = new ArrayList<>();

        newColumns.addAll(Arrays.asList(input.schema().fields()));
        //newColumns.add(new StructField());

        return new StructType(newColumns.toArray(new StructField[0]));
    }
}
