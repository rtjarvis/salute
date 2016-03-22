package uk.org.richardjarvis.derive.tabular;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.*;
import uk.org.richardjarvis.metadata.FieldMeaning;
import uk.org.richardjarvis.metadata.TabularMetaData;
import uk.org.richardjarvis.utils.DataFrameUtils;
import uk.org.richardjarvis.utils.network.NetworkUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.IntStream;

/**
 * Created by rjarvis on 19/03/16.
 */
public class GeoIPDeriver implements TabularDeriveInterface {

    @Override
    public DataFrame derive(DataFrame input, TabularMetaData metaData) {

        List<String> ipColumns = DataFrameUtils.getColumnsNames(DataFrameUtils.getColumnsOfMeaning(input, FieldMeaning.MeaningType.IPv4));

        if (ipColumns.size() == 0)
            return input;

        DataFrame lookupDF = NetworkUtils.getGeoIPDataFrame(input.sqlContext());

        StructType updatedSchema = getUpdatedSchema(input);

        int originalFieldCount = input.schema().fieldNames().length;
        int newFieldCount = updatedSchema.size();

        int lowerSubnetFieldIndex = lookupDF.schema().fieldIndex(NetworkUtils.NETWORK_COLUMN + NetworkUtils.SUBNET_LOWER_RANGE_NAME);
        int upperSubnetFieldIndex = lookupDF.schema().fieldIndex(NetworkUtils.NETWORK_COLUMN + NetworkUtils.SUBNET_UPPER_RANGE_NAME);

        JavaRDD<Row> lookupRDD = lookupDF.javaRDD();

        int[] partitions = IntStream.range(0, lookupRDD.getNumPartitions()).toArray();
        List<Row>[] lookupArray = lookupRDD.collectPartitions(partitions);

        JavaRDD<Row> rows = input.javaRDD().map(row -> {

            Object[] outputRow = new Object[newFieldCount];

            int fieldIndex = 0;
            while (fieldIndex < originalFieldCount) {
                outputRow[fieldIndex] = row.get(fieldIndex);
                fieldIndex++;
            }

            for (String ipColumn : ipColumns) {

                Long value = NetworkUtils.getAddress(row.getString(row.fieldIndex(ipColumn)));
                Integer lookupPartion = NetworkUtils.getPartitionThatContainsIP(value);

                Row match = null;
                if (lookupPartion!=null) {

                    for (Row lookupRow : lookupArray[lookupPartion]) {

                        long lowerNet = lookupRow.getLong(lowerSubnetFieldIndex);
                        long upperNet = lookupRow.getLong(upperSubnetFieldIndex);

                        if (lowerNet <= value && upperNet >= value) {
                            match = lookupRow;
                            break;
                        }
                    }

                    if (match != null) {
                        outputRow[fieldIndex++] = match.get(match.fieldIndex("latitude"));
                        outputRow[fieldIndex++] = match.get(match.fieldIndex("longitude"));
                        outputRow[fieldIndex++] = match.get(match.fieldIndex("country_name"));
                    } else {
                        System.out.println(lookupPartion +" does not contain " + value);
                    }
                }
            }

            return RowFactory.create(outputRow);
        });

        return input.sqlContext().createDataFrame(rows, updatedSchema);

    }


    private StructType getUpdatedSchema(DataFrame input) {

        List<StructField> newColumns = new ArrayList<>();

        newColumns.addAll(Arrays.asList(input.schema().fields()));

        List<String> ipColumns = DataFrameUtils.getColumnsNames(DataFrameUtils.getColumnsOfMeaning(input, FieldMeaning.MeaningType.IPv4));

        for (String column : ipColumns) {
            newColumns.add(new StructField(column + "_latitude", DataTypes.DoubleType, true, Metadata.empty()));
            newColumns.add(new StructField(column + "_longitude", DataTypes.DoubleType, true, Metadata.empty()));
            newColumns.add(new StructField(column + "_country_name", DataTypes.StringType, true, Metadata.empty()));
        }

        return new StructType(newColumns.toArray(new StructField[0]));
    }

}
