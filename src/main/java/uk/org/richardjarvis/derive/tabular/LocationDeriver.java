package uk.org.richardjarvis.derive.tabular;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import uk.org.richardjarvis.metadata.text.FieldMeaning;
import uk.org.richardjarvis.metadata.text.TabularMetaData;
import uk.org.richardjarvis.utils.DataFrameUtils;
import uk.org.richardjarvis.utils.geo.GeoUtils;
import uk.org.richardjarvis.utils.network.GeoIPUtils;
import uk.org.richardjarvis.utils.network.NetworkUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.IntStream;

/**
 * Processes IP addresses and provides Geo information for them
 */
public class LocationDeriver implements TabularDeriveInterface {

    private static final String GEO_DATA_PARQUET_FILE = "/GeoLite2";

    /**
     * @param input    the input dataframe
     * @param metaData the metadata that describes the input dataframe
     * @return an enriched DataFrame containing Geo information for IP addresses
     */
    @Override
    public DataFrame derive(DataFrame input, TabularMetaData metaData) {

        List<String> latitudeColumns = DataFrameUtils.getColumnsNames(DataFrameUtils.getColumnsOfMeaning(input, FieldMeaning.MeaningType.LATITUDE));
        List<String> longitudeColumns = DataFrameUtils.getColumnsNames(DataFrameUtils.getColumnsOfMeaning(input, FieldMeaning.MeaningType.LONGITUDE));

        if (latitudeColumns.size() == 0 || longitudeColumns.size()==0)
            return input;

        DataFrame lookupDF = GeoUtils.getCitiesDataFrame(input.sqlContext());

        StructType updatedSchema = getUpdatedSchema(input, lookupDF);

        int originalFieldCount = input.schema().fieldNames().length;
        int newFieldCount = updatedSchema.size();

        int lowerSubnetFieldIndex = lookupDF.schema().fieldIndex(GeoIPUtils.NETWORK_COLUMN + NetworkUtils.SUBNET_LOWER_RANGE_NAME);
        int upperSubnetFieldIndex = lookupDF.schema().fieldIndex(GeoIPUtils.NETWORK_COLUMN + NetworkUtils.SUBNET_UPPER_RANGE_NAME);

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

//            for (String ipColumn : ipColumns) {
//
//                Long value = NetworkUtils.getAddress(row.getString(row.fieldIndex(ipColumn)));
//                Integer lookupPartion = GeoIPUtils.getPartitionThatContainsIP(value);
//
//                Row match = null;
//                if (lookupPartion != null) {
//
//                    for (Row lookupRow : lookupArray[lookupPartion]) {
//
//                        long lowerNet = lookupRow.getLong(lowerSubnetFieldIndex);
//                        long upperNet = lookupRow.getLong(upperSubnetFieldIndex);
//
//                        if (lowerNet <= value && upperNet >= value) {
//                            match = lookupRow;
//                            break;
//                        }
//                    }
//
//                    if (match != null) {
//                        for (int i = 0; i < match.size(); i++) {
//                            outputRow[fieldIndex++] = match.get(i);
//                        }
//                    }
//                }
//            }

            return RowFactory.create(outputRow);
        });

        return input.sqlContext().createDataFrame(rows, updatedSchema);

    }


    private StructType getUpdatedSchema(DataFrame input, DataFrame lookup) {

        List<StructField> newColumns = new ArrayList<>();

        newColumns.addAll(Arrays.asList(input.schema().fields()));

        List<String> ipColumns = DataFrameUtils.getColumnsNames(DataFrameUtils.getColumnsOfMeaning(input, FieldMeaning.MeaningType.IPv4));

        for (String column : ipColumns) {
            for (StructField lookupField : lookup.schema().fields()) {
                newColumns.add(new StructField(column + "_" + lookupField.name(), lookupField.dataType(), lookupField.nullable(), lookupField.metadata()));
            }
        }

        return new StructType(newColumns.toArray(new StructField[0])

        );
    }

}
