package uk.org.richardjarvis.derive.tabular;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import sun.nio.ch.Net;
import uk.org.richardjarvis.metadata.FieldMeaning;
import uk.org.richardjarvis.metadata.Statistics;
import uk.org.richardjarvis.metadata.TabularMetaData;
import uk.org.richardjarvis.utils.DataFrameUtils;
import uk.org.richardjarvis.utils.network.NetworkUtils;

import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by rjarvis on 19/03/16.
 */
public class GeoIPDeriver implements TabularDeriveInterface {

    @Override
    public DataFrame derive(DataFrame input, TabularMetaData metaData) {

        List<String> ipColumns = DataFrameUtils.getColumnsNames(DataFrameUtils.getColumnsOfMeaning(input, FieldMeaning.MeaningType.IPv4));

        if (ipColumns.size() == 0)
            return input;

        StructType updatedSchema = getUpdatedSchema(input, metaData);

        DataFrame lookup = NetworkUtils.getGeoCities(input.sqlContext());

        Row[] lookupRows = lookup.collect();
        int originalFieldCount = input.schema().fieldNames().length;
        int newFieldCount = updatedSchema.size();

        JavaRDD<Row> rows = input.javaRDD().map(row -> {

            Object[] outputRow = new Object[newFieldCount];

            int fieldIndex = 0;
            while (fieldIndex < originalFieldCount) {
                outputRow[fieldIndex] = row.get(fieldIndex);
                fieldIndex++;
            }

            for (String col : ipColumns) {

                Row match = null;
                for (Row lookupRow : lookupRows) {
                    String networkSubnet = lookupRow.getString(0);
                    String ip = row.getString(row.fieldIndex(col));
                    if (NetworkUtils.netMatch(ip, networkSubnet)) {
                        match = lookupRow;
                        break;
                    }
                }

                if (match!=null) {
                    for(int i=0; i< match.size(); i++) {
                        outputRow[fieldIndex++] = match.get(i);
                    }
                } else {
                    fieldIndex+=lookupRows[0].size();
                }

            }
                return RowFactory.create(outputRow);

        });

        Row test = rows.first();
        return input.sqlContext().createDataFrame(rows, updatedSchema);
    }


    private StructType getUpdatedSchema(DataFrame input, TabularMetaData metaData) {

        List<StructField> newColumns = new ArrayList<>();

        newColumns.addAll(Arrays.asList(input.schema().fields()));

        List<String> ipColumns = DataFrameUtils.getColumnsNames(DataFrameUtils.getColumnsOfMeaning(input, FieldMeaning.MeaningType.IPv4));

        StructType lookupSchema = NetworkUtils.getIPv4BlocksSchema();

        for (String column : ipColumns) {
            for (StructField field : lookupSchema.fields()) {
                newColumns.add(new StructField(column + "_" + field.name(), field.dataType(), field.nullable(), field.metadata()));
            }
        }

        return new StructType(newColumns.toArray(new StructField[0]));
    }

}
