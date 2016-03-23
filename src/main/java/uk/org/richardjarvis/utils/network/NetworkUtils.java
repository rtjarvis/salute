package uk.org.richardjarvis.utils.network;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;

import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by rjarvis on 23/03/16.
 */
public class NetworkUtils {

    public static final String NETWORK_INT_NAME = "_integer";
    public static final String SUBNET_LOWER_RANGE_NAME = "_min";
    public static final String SUBNET_UPPER_RANGE_NAME = "_max";

    public static DataFrame encodeNetworkIPs(DataFrame input, List<String> networkColumns) {

        JavaRDD<Row> rdd = input.javaRDD();

        int originalFieldCount = input.schema().size();
        int newFieldCount = input.schema().size() + networkColumns.size();

        JavaRDD<Row> rows = rdd.map(row -> {

            Object[] outputRow = new Object[newFieldCount];

            int fieldIndex = 0;
            while (fieldIndex < originalFieldCount) {
                outputRow[fieldIndex] = row.get(fieldIndex);
                fieldIndex++;
            }

            for (String networkColumn : networkColumns) {

                String network = row.getString(row.fieldIndex(networkColumn));

                Tuple2<Long, Long> netBounds = getSubnetBounds(network);
                outputRow[fieldIndex++] = netBounds._1;
            }

            return RowFactory.create(outputRow);

        });

        StructType updatedSchema = getUpdatedIPSchema(input, networkColumns);

        return input.sqlContext().createDataFrame(rows, updatedSchema);
    }

    private static StructType getUpdatedIPSchema(DataFrame input, List<String> networkColumns) {

        List<StructField> newColumns = new ArrayList<>();

        newColumns.addAll(Arrays.asList(input.schema().fields()));
        for (String networkColumn : networkColumns) {
            newColumns.add(new StructField(networkColumn + NETWORK_INT_NAME, DataTypes.LongType, true, Metadata.empty()));
        }

        return new StructType(newColumns.toArray(new StructField[0]));
    }

    public static Tuple2<Long, Long> getSubnetBounds(String network) {
        try {

            String[] parts = network.split("/");
            String ip = parts[0];
            int prefix;

            if (parts.length < 2) {
                prefix = 0;
            } else {
                prefix = Integer.parseInt(parts[1]);
            }

            Long address = getAddress(ip);

            Long mask = 4294967295l - ((1 << (32 - prefix)) - 1);

            Long min = address & mask;
            Long max = min + ((1 << (32 - prefix)) - 1);

            return new Tuple2<>(min, max);

        } catch (UnknownHostException e) {
            return null;
        }

    }

    public static Long getAddress(String ip) throws UnknownHostException {
        Inet4Address subnetNet = (Inet4Address) InetAddress.getByName(ip);
        byte[] addressBytes = subnetNet.getAddress();
        return ((long) (addressBytes[0] & 0xFF) << 24l) |
                ((long) (addressBytes[1] & 0xFF) << 16l) |
                ((long) (addressBytes[2] & 0xFF) << 8l) |
                ((long) (addressBytes[3] & 0xFF) << 0l);
    }

    public static boolean netMatch(String testIp, String network) throws UnknownHostException {

        Tuple2<Long, Long> bounds = getSubnetBounds(network);
        long address = getAddress(testIp);
        return bounds._1 <= address && bounds._2 >= address;

    }

    public static DataFrame encodeNetworkSubnets(DataFrame input, String networkColumn) {
        return encodeNetworkSubnets(input, Arrays.asList(networkColumn));
    }

    public static DataFrame encodeNetworkSubnets(DataFrame input, List<String> networkColumns) {

        JavaRDD<Row> rdd = input.javaRDD();

        int originalFieldCount = input.schema().size();
        int newFieldCount = input.schema().size() + 2 * networkColumns.size();

        JavaRDD<Row> rows = rdd.map(row -> {

            Object[] outputRow = new Object[newFieldCount];

            int fieldIndex = 0;
            while (fieldIndex < originalFieldCount) {
                outputRow[fieldIndex] = row.get(fieldIndex);
                fieldIndex++;
            }

            for (String networkColumn : networkColumns) {

                String network = row.getString(row.fieldIndex(networkColumn));

                Tuple2<Long, Long> netBounds = getSubnetBounds(network);
                outputRow[fieldIndex++] = netBounds._1;
                outputRow[fieldIndex++] = netBounds._2;
            }

            return RowFactory.create(outputRow);

        });

        StructType updatedSchema = getUpdatedSubnetSchema(input, networkColumns);

        return input.sqlContext().createDataFrame(rows, updatedSchema);
    }

    private static StructType getUpdatedSubnetSchema(DataFrame input, List<String> networkColumns) {

        List<StructField> newColumns = new ArrayList<>();

        newColumns.addAll(Arrays.asList(input.schema().fields()));
        for (String networkColumn : networkColumns) {
            newColumns.add(new StructField(networkColumn + SUBNET_LOWER_RANGE_NAME, DataTypes.LongType, true, Metadata.empty()));
            newColumns.add(new StructField(networkColumn + SUBNET_UPPER_RANGE_NAME, DataTypes.LongType, true, Metadata.empty()));
        }

        return new StructType(newColumns.toArray(new StructField[0]));
    }

}
