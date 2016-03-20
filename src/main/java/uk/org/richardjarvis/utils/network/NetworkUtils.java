package uk.org.richardjarvis.utils.network;

import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import uk.org.richardjarvis.metadata.FieldMeaning;
import uk.org.richardjarvis.utils.DataFrameUtils;

import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.URL;
import java.net.UnknownHostException;

/**
 * Created by rjarvis on 19/03/16.
 */
public class NetworkUtils {

    public static DataFrame getGeoCities(SQLContext sqlContext) {

        String inputFile = NetworkUtils.class.getResource("/GeoLite2/GeoLite2-City-Blocks-IPv4.csv.gz").getPath();

        DataFrame df = sqlContext.read()
                .format("com.databricks.spark.csv")
                .schema(getIPv4BlocksSchema())
                .option("header", "true")
                .option("codec", "org.apache.hadoop.io.compress.GzipCodec")
                .load(inputFile);

        return df;
    }

    public static StructType getIPv4BlocksSchema() {

        StructField[] fields  = new StructField[9];

        fields[0] = new StructField("network", DataTypes.StringType, true, DataFrameUtils.getMetadata(FieldMeaning.MeaningType.IPv4_SUBNET,null));
        fields[1] = new StructField("geoname_id", DataTypes.StringType, true, DataFrameUtils.getMetadata(null,null));
        fields[2] = new StructField("registered_country_geoname_id", DataTypes.StringType, true, DataFrameUtils.getMetadata(null,null));
        fields[3] = new StructField("represented_country_geoname_id", DataTypes.StringType, true, DataFrameUtils.getMetadata(null,null));
        fields[4] = new StructField("is_anonymous_proxy", DataTypes.StringType, true, DataFrameUtils.getMetadata(null,null));
        fields[5] = new StructField("is_satellite_provider", DataTypes.StringType, true, DataFrameUtils.getMetadata(null,null));
        fields[6] = new StructField("postal_code", DataTypes.StringType, true, DataFrameUtils.getMetadata(FieldMeaning.MeaningType.POSTAL_CODE,null));
        fields[7] = new StructField("latitude", DataTypes.DoubleType, true, DataFrameUtils.getMetadata(FieldMeaning.MeaningType.LATITUDE,null));
        fields[8] = new StructField("longitude", DataTypes.DoubleType, true, DataFrameUtils.getMetadata(FieldMeaning.MeaningType.LONGITUDE,null));

        return new StructType(fields);
    }

    public static boolean netMatch(String testIp, String network) {

        String[] parts = network.split("/");
        String ip = parts[0];
        int prefix;

        if (parts.length < 2) {
            prefix = 0;
        } else {
            prefix = Integer.parseInt(parts[1]);
        }

        Inet4Address subnetNet = null;
        Inet4Address testIPNet = null;
        try {
            subnetNet = (Inet4Address) InetAddress.getByName(ip);
            testIPNet = (Inet4Address) InetAddress.getByName(testIp);
        } catch (UnknownHostException e) {
        }

        byte[] subnetBytes = subnetNet.getAddress();
        int subnetInt = ((subnetBytes[0] & 0xFF) << 24) |
                ((subnetBytes[1] & 0xFF) << 16) |
                ((subnetBytes[2] & 0xFF) << 8) |
                ((subnetBytes[3] & 0xFF) << 0);

        byte[] testIPBytes = testIPNet.getAddress();
        int testIPInt = ((testIPBytes[0] & 0xFF) << 24) |
                ((testIPBytes[1] & 0xFF) << 16) |
                ((testIPBytes[2] & 0xFF) << 8) |
                ((testIPBytes[3] & 0xFF) << 0);

        int mask = ~((1 << (32 - prefix)) - 1);

        if ((subnetInt & mask) == (testIPInt & mask)) {
            return true;
        } else {
            return false;
        }
    }


}
