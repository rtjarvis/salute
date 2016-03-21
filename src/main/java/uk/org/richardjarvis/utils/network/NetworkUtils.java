package uk.org.richardjarvis.utils.network;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.*;
import scala.Tuple2;
import uk.org.richardjarvis.metadata.FieldMeaning;
import uk.org.richardjarvis.utils.DataFrameUtils;
import uk.org.richardjarvis.utils.FIle.FileUtils;

import java.net.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by rjarvis on 19/03/16.
 */
public class NetworkUtils {

    public static final String NETWORK_COLUMN = "network";
    private static final String GEO_DATA_PARQUET_FILE = "/GeoLite2.parquet";
    private static final String IPV4_BLOCKS_FILE = "/GeoLite2-City-Blocks-IPv4.csv";
    private static final String CITY_LOCATIONS_FILE = "/GeoLite2-City-Locations-en.csv";
    public static final String SUBNET_LOWER_RANGE_NAME = "_min";
    public static final String SUBNET_UPPER_RANGE_NAME = "_max";
    public static final String NETWORK_INT_NAME = "_integer";
    private static URL geoCitiesURL;

    static {
        try {
            geoCitiesURL = new URL("http://geolite.maxmind.com/download/geoip/database/GeoLite2-City-CSV.zip");
        } catch (MalformedURLException e) {
            e.printStackTrace();
        }
    }

    public static DataFrame getGeoIPData(SQLContext sqlContext) {

        URL inputFile = NetworkUtils.class.getResource(GEO_DATA_PARQUET_FILE);

        if (inputFile != null) {
            try {
                return sqlContext.read().format("parquet").load(inputFile.getPath());
            } catch (Exception esc) {
                return getGeoCities(sqlContext);
            }
        } else {
            return getGeoCities(sqlContext);
        }
    }

    public static DataFrame getGeoCities(SQLContext sqlContext) {

        URL ipv4Blocks = NetworkUtils.class.getResource(IPV4_BLOCKS_FILE);
        URL cityLocations = NetworkUtils.class.getResource(CITY_LOCATIONS_FILE);

        if (ipv4Blocks == null || cityLocations == null) {
            if (FileUtils.downloadZipFileAsResource(geoCitiesURL)) {
                ipv4Blocks = NetworkUtils.class.getResource(IPV4_BLOCKS_FILE);
                cityLocations = NetworkUtils.class.getResource(CITY_LOCATIONS_FILE);
            } else {
                return null;
            }
        }

        DataFrame ipv4DF = sqlContext.read()
                .format("com.databricks.spark.csv")
                .schema(getIPv4BlocksSchema())
                .option("header", "true")
                .load(ipv4Blocks.getPath());

        DataFrame ipv4Encoded = encodeNetworkSubnets(ipv4DF, NETWORK_COLUMN).select(NETWORK_COLUMN + SUBNET_LOWER_RANGE_NAME, NETWORK_COLUMN + SUBNET_UPPER_RANGE_NAME, "latitude", "longitude", "geoname_id");

        DataFrame citiesDF = sqlContext.read()
                .format("com.databricks.spark.csv")
                .schema(getCityLocationsSchema())
                .option("header", "true")
                .load(cityLocations.getPath())
                .select("geoname_id", "country_name");


        DataFrame df = ipv4Encoded.join(citiesDF, "geoname_id").coalesce(1);

        String outputFileName = FileUtils.getRootDataPath() + GEO_DATA_PARQUET_FILE;

        df.write().format("parquet").save(outputFileName);

        return df;
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

    public static StructType getCityLocationsSchema() {

        StructField[] fields = new StructField[13];

        fields[0] = new StructField("geoname_id", DataTypes.StringType, true, DataFrameUtils.getMetadata(null, null));
        fields[1] = new StructField("locale_code", DataTypes.StringType, true, DataFrameUtils.getMetadata(FieldMeaning.MeaningType.LOCALE, null));
        fields[2] = new StructField("continent_code", DataTypes.StringType, true, DataFrameUtils.getMetadata(null, null));
        fields[3] = new StructField("continent_name", DataTypes.StringType, true, DataFrameUtils.getMetadata(FieldMeaning.MeaningType.CONTINENT, null));
        fields[4] = new StructField("country_iso_code", DataTypes.StringType, true, DataFrameUtils.getMetadata(null, null));
        fields[5] = new StructField("country_name", DataTypes.StringType, true, DataFrameUtils.getMetadata(FieldMeaning.MeaningType.COUNTRY, null));
        fields[6] = new StructField("subdivision_1_iso_code", DataTypes.StringType, true, DataFrameUtils.getMetadata(FieldMeaning.MeaningType.LOCATION, null));
        fields[7] = new StructField("subdivision_1_name", DataTypes.StringType, true, DataFrameUtils.getMetadata(FieldMeaning.MeaningType.LOCATION, null));
        fields[8] = new StructField("subdivision_2_iso_code", DataTypes.StringType, true, DataFrameUtils.getMetadata(FieldMeaning.MeaningType.LOCATION, null));
        fields[9] = new StructField("subdivision_2_name", DataTypes.StringType, true, DataFrameUtils.getMetadata(FieldMeaning.MeaningType.LOCATION, null));
        fields[10] = new StructField("city_name", DataTypes.StringType, true, DataFrameUtils.getMetadata(FieldMeaning.MeaningType.LOCATION, null));
        fields[11] = new StructField("metro_code", DataTypes.StringType, true, DataFrameUtils.getMetadata(FieldMeaning.MeaningType.LOCATION, null));
        fields[12] = new StructField("time_zone", DataTypes.StringType, true, DataFrameUtils.getMetadata(FieldMeaning.MeaningType.DATE_PART, null));

        return new StructType(fields);
    }

    public static StructType getIPv4BlocksSchema() {

        StructField[] fields = new StructField[9];

        fields[0] = new StructField(NETWORK_COLUMN, DataTypes.StringType, true, DataFrameUtils.getMetadata(FieldMeaning.MeaningType.IPv4_SUBNET, null));
        fields[1] = new StructField("geoname_id", DataTypes.StringType, true, DataFrameUtils.getMetadata(null, null));
        fields[2] = new StructField("registered_country_geoname_id", DataTypes.StringType, true, DataFrameUtils.getMetadata(null, null));
        fields[3] = new StructField("represented_country_geoname_id", DataTypes.StringType, true, DataFrameUtils.getMetadata(null, null));
        fields[4] = new StructField("is_anonymous_proxy", DataTypes.StringType, true, DataFrameUtils.getMetadata(null, null));
        fields[5] = new StructField("is_satellite_provider", DataTypes.StringType, true, DataFrameUtils.getMetadata(null, null));
        fields[6] = new StructField("postal_code", DataTypes.StringType, true, DataFrameUtils.getMetadata(FieldMeaning.MeaningType.POSTAL_CODE, null));
        fields[7] = new StructField("latitude", DataTypes.DoubleType, true, DataFrameUtils.getMetadata(FieldMeaning.MeaningType.LATITUDE, null));
        fields[8] = new StructField("longitude", DataTypes.DoubleType, true, DataFrameUtils.getMetadata(FieldMeaning.MeaningType.LONGITUDE, null));

        return new StructType(fields);
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
        return (long) (((addressBytes[0] & 0xFF) << 24) |
                ((addressBytes[1] & 0xFF) << 16) |
                ((addressBytes[2] & 0xFF) << 8) |
                ((addressBytes[3] & 0xFF) << 0));
    }

    public static boolean netMatch(String testIp, String network) throws UnknownHostException {

        Tuple2<Long, Long> bounds = getSubnetBounds(network);
        long address = getAddress(testIp);
        return bounds._1 <= address && bounds._2 >= address;

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

    private static StructType getUpdatedIPSchema(DataFrame input, List<String> networkColumns) {

        List<StructField> newColumns = new ArrayList<>();

        newColumns.addAll(Arrays.asList(input.schema().fields()));
        for (String networkColumn : networkColumns) {
            newColumns.add(new StructField(networkColumn + NETWORK_INT_NAME, DataTypes.LongType, true, Metadata.empty()));
        }

        return new StructType(newColumns.toArray(new StructField[0]));
    }

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
}
