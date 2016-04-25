package uk.org.richardjarvis.utils.network;

import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import uk.org.richardjarvis.metadata.text.FieldMeaning;
import uk.org.richardjarvis.utils.DataFrameUtils;
import uk.org.richardjarvis.utils.file.FileUtils;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Provides utils to geocode an IP address
 */
public class GeoIPUtils {

    public static final String NETWORK_COLUMN = "network";
    private static final String GEO_DATA_PARQUET_FILE = "/GeoLite2";
    private static final String IPV4_BLOCKS_FILE = "/GeoLite2-City-Blocks-IPv4.csv";
    private static final String CITY_LOCATIONS_FILE = "/GeoLite2-City-Locations-en.csv";
    private static final int NUM_GEO_PARTITIONS = 100;
    private static final String GEO_DATA_PARQUET_INDX_FILE = "/GeoLite2_index";
    private static final String GEO_DATA_PARQUET_FILE_FORMAT = "parquet";
    private static URL geoCitiesURL;
    private static List<SubnetRange> partitionIndex;
    private static List<Integer> usefulFieldIndicies = new ArrayList<>();
    private static DataFrame geoIPDataFrame;

    static {
        try {
            geoCitiesURL = new URL("http://geolite.maxmind.com/download/geoip/database/GeoLite2-City-CSV.zip");
        } catch (MalformedURLException e) {
            e.printStackTrace();
        }
    }

    public static String getKeyColumn() {
        return NETWORK_COLUMN + NetworkUtils.SUBNET_UPPER_RANGE_NAME;
    }

    /**
     * @param sqlContext the SQLContext used to load the GeoIP lookup file
     * @return the DataFrame representing the GeoIP lookup file
     */
    public static DataFrame getGeoIPDataFrame(SQLContext sqlContext) {

        URL inputFile = GeoIPUtils.class.getResource(GEO_DATA_PARQUET_FILE);

        if (inputFile != null) {
            try {
                loadGeoFileIndex();
                return sqlContext
                        .read()
                        .format(GEO_DATA_PARQUET_FILE_FORMAT)
                        .load(inputFile.getPath())
                        .select(NETWORK_COLUMN + NetworkUtils.SUBNET_LOWER_RANGE_NAME, NETWORK_COLUMN + NetworkUtils.SUBNET_UPPER_RANGE_NAME, "country_name", "city_name", "is_anonymous_proxy", "is_satellite_provider", "postal_code", "latitude", "longitude");
            } catch (Exception esc) {
                return createGeoCitiesDataFrame(sqlContext);
            }
        } else {
            return createGeoCitiesDataFrame(sqlContext);
        }
    }

    public static DataFrame createGeoCitiesDataFrame(SQLContext sqlContext) {

        if (geoIPDataFrame != null)
            return geoIPDataFrame;

        URL ipv4Blocks = GeoIPUtils.class.getResource(IPV4_BLOCKS_FILE);
        URL cityLocations = GeoIPUtils.class.getResource(CITY_LOCATIONS_FILE);

        if (ipv4Blocks == null || cityLocations == null) {
            if (FileUtils.downloadZipFileAsResource(geoCitiesURL)) {
                ipv4Blocks = GeoIPUtils.class.getResource(IPV4_BLOCKS_FILE);
                cityLocations = GeoIPUtils.class.getResource(CITY_LOCATIONS_FILE);
            } else {
                return null;
            }
        }

        DataFrame ipv4DF = sqlContext.read()
                .format("com.databricks.spark.csv")
                .schema(getIPv4BlocksSchema())
                .option("header", "true")
                .load(ipv4Blocks.getPath());

        DataFrame ipv4Encoded = NetworkUtils.encodeNetworkSubnets(ipv4DF, NETWORK_COLUMN);

        DataFrame citiesDF = sqlContext.read()
                .format("com.databricks.spark.csv")
                .schema(getCityLocationsSchema())
                .option("header", "true")
                .load(cityLocations.getPath());

        geoIPDataFrame = ipv4Encoded.join(citiesDF, "geoname_id");

        Column keyColumn = geoIPDataFrame.col(getKeyColumn());
        geoIPDataFrame = geoIPDataFrame.orderBy(keyColumn).coalesce(NUM_GEO_PARTITIONS);

        String outputFileName = FileUtils.getRootDataPath() + GEO_DATA_PARQUET_FILE;

        geoIPDataFrame.write().format(GEO_DATA_PARQUET_FILE_FORMAT).save(outputFileName);

        geoIPDataFrame = sqlContext.read().format(GEO_DATA_PARQUET_FILE_FORMAT).load(outputFileName);

        try {
            saveGeoFileIndex(geoIPDataFrame);
        } catch (IOException e) {
            e.printStackTrace();
        }

        return geoIPDataFrame;
    }

    public static void loadGeoFileIndex() {
        try {
            ObjectMapper mapper = new ObjectMapper();

            JavaType type = mapper.getTypeFactory().constructCollectionType(List.class, SubnetRange.class);
            partitionIndex = mapper.readValue(new File(FileUtils.getRootDataPath() + GEO_DATA_PARQUET_INDX_FILE), type);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void saveGeoFileIndex(DataFrame df) throws IOException {

        int minSubnetIndex = df.schema().fieldIndex(NETWORK_COLUMN + NetworkUtils.SUBNET_LOWER_RANGE_NAME);
        int maxSubnetIndex = df.schema().fieldIndex(NETWORK_COLUMN + NetworkUtils.SUBNET_UPPER_RANGE_NAME);

        JavaRDD<SubnetRange> maxNetworkInPartion = df.javaRDD().mapPartitionsWithIndex((index, rowIterator) -> {

            Long max = null;
            Long min = null;
            Long count = 0l;
            while (rowIterator.hasNext()) {
                Row row = rowIterator.next();
                Long newMin = row.getLong(minSubnetIndex);
                Long newMax = row.getLong(maxSubnetIndex);

                if (min == null || newMin < min)
                    min = newMin;
                if (max == null || newMax > max)
                    max = newMax;
                count++;
            }

            return Arrays.asList(new SubnetRange(min, max, index)).iterator();
        }, true);

        partitionIndex = maxNetworkInPartion.collect();

        new ObjectMapper().writeValue(new File(FileUtils.getRootDataPath() + GEO_DATA_PARQUET_INDX_FILE), partitionIndex);
    }

    public static Integer getPartitionThatContainsIP(long ipAddress) {

        for (SubnetRange range : partitionIndex) {
            if (range.getMin() <= ipAddress && range.getMax() >= ipAddress)
                return range.getPartitionIndex();
        }
        return null;
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

    /**
     * Holds information on a Subnet (min and max address and contained partition in lookup DataFrame)
     */
    public static class SubnetRange implements Serializable {
        Long max;
        Long min;
        Integer partitionIndex;

        public SubnetRange(Long min, Long max, Integer partitionIndex) {
            this.max = max;
            this.min = min;
            this.partitionIndex = partitionIndex;

        }

        public SubnetRange() {
        }

        public SubnetRange(Long min, Long max) {
            this.max = max;
            this.min = min;
        }

        public Integer getPartitionIndex() {
            return partitionIndex;
        }

        public void setPartitionIndex(Integer partitionIndex) {
            this.partitionIndex = partitionIndex;
        }

        public Long getMax() {
            return max;
        }

        public void setMax(Long max) {
            this.max = max;
        }

        public Long getMin() {
            return min;
        }

        public void setMin(Long min) {
            this.min = min;
        }

    }


}
