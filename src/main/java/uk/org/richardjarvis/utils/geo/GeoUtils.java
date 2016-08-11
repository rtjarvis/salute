package uk.org.richardjarvis.utils.geo;

import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import uk.org.richardjarvis.metadata.text.FieldMeaning;
import uk.org.richardjarvis.utils.DataFrameUtils;
import uk.org.richardjarvis.utils.file.FileUtils;
import uk.org.richardjarvis.utils.network.NetworkUtils;

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
public class GeoUtils {

    private static final String GEO_DATA_PARQUET_FILE = "/Cities";
    private static final String CITY_LOCATIONS_FILE = "/worldcitiespop.txt.gz";
    private static final String GEO_DATA_PARQUET_FILE_FORMAT = "parquet";
    private static URL citiesURL;
    private static DataFrame geoDataFrame;

    static {
        try {
            citiesURL = new URL("http://download.maxmind.com/download/worldcities/worldcitiespop.txt.gz");
        } catch (MalformedURLException e) {
            e.printStackTrace();
        }
    }

    public static String getKeyColumn() {
        return "ARGH";
    }

    /**
     * @param sqlContext the SQLContext used to load the GeoIP lookup file
     * @return the DataFrame representing the GeoIP lookup file
     */
    public static DataFrame getCitiesDataFrame(SQLContext sqlContext) {

        URL inputFile = GeoUtils.class.getResource(GEO_DATA_PARQUET_FILE);

        if (inputFile != null) {
            try {
                return sqlContext
                        .read()
                        .format(GEO_DATA_PARQUET_FILE_FORMAT)
                        .load(inputFile.getPath());
            } catch (Exception esc) {
                return createGeoCitiesDataFrame(sqlContext);
            }
        } else {
            return createGeoCitiesDataFrame(sqlContext);
        }
    }

    public static DataFrame createGeoCitiesDataFrame(SQLContext sqlContext) {

        if (geoDataFrame != null)
            return geoDataFrame;

        URL cityLocations = GeoUtils.class.getResource(CITY_LOCATIONS_FILE);

        if (cityLocations == null) {
            if (FileUtils.downloadZipFileAsResource(citiesURL)) {
                cityLocations = GeoUtils.class.getResource(CITY_LOCATIONS_FILE);
            } else {
                return null;
            }
        }

        DataFrame rawDF = sqlContext.read()
                .format("com.databricks.spark.csv")
                .schema(getCityLocationsSchema())
                .option("header", "true")
                .load(cityLocations.getPath());

        geoDataFrame = addGeoHash(rawDF, "latitude", "longitude");

        String outputFileName = FileUtils.getRootDataPath() + GEO_DATA_PARQUET_FILE;
        geoDataFrame.write().format(GEO_DATA_PARQUET_FILE_FORMAT).save(outputFileName);

        return geoDataFrame;
    }

    public static DataFrame addGeoHash(DataFrame df, String latitudeColumnName, String longitudeColumnName) {

        int latColIdx = df.schema().fieldIndex(latitudeColumnName);
        int longColIdx = df.schema().fieldIndex(longitudeColumnName);

        JavaRDD<Row> rows = df.javaRDD().map(row -> {
            Object[] o = new Object[row.size() + 1];
            for (int i = 0; i < row.size(); i++) {
                o[i] = row.get(i);
            }
            o[row.size()] = latColIdx +", " + longColIdx;
            return RowFactory.create(o);
        });

        StructType newSchema = df.schema();
        newSchema.add(new StructField("geohash", DataTypes.StringType, true, DataFrameUtils.getMetadata(FieldMeaning.MeaningType.LOCATION, null)));
        return df.sqlContext().createDataFrame(rows, newSchema);

    }

    public static StructType getCityLocationsSchema() {

        StructField[] fields = new StructField[7];
        fields[0] = new StructField("country", DataTypes.StringType, true, DataFrameUtils.getMetadata(FieldMeaning.MeaningType.COUNTRY, null));
        fields[1] = new StructField("city", DataTypes.StringType, true, DataFrameUtils.getMetadata(FieldMeaning.MeaningType.LOCATION, null));
        fields[2] = new StructField("accent_city", DataTypes.StringType, true, DataFrameUtils.getMetadata(FieldMeaning.MeaningType.LOCATION, null));
        fields[3] = new StructField("region", DataTypes.StringType, true, DataFrameUtils.getMetadata(FieldMeaning.MeaningType.LOCATION, null));
        fields[4] = new StructField("population", DataTypes.LongType, true, DataFrameUtils.getMetadata(null, null));
        fields[5] = new StructField("latitude", DataTypes.DoubleType, true, DataFrameUtils.getMetadata(FieldMeaning.MeaningType.LATITUDE, null));
        fields[6] = new StructField("longitude", DataTypes.DoubleType, true, DataFrameUtils.getMetadata(FieldMeaning.MeaningType.LONGITUDE, null));

        return new StructType(fields);
    }


}
