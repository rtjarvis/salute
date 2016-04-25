package uk.org.richardjarvis.utils;

import org.apache.commons.io.FileUtils;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.Test;

import java.io.File;
import java.util.Arrays;

/**
 * Created by rjarvis on 25/04/16.
 */
public class DataFrameUtilsTest {

    @Test
    public void unNestColumnNames() throws Exception {


        StructField a = new StructField("a", DataTypes.StringType, true, Metadata.empty());
        StructField b = new StructField("b", DataTypes.StringType, true, Metadata.empty());
        StructField c = new StructField("c", DataTypes.StringType, true, Metadata.empty());
        StructField d = new StructField("d", DataTypes.createArrayType(DataTypes.createArrayType(DataTypes.StringType, true), true), true, Metadata.empty());
        StructField e = new StructField("e", DataTypes.StringType, true, Metadata.empty());


        StructType st = new StructType(Arrays.asList(a, b, c, d, e).toArray(new StructField[0]));

        StructType schema = DataFrameUtils.unNestColumns(st);

        for (StructField s : schema.fields()) {
            System.out.println(s);
        }

    }
    @Test
    public void unNestColumnNames2() throws Exception {

        String outputDir = "/tmp/test_json.json";
        File output = new File(outputDir);
        FileUtils.deleteDirectory(output);

        String testFile = getClass().getResource("/json/test3.json").getFile();


        DataFrame df = SparkProvider.getSQLContext().read().json(testFile);

        StructType schema = DataFrameUtils.unNestColumns(df.schema());

        for (StructField s : schema.fields()) {
            System.out.println(s);
        }
    }
}