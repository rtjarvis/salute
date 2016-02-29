package uk.org.richardjarvis.utils;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;

/**
 * Created by rjarvis on 29/02/16.
 */
public class SparkProvider {

    public static final String APP_NAME = "SALUTE DATA PREPARATION";

    private static SQLContext sqlContext;
    private static JavaSparkContext sparkContext;
    private static SparkConf sparkConf;

    public static SQLContext getSQLContext() {

        if (sqlContext==null)
            sqlContext=new SQLContext(getSparkContext());

        return sqlContext;
    }

    public static JavaSparkContext getSparkContext() {
        if (sparkContext==null) {
            getSparkConfig();
            sparkContext = new JavaSparkContext(sparkConf);

        }
        return sparkContext;
    }

    private static void getSparkConfig() {
        sparkConf = new SparkConf();
        sparkConf.setAppName(APP_NAME);
        sparkConf.setMaster("local");
    }
}
