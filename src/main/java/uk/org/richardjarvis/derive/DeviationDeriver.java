package uk.org.richardjarvis.derive;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import uk.org.richardjarvis.utils.DataFrameUtils;


import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Created by rjarvis on 29/02/16.
 */
public class DeviationDeriver implements DeriveInterface {

    @Override
    public DataFrame derive(DataFrame input) {

        List<Column> numericColumns = DataFrameUtils.getNumericColumns(input);

        List<String> numericColumnStrings = numericColumns.stream().map(column -> column.expr().prettyString()).collect(Collectors.toList());

        JavaRDD<Row> rows = input.select(numericColumns.toArray(new Column[0])).javaRDD();
        int numericColumnLength = numericColumnStrings.size();

        List<Statistics> statisticsList = calculateStats(rows, numericColumnLength);

        return scaleData(input, statisticsList);
    }

    private DataFrame scaleData(DataFrame rows, List<Statistics> statisticsList) {

        List<Column> numericColumns = DataFrameUtils.getNumericColumns(rows);

        int numericColumnCount = numericColumns.size();

        DataFrame output = rows;

        for (int fieldIndex = 0; fieldIndex < numericColumnCount; fieldIndex++) {
            Column valueColumn = numericColumns.get(fieldIndex);
            Statistics stats = statisticsList.get(fieldIndex);
            output = output.withColumn(valueColumn.expr().prettyString() + "_zindex", valueColumn.minus(stats.getMean()).divide(stats.getStandardDeviation()));
        }

        return output;
    }

    /*
     * Calculate the summary statistics for each column
     */
    private List<Statistics> calculateStats(JavaRDD<Row> rows, int numericColumnLength) {

        List<Statistics> zeroArray = new ArrayList<>(numericColumnLength);

        for (int i = 0; i < numericColumnLength; i++) {
            zeroArray.add(new Statistics());
        }

        List<Statistics> statisticsList = rows.aggregate(zeroArray, (statisticses, row) -> {
                    List<Statistics> updatedStats = new ArrayList<>(numericColumnLength);

                    for (int fieldIndex = 0; fieldIndex < numericColumnLength; fieldIndex++) {

                        Statistics statistics = statisticses.get(fieldIndex);
                        Number number = ((Number) (row.get(fieldIndex)));
                        if (number != null) {
                            statistics.addValue(number.doubleValue());
                        }
                        updatedStats.add(statistics);

                    }
                    return updatedStats;
                }, (statisticsesA, statisticsesB) -> {
                    List<Statistics> updatedStats = new ArrayList<>(numericColumnLength);

                    for (int fieldIndex = 0; fieldIndex < numericColumnLength; fieldIndex++) {
                        updatedStats.add(statisticsesA.get(fieldIndex).combine(statisticsesB.get(fieldIndex)));
                    }
                    return updatedStats;
                }
        );

        return statisticsList;
    }


}
