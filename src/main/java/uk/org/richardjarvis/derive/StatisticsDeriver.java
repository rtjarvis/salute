package uk.org.richardjarvis.derive;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import uk.org.richardjarvis.utils.DataFrameUtils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static scala.collection.JavaConversions.asScalaBuffer;

/**
 * Created by rjarvis on 09/03/16.
 */
public class StatisticsDeriver implements DeriveInterface {

    @Override
    public DataFrame derive(DataFrame input, Statistics statisticsMap) {

        List<String> numericColumns = DataFrameUtils.getNumericColumnsNames(input);

        if (numericColumns.size() > 0) {
            statisticsMap.coallase(calculateNumericStats(input, numericColumns));
        } else {
            statisticsMap.setCount(input.count());
        }

        List<String> stringColumns = DataFrameUtils.getStringColumnsNames(input);

        calculateCategoryStats(input, stringColumns, statisticsMap);

        return input;

    }

    private void calculateCategoryStats(DataFrame input, List<String> columns, Statistics statisticsMap) {

        int stringColumnCount = columns.size();

        for (int fieldIndex = 0; fieldIndex < stringColumnCount; fieldIndex++) {

            String column = columns.get(fieldIndex);

            Row[] rows = input.groupBy(column).count().collect();

            statisticsMap.put(column, getFrequencyStatistics(rows, statisticsMap.getCount()));

        }

    }

    /*
     * Calculate the summary statistics for each column
     */
    private Statistics calculateNumericStats(DataFrame input, List<String> columns) {

        JavaRDD<Row> rows = input.selectExpr(columns.toArray(new String[0])).javaRDD();

        int numericColumnLength = columns.size();

        Statistics zeroArray = new Statistics();

        Statistics statisticsMap = rows.aggregate(zeroArray, (statistics, row) -> {

                    Statistics updatedStats = new Statistics();

                    for (int fieldIndex = 0; fieldIndex < numericColumnLength; fieldIndex++) {

                        String column = columns.get(fieldIndex);
                        ColumnStatistics columnStatistics = statistics.get(column);
                        Number number = ((Number) (row.get(fieldIndex)));
                        if (number != null) {
                            columnStatistics.addValue(number.doubleValue());
                        }
                        updatedStats.put(column, columnStatistics);

                    }
                    return updatedStats;
                }, (statisticsesA, statisticsesB) -> {

                    return statisticsesA.coallase(statisticsesB);
                }
        );

        return statisticsMap;
    }

    private ColumnStatistics getFrequencyStatistics(Row[] rows, Long totalCount) {

        ColumnStatistics statistics = new ColumnStatistics();

        for (Row row : rows) {
            statistics.addToFrequnecyTable(row.getString(0), row.getLong(1));
        }

        statistics.setCount(totalCount);

        return statistics;
    }

}
