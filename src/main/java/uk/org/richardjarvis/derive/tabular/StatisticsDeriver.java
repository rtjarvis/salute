package uk.org.richardjarvis.derive.tabular;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import uk.org.richardjarvis.metadata.text.FieldMeaning;
import uk.org.richardjarvis.metadata.text.FieldStatistics;
import uk.org.richardjarvis.metadata.text.Statistics;
import uk.org.richardjarvis.metadata.text.TabularMetaData;
import uk.org.richardjarvis.utils.DataFrameUtils;

import java.util.Arrays;
import java.util.List;

/**
 * Calculates statistics on input columns. Provides no transformation
 */
public class StatisticsDeriver implements TabularDeriveInterface {

    private static final int MAX_CARDINALITY = 10;

    /**
     *
     * @param input the input dataframe
     * @param metaData the metadata that describes the input dataframe
     * @return the input DataFrame
     */
    @Override
    public DataFrame derive(DataFrame input, TabularMetaData metaData) {

        List<String> numericColumns = DataFrameUtils.getColumnsNames(DataFrameUtils.getColumnsOfMeaning(input, FieldMeaning.MeaningType.NUMERIC));
        List<String> stringColumns = DataFrameUtils.getColumnsNames(DataFrameUtils.getColumnsOfMeaning(input, FieldMeaning.MeaningType.TEXT));

        if (numericColumns.size()==0 && stringColumns.size()==0)
            return input;

        if (numericColumns.size() > 0) {
            metaData.getStatistics().coalesce(calculateNumericStats(input, numericColumns));
        } else {
            metaData.getStatistics().setCount(input.count());
        }

        calculateCategoryStats(input, stringColumns, metaData.getStatistics());

        return input;

    }

    private void calculateCategoryStats(DataFrame input, List<String> columns, Statistics statisticsMap) {

        int stringColumnCount = columns.size();

        for (int fieldIndex = 0; fieldIndex < stringColumnCount; fieldIndex++) {

            String column = columns.get(fieldIndex);


            Row[] result = input.groupBy(column).count().sort(new Column("count").desc()).sort(input.col(column).asc()).head(MAX_CARDINALITY + 1); // I request more than the number of categories. If there are that many returned then there is an "Other category" otherwise there is not

            int returnedCategories = (result.length < MAX_CARDINALITY) ? result.length : MAX_CARDINALITY;

            Row[] rows = Arrays.copyOfRange(result, 0, returnedCategories);

            FieldStatistics fieldStatistics = getFrequencyStatistics(rows, statisticsMap.getCount());

            statisticsMap.put(column, fieldStatistics);

            if (result.length > MAX_CARDINALITY) {
                Long count = getTotalCount(rows);
                fieldStatistics.addToFrequnecyTable("_OTHER_",statisticsMap.getCount()-count);
            }

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
                        FieldStatistics fieldStatistics = statistics.get(column);
                        Number number = ((Number) (row.get(fieldIndex)));
                        if (number != null) {
                            fieldStatistics.addValue(number.doubleValue());
                        }
                        updatedStats.put(column, fieldStatistics);

                    }
                    return updatedStats;
                }, (statisticsesA, statisticsesB) -> {

                    return statisticsesA.coalesce(statisticsesB);
                }
        );

        return statisticsMap;
    }

    private FieldStatistics getFrequencyStatistics(Row[] rows, Long totalCount) {

        FieldStatistics statistics = new FieldStatistics();

        for (Row row : rows) {
            statistics.addToFrequnecyTable(row.getString(0), row.getLong(1));
        }

        statistics.setCount(totalCount);

        return statistics;
    }


    private Long getTotalCount(Row[] rows) {

        Long count = 0l;
        for (Row row : rows) {
            count += row.getLong(1);
        }
        return count;
    }

}
