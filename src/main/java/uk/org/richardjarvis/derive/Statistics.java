package uk.org.richardjarvis.derive;

import org.apache.spark.sql.Column;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Created by rjarvis on 09/03/16.
 */
public class Statistics implements Serializable {

    Map<String, ColumnStatistics> columnStatisticsMap;
    Long count=0l;
    Long notNullCount=0l;


    public Statistics() {
        columnStatisticsMap = new HashMap<>();
    }


    public Statistics(Long count) {
        this.count = count;
    }

    public ColumnStatistics get(Column column) {
        return get(column.expr().prettyString());
    }

    public ColumnStatistics get(String column) {
        if (columnStatisticsMap.containsKey(column)) {
            return columnStatisticsMap.get(column);
        } else {
            ColumnStatistics columnStatistics = new ColumnStatistics();
            columnStatisticsMap.put(column, columnStatistics);
            return columnStatistics;
        }
    }

    public void put(Column column, ColumnStatistics columnStatistics) {
        put(column.expr().prettyString(), columnStatistics);

    }

    public void put(String column, ColumnStatistics columnStatistics) {
        if (columnStatistics != null) {
            columnStatisticsMap.put(column, columnStatistics);
            Long newCount = columnStatistics.getCount();
            if (newCount!=null && newCount>count)
                count=newCount;

            Long newNotNullCount = columnStatistics.getNotNullCount();
            if (newNotNullCount!=null && newNotNullCount>notNullCount)
                notNullCount=newNotNullCount;
        }

    }

    public Set<String> getColumnNames() {
        return columnStatisticsMap.keySet();
    }

    public Long getCount() {
        return count;
    }

    public Long getNotNullCount() {
        return notNullCount;
    }

    public void combine(Column column, ColumnStatistics columnStatistics) {
        combine(column.expr().prettyString(), columnStatistics);
    }

    public void combine(String column, ColumnStatistics columnStatistics) {
        if (columnStatistics != null) {
            ColumnStatistics existingColumn = columnStatisticsMap.get(column);
            if (existingColumn != null) {
                put(column, existingColumn.combine(columnStatistics));
            } else {
                put(column, columnStatistics);
            }
        }
    }

    public Statistics coallase(Statistics statistics) {

        for (String column : statistics.getColumnNames()) {
            combine(column, statistics.get(column));
        }
        return this;
    }

    public void setCount(long count) {
        this.count = count;
    }
}
