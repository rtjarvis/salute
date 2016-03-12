package uk.org.richardjarvis.metadata;

import org.apache.spark.sql.Column;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Created by rjarvis on 09/03/16.
 */
public class Statistics implements Serializable {

    Map<String, FieldStatistics> columnStatisticsMap;
    Long count=0l;
    Long notNullCount=0l;


    public Statistics() {
        columnStatisticsMap = new HashMap<>();
    }


    public Statistics(Long count) {
        this.count = count;
    }

    public FieldStatistics get(Column column) {
        return get(column.expr().prettyString());
    }

    public FieldStatistics get(String column) {
        if (columnStatisticsMap.containsKey(column)) {
            return columnStatisticsMap.get(column);
        } else {
            FieldStatistics fieldStatistics = new FieldStatistics();
            columnStatisticsMap.put(column, fieldStatistics);
            return fieldStatistics;
        }
    }

    public void put(Column column, FieldStatistics fieldStatistics) {
        put(column.expr().prettyString(), fieldStatistics);

    }

    public void put(String column, FieldStatistics fieldStatistics) {
        if (fieldStatistics != null) {
            columnStatisticsMap.put(column, fieldStatistics);
            Long newCount = fieldStatistics.getCount();
            if (newCount!=null && newCount>count)
                count=newCount;

            Long newNotNullCount = fieldStatistics.getNotNullCount();
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

    public void combine(Column column, FieldStatistics fieldStatistics) {
        combine(column.expr().prettyString(), fieldStatistics);
    }

    public void combine(String column, FieldStatistics fieldStatistics) {
        if (fieldStatistics != null) {
            FieldStatistics existingColumn = columnStatisticsMap.get(column);
            if (existingColumn != null) {
                put(column, existingColumn.combine(fieldStatistics));
            } else {
                put(column, fieldStatistics);
            }
        }
    }

    public Statistics coalesce(Statistics statistics) {

        for (String column : statistics.getColumnNames()) {
            combine(column, statistics.get(column));
        }
        return this;
    }

    public void setCount(long count) {
        this.count = count;
    }
}
