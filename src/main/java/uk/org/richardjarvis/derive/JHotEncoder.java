package uk.org.richardjarvis.derive;

/**
 * Created by rjarvis on 29/02/16.
 */

import org.apache.spark.sql.Column;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class JHotEncoder implements Serializable {

    public static final String OTHER_COLUMN_NAME = "Other";

    private enum JHOT_TYPE {COUNT, SUM, AVG};

    public DataFrame transformCount(DataFrame df, int numberOfCategories, String keyColumnName, String valueColumnName, String columnPrefix) {
        return transform(df, numberOfCategories, keyColumnName, valueColumnName, columnPrefix, JHOT_TYPE.COUNT);
    }

    public DataFrame transformSum(DataFrame df, int numberOfCategories, String keyColumnName, String valueColumnName, String columnPrefix) {
        return transform(df, numberOfCategories, keyColumnName, valueColumnName, columnPrefix, JHOT_TYPE.SUM);
    }

    public DataFrame transformAvg(DataFrame df, int numberOfCategories, String keyColumnName, String valueColumnName, String columnPrefix) {
        return transform(df, numberOfCategories, keyColumnName, valueColumnName, columnPrefix, JHOT_TYPE.AVG);
    }

    private DataFrame transform(DataFrame df, int numberOfCategories, String keyColumnName, String valueColumnName, String columnPrefix, JHOT_TYPE type) {


        if (!columnPrefix.startsWith("_"))
            columnPrefix = "_" + columnPrefix;

        // build new schema
        List<StructField> schemaFields = new ArrayList<>();
        schemaFields.add(new StructField(keyColumnName, DataTypes.StringType, false, null));
        List<StructField> jhotColumns = getJHotColumns(df, numberOfCategories, valueColumnName);
        schemaFields.addAll(jhotColumns);

        DataFrame baseFrame = df.select(keyColumnName).distinct();

        DataFrame group = null;
        switch (type) {
            case AVG:
                group = df.groupBy(df.col(keyColumnName), df.col(valueColumnName)).avg();
                break;
            case COUNT:
                group = df.groupBy(df.col(keyColumnName), df.col(valueColumnName)).count();
                break;
            case SUM:
                group = df.groupBy(df.col(keyColumnName), df.col(valueColumnName)).sum();
                break;
        }

        String[] fieldNames = new String[schemaFields.size()];

        boolean computeOtherColumn= false;
        int counter = 0;
        for (StructField field : jhotColumns) {

            String fieldName = field.name();

            if (!fieldName.equals(OTHER_COLUMN_NAME)) {

                fieldNames[counter++] = fieldName;

                DataFrame filter = group.filter(group.col(valueColumnName).equalTo(fieldName))
                        .withColumnRenamed(keyColumnName, keyColumnName + "_temp")
                        .drop(group.col(valueColumnName));

                filter = renameDataFrameValueColumns(fieldName, filter);

                baseFrame = baseFrame.join(filter, baseFrame.col(keyColumnName).equalTo(filter.col(keyColumnName + "_temp")), "outer")
                        .na().fill(0.0)
                        .drop(keyColumnName + "_temp");
            } else {
                computeOtherColumn= true;
            }
        }

        if (computeOtherColumn) {
            // build "Other" filter
            Column filter = null;
            for (String fieldName : fieldNames) {
                if (fieldName != null && !fieldName.equals(OTHER_COLUMN_NAME)) {
                    if (filter == null) {
                        filter = group.col(valueColumnName).notEqual(fieldName);
                    }
                    filter = filter.and(group.col(valueColumnName).notEqual(fieldName));
                }
            }

            DataFrame other = null;
            switch (type) {
                case AVG:
                    other = df.filter(filter)
                            .groupBy(df.col(keyColumnName)).avg()
                            .withColumnRenamed(keyColumnName, keyColumnName + "_temp")
                            .drop(group.col(valueColumnName));

                    break;
                case COUNT:
                    other = df.filter(filter)
                            .groupBy(df.col(keyColumnName)).count()
                            .withColumnRenamed(keyColumnName, keyColumnName + "_temp")
                            .drop(group.col(valueColumnName));
                    break;
                case SUM:
                    other = df.filter(filter)
                            .groupBy(df.col(keyColumnName)).sum()
                            .withColumnRenamed(keyColumnName, keyColumnName + "_temp")
                            .drop(group.col(valueColumnName));
                    break;
            }

            other = renameDataFrameValueColumns(OTHER_COLUMN_NAME, other);

            baseFrame = baseFrame.join(other, baseFrame.col(keyColumnName).equalTo(other.col(keyColumnName + "_temp")), "outer")
                    .na().fill(0.0)
                    .drop(keyColumnName + "_temp");
        }
        return friendlyColumnNames(baseFrame);

    }

    private DataFrame renameDataFrameValueColumns(String fieldName, DataFrame filter) {
        String[] valueFields = filter.columns();
        for (int i = 1; i < valueFields.length; i++) {
            filter = filter.withColumnRenamed(valueFields[i], fieldName + "_" + valueFields[i]);
        }
        return filter;
    }

    public List<StructField> getJHotColumns(DataFrame df, int numberOfCategories, String valueColumnName) {

        List<StructField> schemaFields = new ArrayList<>();

        DataFrame jh = df.groupBy(valueColumnName).count().sort(new Column("count").desc()).sort(df.col(valueColumnName).asc());

        Row[] jHotColumns = jh.head(numberOfCategories+1); // I request more than the number of categories. If there are that many returned then there is an "Other category" otherwise there is not

        int returnedCategories = (jHotColumns.length < numberOfCategories) ? jHotColumns.length : numberOfCategories;

        for (int i = 0; i < returnedCategories; i++) {
            Row row = jHotColumns[i];
            schemaFields.add(new StructField(row.getString(0), DataTypes.DoubleType, false, Metadata.empty()));
        }

        if (jHotColumns.length > numberOfCategories) {
            schemaFields.add(new StructField(OTHER_COLUMN_NAME, DataTypes.DoubleType, false, Metadata.empty()));
        }
        return schemaFields;

    }

    public static DataFrame friendlyColumnNames(DataFrame df) {

        String[] cols = df.columns();

        for (int i = 0; i < cols.length; i++) {
            String valueColumn = cols[i];
            cols[i] = valueColumn.replace('(', '[').replace(')', ']');
        }
        return df.toDF(cols);

    }




}
