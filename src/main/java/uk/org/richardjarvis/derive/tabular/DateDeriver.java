package uk.org.richardjarvis.derive.tabular;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.GroupedData;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;
import uk.org.richardjarvis.utils.Event;
import uk.org.richardjarvis.utils.SparkProvider;

import static uk.org.richardjarvis.derive.tabular.Aspect.CubeType.*;

import java.util.*;

/**
 * Created by rjarvis on 29/02/16.
 */
public class DateDeriver {

    public DataFrame transform(DataFrame input, String idColumnName, String typeColumnName) {

        List<String> cubeValues = null; //Event.getSummaryMethods();

        JavaRDD<? extends Event> rdd = null; //  TODO

        Map<Aspect, DataFrame> cubeResults = getSummaryCubes(input, typeColumnName, cubeValues);

        List<Aspect> allAspects = new ArrayList<>();

        Map<Aspect, Tuple2<Map<String, Double>, Long>> summaryData = collectSummaries(cubeResults, allAspects);

        List<Aspect> aspects = new ArrayList<>(summaryData.keySet());

        StructType schema = createSchema(idColumnName, aspects, cubeValues);

        System.out.println(schema.fieldNames().length);

        int aspecstSize = aspects.size();

        JavaPairRDD<String, ? extends Iterable<? extends Event>> groupedRows = rdd.groupBy(event -> {
            return event.getId();
        });

        // Go through the person's events
        JavaRDD<Row> r = groupedRows.map(group -> {

            Map<Aspect, Tuple2<Map<String, Double>, Long>> myData = Collections.synchronizedMap(new HashMap<>());

            Iterable<? extends Event> iterator = group._2;

            iterator.forEach(event -> {

                for (String window : event.getWindowList()) {

                    for (String cubeValue : cubeValues) {

                        Aspect aspect = new Aspect(event.getEventType(), window, null, null);
                        Double value = null;
                        try {
                            value = Double.valueOf(BeanUtils.getProperty(event, cubeValue));
                        } catch (Exception e) {
                            value = null;
                        }

                        Tuple2<Map<String, Double>, Long> datum = myData.get(aspect);

                        Map<String, Double> valueMap;
                        Long count = 0l;
                        if (datum == null) {
                            valueMap = new HashMap<>();
                        } else {
                            valueMap = datum._1;
                            count = datum._2;
                        }

                        Double mapValue = valueMap.get(cubeValue);

                        if (mapValue != null) {
                            value += mapValue;
                        }

                        valueMap.put(cubeValue, value);

                        myData.put(aspect, new Tuple2<>(valueMap, count + 1));


                    }
                }

            });


            int cubeValueSize = cubeValues.size();
            // output row is aspects * cubeValues * (SUM | MEAN | COUNT) *  4
            // since there are 4 typse of comparison per aspect
            Object[] dataResult = new Object[aspecstSize * cubeValueSize * 3 * 4 + 1];

            dataResult[0] = group._1; //  add key


            int i = 1;
            for (Aspect aspect : aspects) {

                Tuple2<Map<String, Double>, Long> datum = myData.get(aspect);
                Tuple2<Map<String, Double>, Long> summaryOtherIds = summaryData.get(aspect);

                if (datum != null) {
                    for (String cubeValue : cubeValues) {

                        // SUM -> COUNT -> AVG
                        Double sum = datum._1.get(cubeValue);
                        Long count = datum._2;

                        if (sum == null) {
                            for (int t = 0; t < 4; t++) {
                                dataResult[i++] = 0d;
                            }
                        } else {
                            dataResult[i++] = sum;
                            // INSERT SUM COMPARISONS
                            dataResult[i++] = 0d;
                            dataResult[i++] = 0d;
                            dataResult[i++] = 0d;
                        }

                        if (count == null) {
                            for (int t = 0; t < 4; t++) {
                                dataResult[i++] = 0l;
                            }
                        } else {
                            // INSERT COUNT COMPARISONS
                            dataResult[i++] = count;
                            dataResult[i++] = 0l;
                            dataResult[i++] = 0l;
                            dataResult[i++] = 0l;
                        }

                        if (sum == null || count == null) {
                            for (int t = 0; t < 4; t++) {
                                dataResult[i++] = 0d;
                            }
                        } else {
                            // INSERT AVG COMPARISONS
                            dataResult[i++] = sum / count;
                            dataResult[i++] = 0d;
                            dataResult[i++] = 0d;
                            dataResult[i++] = 0d;
                        }

                    }
                } else {
                    for (int t = 0; t < cubeValueSize; t++) {
                        dataResult[i++] = 0d;
                        dataResult[i++] = 0d;
                        dataResult[i++] = 0d;
                        dataResult[i++] = 0d;
                        dataResult[i++] = 0l;
                        dataResult[i++] = 0l;
                        dataResult[i++] = 0l;
                        dataResult[i++] = 0l;
                        dataResult[i++] = 0d;
                        dataResult[i++] = 0d;
                        dataResult[i++] = 0d;
                        dataResult[i++] = 0d;
                    }
                }
            }

            return RowFactory.create(dataResult);
        });

        return SparkProvider.getSQLContext().createDataFrame(r, schema);
    }

    private Map<Aspect, DataFrame> getSummaryCubes(DataFrame df, String typeColumnName, List<String> cubeValues) {

        Map<Aspect, DataFrame> cubeResults = Collections.synchronizedMap(new HashMap<>());

        List<String> timeWindows = Event.getTimeWindowMethods();

        timeWindows.stream().parallel().forEach(timeWindow -> {

            GroupedData gd = df.cube(df.col(typeColumnName), df.col(timeWindow));

            for (String cubeValue : cubeValues) {
                cubeResults.put(new Aspect(null, timeWindow, cubeValue, SUM), gd.sum(cubeValue));
            }
            cubeResults.put(new Aspect(null, timeWindow, null, COUNT), gd.count());

        });
        return cubeResults;

    }

    private Map<Aspect, Tuple2<Map<String, Double>, Long>> collectSummaries(Map<Aspect, DataFrame> cubeResults, List<Aspect> allAspects) {

        Map<Aspect, Tuple2<Map<String, Double>, Long>> lookup = Collections.synchronizedMap(new HashMap<>());
        List<Aspect> allAspectsLocal = Collections.synchronizedList(new ArrayList<>());

        cubeResults.keySet().stream().forEach(aspect -> {

            List<Row> rows = cubeResults.get(aspect).collectAsList();

            for (Row row : rows) {

                Aspect keyAspect = new Aspect(row.getString(0), row.getString(1), aspect.getValueName(), aspect.getCubeType());
                Aspect lookupAspect = new Aspect(row.getString(0), row.getString(1), null, null);

                allAspectsLocal.add(keyAspect);

                Double sum = null;
                Long count = null;

                switch (aspect.getCubeType()) {
                    case SUM:
                        sum = row.getDouble(2);
                        break;
                    case COUNT:
                        count = row.getLong(2);
                        break;
                }

                Map<String, Double> valueMap;

                Tuple2<Map<String, Double>, Long> summary = lookup.get(lookupAspect);

                if (summary == null) {
                    valueMap = new HashMap<>();
                    count = 0l;
                } else {
                    valueMap = summary._1;
                    if (count != null) {
                        count = count + summary._2;
                    } else {
                        count = summary._2;
                    }
                }

                if (sum != null) {
                    valueMap.put(aspect.getValueName(), sum);
                }

                summary = new Tuple2<>(valueMap, count);

                lookup.put(lookupAspect, summary);
            }

        });

        allAspects.addAll(allAspectsLocal);
        return lookup;
    }

    private StructType createSchema(String idColumnName, List<Aspect> aspects, List<String> cubeValues) {

        List<StructField> fields = new ArrayList<>();
        fields.add(DataTypes.createStructField(idColumnName, DataTypes.StringType, false, Metadata.empty()));

        for (Aspect aspect : aspects) {
            String columnName = aspect.toColumnName();
            if (columnName != null) {
                for (String cubeValue : cubeValues) {
                    String actualName1 = columnName + "_" + cubeValue;
                    for (String aggType : "SUM,AVG".split(",")) {
                        String actualName = actualName1 + "_" + aggType;
                        fields.add(DataTypes.createStructField(actualName, DataTypes.DoubleType, false, Metadata.empty()));
                        fields.add(DataTypes.createStructField(actualName + "_comp_all_ids", DataTypes.DoubleType, false, Metadata.empty()));
                        fields.add(DataTypes.createStructField(actualName + "_comp_all_types_for_id", DataTypes.DoubleType, false, Metadata.empty()));
                        fields.add(DataTypes.createStructField(actualName + "_comp_all_windows_for_id", DataTypes.DoubleType, false, Metadata.empty()));
                    }
                    String actualName = actualName1 + "_COUNT";

                    fields.add(DataTypes.createStructField(actualName, DataTypes.LongType, false, Metadata.empty()));
                    fields.add(DataTypes.createStructField(actualName + "_comp_all_ids", DataTypes.LongType, false, Metadata.empty()));
                    fields.add(DataTypes.createStructField(actualName + "_comp_all_types_for_id", DataTypes.LongType, false, Metadata.empty()));
                    fields.add(DataTypes.createStructField(actualName + "_comp_all_windows_for_id", DataTypes.LongType, false, Metadata.empty()));

                }
            }
        }

        return DataTypes.createStructType(fields);

    }

}

