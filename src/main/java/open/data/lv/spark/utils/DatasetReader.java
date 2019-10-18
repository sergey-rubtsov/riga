package open.data.lv.spark.utils;

import open.data.lv.spark.Pipeline;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

import java.util.List;
import java.util.Objects;

public class DatasetReader {

    public static Dataset<Row> readFiles(SQLContext sqlContext,
                                         String file,
                                         String dateFormat,
                                         String nullValue,
                                         String delimiter) {
        ClassLoader classLoader = Pipeline.class.getClassLoader();
        DataFrameReader reader = sqlContext.read().option("inferSchema", "true").option("header", "true");
        if (!Objects.isNull(nullValue)) {
            reader = reader.option("nullValue", nullValue);
        }
        if (!Objects.isNull(delimiter)) {
            reader = reader.option("delimiter", delimiter);
        }
        if (!Objects.isNull(dateFormat)) {
            reader = reader.option("dateFormat", dateFormat);
        }
        return reader
                .csv(Objects.requireNonNull(classLoader.getResource(file)).getPath());
    }

    public static Dataset<Row> readFiles(SQLContext sqlContext,
                                         List<String> files,
                                         String dateFormat,
                                         String nullValue,
                                         String delimiter) {
        Dataset<Row> result = readFiles(sqlContext, files.get(0), dateFormat, nullValue, delimiter);
        if (files.size() > 1) {
            for (int i = 1; i < files.size(); i++) {
                result = result.union(readFiles(sqlContext, files.get(i), dateFormat, nullValue, delimiter));
            }
        }
        return result;
    }

}
