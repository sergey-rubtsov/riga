package open.data.lv.spark;

import org.apache.log4j.PropertyConfigurator;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static org.apache.spark.sql.functions.coalesce;
import static org.apache.spark.sql.functions.count;
import static org.apache.spark.sql.functions.first;
import static org.apache.spark.sql.functions.lag;
import static org.apache.spark.sql.functions.last;
import static org.apache.spark.sql.functions.min;
import static org.apache.spark.sql.functions.pow;
import static org.apache.spark.sql.functions.sqrt;
import static org.apache.spark.sql.functions.udf;
import static org.apache.spark.sql.functions.unix_timestamp;

public class Pipeline {

    private static String MASTER_URL = "local[1]";

    private static List<String> TICKET_VALIDATIONS_FILES = new ArrayList<>();

    private static List<String> VEHICLE_MESSAGES_FILES = new ArrayList<>();

    private static void initPipelineParameters() {
        TICKET_VALIDATIONS_FILES.add("real/TestValidations.csv");
        VEHICLE_MESSAGES_FILES.add("real/TestVehicleMessages.csv");
    }

    private static Dataset<Row> readFiles(SQLContext sqlContext,
                                          List<String> files,
                                          String dateFormat,
                                          String nullValue,
                                          String delimiter
                                                       ) {
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
        Dataset<Row> result = reader
                .csv(Objects.requireNonNull(classLoader.getResource(files.get(0))).getPath());
        if (files.size() > 1) {
            for (int i = 1; i < files.size(); i++) {
                result = result.union(reader
                        .csv(Objects.requireNonNull(classLoader.getResource(files.get(i))).getPath()));
            }
        }
        return result;
    }

    public static void main(String[] args) {
        System.setProperty("hadoop.home.dir", System.getProperty("user.dir") + "\\hadoop");
        System.setProperty("SPARK_CONF_DIR", System.getProperty("user.dir") + "\\conf");
        PropertyConfigurator.configure(System.getProperty("user.dir") + "\\conf\\log4j.properties");

        SparkConf conf = new SparkConf()
                .setMaster(MASTER_URL)
                .setAppName("Riga public transport")
                .set("SPARK_HOME", System.getProperty("user.dir"))
                .set("SPARK_CONF_DIR", System.getProperty("user.dir") + "\\conf");
        SparkSession spark = SparkSession
                .builder()
                .config(conf)
                .getOrCreate();
        SQLContext sqlContext = new SQLContext(spark);
        sqlContext.setConf("spark.sql.caseSensitive", "true");
        initPipelineParameters();
        Dataset<Row> tickets = readFiles(sqlContext, TICKET_VALIDATIONS_FILES, "dd.MM.yyyy HH:mm:ss", null, null);
        Dataset<Row> vehicles = readFiles(sqlContext, VEHICLE_MESSAGES_FILES, "yyyy-MM-dd HH:mm:ss.SSS", "NULL", ";");
    }


}
