package open.data.lv.spark;

import org.apache.log4j.PropertyConfigurator;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Row;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.*;
import org.apache.log4j.Logger;
import org.apache.log4j.Level;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.collection.immutable.HashSet;

import java.sql.Date;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;

//'inner', 'outer', 'full', 'fullouter', 'full_outer', 'leftouter', 'left', 'left_outer', 'rightouter', 'right', 'right_outer', 'leftsemi', 'left_semi', 'leftanti', 'left_anti', 'cross'.
public class SparkAnalysis {

    public static void main(String[] args) {
        Logger.getRootLogger().setLevel(Level.OFF);
        System.setProperty("hadoop.home.dir", System.getProperty("user.dir") + "/hadoop");
        System.setProperty("SPARK_CONF_DIR", System.getProperty("user.dir") + "/conf");
        PropertyConfigurator.configure(System.getProperty("user.dir") + "conf/log4j.properties");
        //SPARK_CONF_DIR
        //SPARK_HOME
        String masterUrl = "local[1]";
        SparkConf conf = new SparkConf()
                .setMaster(masterUrl)
                .setAppName("Riga public transport")
                .set("SPARK_HOME", System.getProperty("user.dir"))
                .set("SPARK_CONF_DIR", System.getProperty("user.dir") + "/conf");
        SparkSession spark = SparkSession
                .builder()
                .config(conf)
                .getOrCreate();
        Logger.getRootLogger().setLevel(Level.OFF);

        /*
        spark.sparkContext().setLogLevel("ERROR");


        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);*/
        SQLContext sqlContext = new SQLContext(spark);
        sqlContext.setConf("spark.sql.caseSensitive", "true");
        //Dataset<Row> df1Map = df1.select(functions.concat(df1.col("beginTime"), df1.col("endTime")));
        //df1Map.show();
        readDataAndCreateViews(sqlContext);
        //analyzeData(sqlContext);
        processData(sqlContext);
    }

    private static void analyzeData(SQLContext sqlContext) {
        //sqlContext.sql("SELECT COUNT(ValidTalonaId) FROM tickets GROUP BY ValidTalonaId").show(1000);
        System.out.println("Number of unique passengers");
        //sqlContext.sql("SELECT COUNT(ValidTalonaId), GarNr FROM tickets GROUP BY GarNr").show(1000);
        System.out.println("Number of passengers for each Garage Number");
        //sqlContext.sql("SELECT TMarsruts, GarNr FROM tickets GROUP BY TMarsruts, GarNr ORDER BY TMarsruts").show(1000);
        System.out.println("Routes for each Garage Number");
        Dataset<Row> dataset0 = sqlContext.sql("SELECT GarNr, TMarsruts FROM tickets GROUP BY TMarsruts, GarNr ORDER BY GarNr");
        dataset0.show(100);
        Dataset<Row> dataset1 = sqlContext.sql("SELECT * FROM SELECT GarNr, TMarsruts FROM tickets GROUP BY GarNr, TMarsruts ORDER BY GarNr");
        dataset1.show(100);
    }

    private static void processData(SQLContext sqlContext) {
        Dataset<Row> transportEvents = sqlContext.sql("SELECT SentDate, VehicleID, event_type.Code, WGS84Fi, WGS84La FROM routes " +
                "INNER JOIN event_type ON routes.SendingReason=event_type.SendingReason");
        transportEvents = transportEvents
                .filter(transportEvents.col("Code").equalTo("DoorsClosed").or(
                        transportEvents.col("Code").equalTo("DoorsOpen")));
        transportEvents.createOrReplaceTempView("transport_events");
        Dataset<Row> validationEvents = sqlContext.sql("SELECT GarNr as GN, TMarsruts, Virziens, ValidTalonaId, " +
                "Laiks " +
                "FROM tickets");
        validationEvents = validationEvents
                .filter(validationEvents.col("TMarsruts")
                        .eqNullSafe("Tr 101"));
        UserDefinedFunction garageNumber = udf(
                (Integer i) -> {
                    while (i > 9999) {
                        i = i / 10;
                    }
                    return i;
                }, DataTypes.IntegerType
        );
        validationEvents = validationEvents.withColumn("GarNr",
                garageNumber.apply(validationEvents.col("GN"))).drop("GN");
        SimpleDateFormat dateFormat = new SimpleDateFormat("dd.MM.yyyy HH:mm:ss");
        UserDefinedFunction time = udf(
                (String s) -> new Timestamp(dateFormat.parse(s).getTime()), DataTypes.TimestampType
        );
        validationEvents = validationEvents.withColumn("time",
                time.apply(validationEvents.col("Laiks"))).drop(validationEvents.col("Laiks"));
        validationEvents.createOrReplaceTempView("validation_events");

        Dataset<Row> vehiclesOnRoute = sqlContext
                .sql("SELECT first(TMarsruts) as route, GarNr, vehicles.VehicleID, min(time) as first_time, max(time) as last_time, count(time) as events FROM validation_events " +
                "INNER JOIN vehicles ON validation_events.GarNr=vehicles.VehicleCompanyCode GROUP BY GarNr, vehicles.VehicleID ORDER BY first_time");
        //In Validations table for 'Tr 101' we have 119 records garage numbers of vehicles,
        //in Vehicles table we have only 93 records with connected VehicleID
        vehiclesOnRoute.createOrReplaceTempView("vehicles_on_route");
        long events = vehiclesOnRoute.agg(sum(col("events"))).first().getLong(0);
        System.out.println("Number of events: " + events);
        StructType s1 = validationEvents.schema();
        for(StructField e : s1.fields()) {
            if (!Arrays.asList(transportEvents.schema().fieldNames()).contains(e)) {
                transportEvents = transportEvents
                        .withColumn(e.name(),
                                lit(null));
            }
        }
        StructType s0 = transportEvents.schema();
        for(StructField e : s0.fields()) {
            if (!Arrays.asList(validationEvents.schema().fieldNames()).contains(e)) {
                validationEvents = validationEvents
                        .withColumn(e.name(),
                                lit(null));
            }
        }
        for(StructField e : s1.fields()) {
            transportEvents = transportEvents.withColumn(e.name(),
                  transportEvents.col(e.name()).cast(e.dataType().typeName()));


        }
        for(StructField e : s0.fields()) {
            validationEvents = validationEvents.withColumn(e.name(),
                    validationEvents.col(e.name()).cast(e.dataType()));
        }

        //StructType s3 = new StructType(Stream.of(s0.fields(), s1.fields()).flatMap(Stream::of).toArray(StructField[]::new));
        //List<Row> rows = new ArrayList<>();
        //Dataset<Row> df = sqlContext.sparkSession().createDataFrame(rows, s1);
        transportEvents = transportEvents.union(validationEvents);
        transportEvents.createOrReplaceTempView("transport_events");

        Dataset<Row> transportEventsBetweenFirstAndLastValidation =
                sqlContext.sql("SELECT DISTINCT * FROM transport_events LEFT JOIN vehicles_on_route ON " +
                        "vehicles_on_route.VehicleID=transport_events.VehicleID WHERE SentDate <= last_time AND SentDate >= first_time");
        transportEventsBetweenFirstAndLastValidation = transportEventsBetweenFirstAndLastValidation
                .withColumn("timestamp", coalesce(col("time"), col("SentDate")))
                .drop("time", "SentDate");
        transportEventsBetweenFirstAndLastValidation = transportEventsBetweenFirstAndLastValidation
                .sort(transportEventsBetweenFirstAndLastValidation.col("timestamp"));
        transportEventsBetweenFirstAndLastValidation.createOrReplaceTempView("transport_events");
        //StructType st3 = new StructType(new StructField[] {new StructField("name", DataTypes.StringType, true, null), new StructField("age", DataTypes.IntegerType, true, null)});
        //StructType validationSchema = new StructType(validationEvents.schema().fields());

        //df.union(transportEventsBetweenFirstAndLastValidation);
        //df.union(validationEvents);
        transportEventsBetweenFirstAndLastValidation.printSchema();
        transportEventsBetweenFirstAndLastValidation.show(100);
//        Dataset<Row> consolidated = sqlContext.sql("SELECT " +
//                "TMarsruts, Virziens, ValidTalonaId, time, SentDate, Code, WGS84Fi, WGS84La " +
//                "FROM validation_events " +
//                "FULL OUTER JOIN transport_events ON transport_events.GarNr=validation_events.GarNr")
//                .withColumn("timestamp", coalesce(col("time"), col("SentDate")))
//                .drop("time", "SentDate");
//        consolidated.createOrReplaceTempView("consolidated_events");
//        consolidated = sqlContext.sql("SELECT * FROM consolidated_events " +
//                "ORDER BY timestamp");
//        consolidated.createOrReplaceTempView("consolidated_events");
//
//        //consolidated.show(1000);
        transportEventsBetweenFirstAndLastValidation.coalesce(1).write()
                .option("header", "true").csv(System.getProperty("user.dir") + "/result/" + UUID.randomUUID());
    }

    private static void readDataAndCreateViews(SQLContext sqlContext) {
        ClassLoader classLoader = SparkAnalysis.class.getClassLoader();
        String valFile = "real/ValidDati23_11_18.txt";
        Dataset<Row> tickets = sqlContext.read()
                .option("inferSchema", "true")
                .option("header", "true")
                .option("dateFormat","dd.MM.yyyy HH:mm:ss")
                .csv(classLoader.getResource(valFile).getPath());
        tickets.createOrReplaceTempView("tickets");
        Dataset<Row> routes = sqlContext.read()
                .option("inferSchema", "true")
                .option("header", "true")
                .option("delimiter", ";")
                .option("dateFormat","yyyy-MM-dd HH:mm:ss.SSS")
                .csv(classLoader.getResource("real/VehicleMessages20181123d1.csv").getPath());

/*        routes.union(sqlContext.read()
                .option("inferSchema", "true")
                .option("header", "true")
                .option("delimiter", ";")
                .option("dateFormat","yyyy-MM-dd HH:mm:ss.SSS")
                .csv(classLoader.getResource("real/VehicleMessages20181123d2.csv").getPath()));*/

        routes.createOrReplaceTempView("routes");
        Dataset<Row> vehicles = sqlContext.read()
                .option("header", "true")
                .option("delimiter", ";")
                .csv(classLoader.getResource("Vehicles.csv").getPath());
        vehicles.createOrReplaceTempView("vehicles");
        Dataset<Row> eventType = sqlContext.read()
                .option("header", "true")
                .option("delimiter", ";")
                .csv(classLoader.getResource("SendingReasons.csv").getPath());
        eventType.createOrReplaceTempView("event_type");
    }
}
