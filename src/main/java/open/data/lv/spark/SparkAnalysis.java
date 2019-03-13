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

import java.sql.Date;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.UUID;

//'inner', 'outer', 'full', 'fullouter', 'full_outer', 'leftouter', 'left', 'left_outer', 'rightouter', 'right', 'right_outer', 'leftsemi', 'left_semi', 'leftanti', 'left_anti', 'cross'.
public class SparkAnalysis {

    public static void main(String[] args) {
        System.setProperty("hadoop.home.dir", System.getProperty("user.dir") + "/hadoop");
        ClassLoader classLoader = SparkAnalysis.class.getClassLoader();
        String masterUrl = "local[1]";
        SparkConf conf = new SparkConf().setMaster(masterUrl).setAppName("Riga public transport");
        SparkSession spark = SparkSession
                .builder()
                .config(conf)
                .getOrCreate();
        /* spark.sparkContext().setLogLevel("WARN");
        PropertyConfigurator.configure("conf/log4j.properties");
        Logger.getRootLogger().setLevel(Level.OFF);
        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);*/
        SQLContext sqlContext = new SQLContext(spark);
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
        routes.createOrReplaceTempView("routes");
        tickets.printSchema();
        routes.printSchema();
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
        Dataset<Row> transportEvents = sqlContext.sql("SELECT SentDate, VehicleID, event_type.Code, WGS84Fi, WGS84La FROM routes " +
                "INNER JOIN event_type ON routes.SendingReason=event_type.SendingReason");
        transportEvents = transportEvents.filter(transportEvents.col("Code").eqNullSafe("DoorsClosed"));
        transportEvents.createOrReplaceTempView("transport_events");

        Dataset<Row> validationEvents = sqlContext.sql("SELECT GarNr, vehicles.VehicleID, TMarsruts, Virziens, ValidTalonaId, " +
                "Laiks " +
                "FROM tickets " +
                "INNER JOIN vehicles ON tickets.GarNr=vehicles.VehicleCompanyCode");

        validationEvents = validationEvents
                .filter(validationEvents.col("TMarsruts")
                        .eqNullSafe("Tr 101"));
        validationEvents.createOrReplaceTempView("validation_events");
        Dataset<Row> vehiclesOnRoute = sqlContext.sql("SELECT GarNr as vehicle FROM validation_events GROUP BY GarNr");
        vehiclesOnRoute.createOrReplaceTempView("vehicles_on_route");

        SimpleDateFormat dateFormat = new SimpleDateFormat("dd.MM.yyyy HH:mm:ss");
        UserDefinedFunction time = udf(
                (String s) -> new Timestamp(dateFormat.parse(s).getTime()), DataTypes.TimestampType
        );
        validationEvents = validationEvents.withColumn("time",
                time.apply(validationEvents.col("Laiks"))).drop(validationEvents.col("Laiks"));
        validationEvents.createOrReplaceTempView("validation_events");

        Dataset<Row> consolidated = sqlContext.sql("SELECT validation_events.VehicleID as vId, TMarsruts, Virziens, ValidTalonaId, time, " +
                "SentDate, Code, WGS84Fi, WGS84La " +
                "FROM validation_events " +
                "FULL OUTER JOIN transport_events ON validation_events.VehicleID=transport_events.VehicleID")
                .withColumn("timestamp", coalesce(col("time"), col("SentDate")))
                .drop("time", "SentDate");
        consolidated.createOrReplaceTempView("consolidated_events");
        consolidated = sqlContext.sql("SELECT * FROM consolidated_events INNER JOIN vehicles_on_route " +
                "ON vehicles_on_route.vehicle=consolidated_events.vId " +
                "ORDER BY timestamp");
        consolidated.createOrReplaceTempView("consolidated_events");
        consolidated.show(1000);

        consolidated.coalesce(1).write().option("header", "true").csv(System.getProperty("user.dir") + "/result/" + UUID.randomUUID());

    }
}
