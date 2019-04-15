package open.data.lv.spark;

import org.apache.log4j.PropertyConfigurator;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.*;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.*;
import static org.apache.spark.sql.types.DataTypes.StringType;

import org.apache.log4j.Logger;
import org.apache.log4j.Level;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.collection.Seq;
import scala.collection.immutable.Set;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

/**
 * SELECT latitude, longitude, SQRT(
 *     POW(69.1 * (latitude - [startlat]), 2) +
 *     POW(69.1 * ([startlng] - longitude) * COS(latitude / 57.3), 2)) AS distance
 * FROM TableName HAVING distance < 25 ORDER BY distance;
 */
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
        readEventsDataAndCreateViews(sqlContext);
        readGTFSDataAndCreateViews(sqlContext);
        processGTFSData(sqlContext);
        processEventsData(sqlContext);
    }

    private static void readGTFSDataAndCreateViews(SQLContext sqlContext) {
        ClassLoader classLoader = SparkAnalysis.class.getClassLoader();
        Dataset<Row> routes = sqlContext.read()
                .option("inferSchema", "true")
                .option("header", "true")
                .option("delimiter", ",")
                .option("dateFormat","HH:mm:ss")
                .csv(classLoader.getResource("real/GTFS/routes.txt").getPath());
        routes.createOrReplaceTempView("gtfs_routes");
        Dataset<Row> stop_times = sqlContext.read()
                .option("inferSchema", "true")
                .option("header", "true")
                .option("delimiter", ",")
                .option("dateFormat","HH:mm:ss")
                .csv(classLoader.getResource("real/GTFS/stop_times.txt").getPath());
        stop_times.createOrReplaceTempView("stop_times");
        Dataset<Row> stops = sqlContext.read()
                .option("inferSchema", "true")
                .option("header", "true")
                .option("delimiter", ",")
                .option("dateFormat","HH:mm:ss")
                .csv(classLoader.getResource("real/GTFS/stops.txt").getPath());
        stops.createOrReplaceTempView("stops");
        Dataset<Row> trips = sqlContext.read()
                .option("inferSchema", "true")
                .option("header", "true")
                .option("delimiter", ",")
                .option("dateFormat","HH:mm:ss")
                .csv(classLoader.getResource("real/GTFS/trips.txt").getPath());
        trips.createOrReplaceTempView("trips");
        Dataset<Row> type = sqlContext.read()
                .option("inferSchema", "true")
                .option("header", "true")
                .option("delimiter", ",")
                .csv(classLoader.getResource("real/GTFS/route_types.txt").getPath());
        type.createOrReplaceTempView("route_types");
    }

    /**
     +-----+--------------------+--------+--------+
     |route|           stop_name|stop_lat|stop_lon|
     +-----+--------------------+--------+--------+
     |  A 1|        Abrenes iela|56.94606|24.13001|
     |  A 1|        Merķeļa iela|56.94945|24.11884|
     |  A 1|       Tērbatas iela|56.95303|24.11615|
     */
    private static void processGTFSData(SQLContext sqlContext) {
        Dataset<Row> schedule = sqlContext
                .sql("SELECT CONCAT(route_types.short_name, ' ', route_short_name) AS route, direction_id, " +
                        "trips.route_id, stop_name, stop_lat, stop_lon, arrival_time, departure_time FROM stop_times " +
                        "INNER JOIN stops ON stop_times.stop_id=stops.stop_id " +
                        "INNER JOIN trips on stop_times.trip_id=trips.trip_id " +
                        "INNER JOIN gtfs_routes on gtfs_routes.route_id=trips.route_id " +
                        "INNER JOIN route_types ON route_types.route_type=gtfs_routes.route_type");
        //we use only one direction
        schedule = schedule.filter(col("direction_id").eqNullSafe(0));
        schedule = schedule.groupBy(col("route"),
                col("stop_name"),
                col("stop_lat"),
                col("stop_lon")).agg(min(col("arrival_time")).as("arrival_time"))
                .orderBy(
                        col("route"),
                        col("arrival_time")).drop(col("arrival_time"));
        schedule.createOrReplaceTempView("schedule");
    }

    private static void processEventsData(SQLContext sqlContext) {
        //Gyro, Speed
        Dataset<Row> transportEvents = sqlContext
                .sql("SELECT SentDate, VehicleID, event_type.Code, " +
                        "WGS84Fi, WGS84La, Odometer, Delay, ShiftID, TripID FROM routes " +
                        "INNER JOIN event_type ON routes.SendingReason=event_type.SendingReason");
        transportEvents = transportEvents
                .filter(transportEvents.col("Code").equalTo("DoorsOpen"));
        //.or(transportEvents.col("Code").equalTo("DoorsClosed")));
        //.or(transportEvents.col("Code").equalTo("Periodical")));
        transportEvents.createOrReplaceTempView("transport_events");
        Dataset<Row> validationEvents = sqlContext
                .sql("SELECT GarNr as GN, TMarsruts, Virziens, ValidTalonaId, Laiks FROM tickets");
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
                        "INNER JOIN vehicles ON validation_events.GarNr=vehicles.VehicleCompanyCode "
                        + "GROUP BY GarNr, vehicles.VehicleID ORDER BY first_time");
        //In Validations table for 'Tr 101' we have 119 records garage numbers of vehicles,
        //in Vehicles table we have only 93 records with connected VehicleID
        vehiclesOnRoute.createOrReplaceTempView("vehicles_on_route");
        long events = vehiclesOnRoute.agg(sum(col("events"))).first().getLong(0);
        System.out.println("Number of events: " + events);
        Seq<String> vehicleIDColumn =  new Set.Set1<>("VehicleID").toSeq();
        transportEvents = transportEvents
                .join(vehiclesOnRoute, vehicleIDColumn, "inner");
        transportEvents = transportEvents.withColumn("timestamp_of_transport_event", transportEvents.col("SentDate"));
        transportEvents.createOrReplaceTempView("transport_events");

        Seq<String> garageNumberColumn =  new Set.Set1<>("GarNr").toSeq();
        validationEvents = validationEvents
                .join(vehiclesOnRoute, garageNumberColumn, "inner");
        validationEvents.createOrReplaceTempView("validation_events");

        StructType validationSchema = validationEvents.schema();
        List<String> transportFields = Arrays.asList(transportEvents.schema().fieldNames());
        for(StructField e : validationSchema.fields()) {
            if (!transportFields.contains(e.name())) {
                transportEvents = transportEvents
                        .withColumn(e.name(),
                                lit(null));
                transportEvents = transportEvents.withColumn(e.name(),
                        transportEvents.col(e.name()).cast(Optional.ofNullable(e.dataType()).orElse(StringType)));
            }
        }
        StructType transportSchema = transportEvents.schema();
        List<String> validationFields = Arrays.asList(validationEvents.schema().fieldNames());
        for(StructField e : transportSchema.fields()) {
            if (!validationFields.contains(e.name())) {
                validationEvents = validationEvents
                        .withColumn(e.name(),
                                lit(null));
                validationEvents = validationEvents.withColumn(e.name(),
                        validationEvents.col(e.name()).cast(Optional.ofNullable(e.dataType()).orElse(StringType)));
            }
        }
        transportEvents = transportEvents
                .withColumn("event_source",
                        lit("vehicle"));
        validationEvents = validationEvents
                .withColumn("event_source",
                        lit("passenger"));
        transportEvents = transportEvents.unionByName(validationEvents);
        transportEvents.createOrReplaceTempView("transport_events");
        transportEvents = transportEvents
                .withColumn("timestamp", coalesce(col("time"), col("SentDate")))
                .drop("time", "SentDate");
        transportEvents = transportEvents
                .sort(transportEvents.col("GarNr"),
                        transportEvents.col("timestamp"));
        transportEvents.createOrReplaceTempView("transport_events");

        Dataset<Row> consolidated = sqlContext
                .sql("SELECT VehicleID, GarNr, TMarsruts, route, Virziens, Code, WGS84Fi, WGS84La, " +
                        "Odometer, Delay, ShiftID, TripID, event_source, ValidTalonaId, timestamp, timestamp_of_transport_event " +
                        "FROM transport_events")
                .withColumn("route", coalesce(col("TMarsruts"), col("route")))
                .drop("TMarsruts");
        consolidated.createOrReplaceTempView("transport_events");

        //fill coordinates with Window function
        WindowSpec ws = Window
                .partitionBy(consolidated.col("GarNr"))
                .orderBy(consolidated.col("timestamp"))
                .rowsBetween(Integer.MIN_VALUE + 1, Window.currentRow());
        Column hypotheticalFi = last(consolidated.col("WGS84Fi"), true).over(ws);
        Column hypotheticalLa = last(consolidated.col("WGS84La"), true).over(ws);
        Column timePassed = last(consolidated.col("timestamp_of_transport_event"), true).over(ws);
        consolidated = consolidated
                .withColumn("hypothetical_fi", hypotheticalFi)
                .withColumn("hypothetical_la", hypotheticalLa)
                .withColumn("time_of_last_transport_event", timePassed);
        consolidated = consolidated.withColumn("time_between_validation_and_transport_event",
                unix_timestamp(consolidated.col("timestamp"))
                        .minus(unix_timestamp(consolidated.col("time_of_last_transport_event"))))
                .drop("timestamp_of_transport_event");
        consolidated = consolidated.filter(col("VehicleID").eqNullSafe(273).and(col("event_source").eqNullSafe("passenger")));
        consolidated.createOrReplaceTempView("transport_events");

        consolidated = sqlContext
                .sql("SELECT VehicleID, GarNr, hypothetical_fi, hypothetical_la, " +
                        "schedule.route, stop_name, stop_lat, stop_lon, " +
                        "" +
                        "  SQRT(" +
                        "    POW((stop_lat - hypothetical_fi), 2) + " +
                        "    POW((stop_lon - hypothetical_la), 2) " +
                        "  )" +
                        " AS distance, " +
                        "Virziens, " +
                        "ValidTalonaId, " +
                        "timestamp, " +
                        "time_between_validation_and_transport_event " +
                        "FROM transport_events " +
                        "INNER JOIN schedule ON schedule.route=transport_events.route");
        Seq<String> minimalDistanceColumn =  new Set.Set1<>("distance").toSeq();
        Dataset<Row> result = consolidated.join(consolidated.as("minimal").groupBy(
                col("route").as("m_route"),
                col("GarNr").as("GN"),
                col("ValidTalonaId").as("VTI")
                //col("timestamp"),
                //col("time_between_validation_and_transport_event"),
                //col("hypothetical_fi"),
                //col("hypothetical_la"),
                //col("stop_name"),
                //col("stop_lat"),
                //col("stop_lon")
                ).agg(min(col("distance")).as("distance")), minimalDistanceColumn, "inner");
                //.orderBy(
                        //col("GarNr"),
                        //col("timestamp"));
        String dir = UUID.randomUUID().toString();
        result.coalesce(1).write()
                .option("header", "true").csv(System.getProperty("user.dir") + "/result/" + dir);
    }

    private static void readEventsDataAndCreateViews(SQLContext sqlContext) {
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
                .option("nullValue","NULL")
                .option("delimiter", ";")
                .option("dateFormat","yyyy-MM-dd HH:mm:ss.SSS")
                .csv(classLoader.getResource("real/VehicleMessages20181123d1.csv").getPath());
        routes = routes.union(sqlContext.read()
                .option("inferSchema", "true")
                .option("header", "true")
                .option("nullValue","NULL")
                .option("delimiter", ";")
                .option("dateFormat","yyyy-MM-dd HH:mm:ss.SSS")
                .csv(classLoader.getResource("real/VehicleMessages20181123d2.csv").getPath()));
        routes.createOrReplaceTempView("routes");
        Dataset<Row> vehicles = sqlContext.read()
                .option("header", "true")
                .option("delimiter", ";")
                .option("inferSchema", "true")
                .csv(classLoader.getResource("Vehicles.csv").getPath());
        vehicles.createOrReplaceTempView("vehicles");
        Dataset<Row> eventType = sqlContext.read()
                .option("header", "true")
                .option("delimiter", ";")
                .option("inferSchema", "true")
                .csv(classLoader.getResource("SendingReasons.csv").getPath());
        eventType.createOrReplaceTempView("event_type");
    }
}
