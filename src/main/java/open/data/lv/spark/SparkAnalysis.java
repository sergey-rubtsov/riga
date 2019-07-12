package open.data.lv.spark;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
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

import static org.apache.spark.sql.functions.abs;
import static org.apache.spark.sql.functions.coalesce;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.count;
import static org.apache.spark.sql.functions.date_format;
import static org.apache.spark.sql.functions.desc;
import static org.apache.spark.sql.functions.first;
import static org.apache.spark.sql.functions.lag;
import static org.apache.spark.sql.functions.last;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.min;
import static org.apache.spark.sql.functions.pow;
import static org.apache.spark.sql.functions.row_number;
import static org.apache.spark.sql.functions.sqrt;
import static org.apache.spark.sql.functions.udf;
import static org.apache.spark.sql.functions.unix_timestamp;
import static org.apache.spark.sql.functions.when;
import static org.apache.spark.sql.types.DataTypes.StringType;

public class SparkAnalysis {

    public static void main(String[] args) {
        System.setProperty("hadoop.home.dir", System.getProperty("user.dir") + "\\hadoop");
        System.setProperty("SPARK_CONF_DIR", System.getProperty("user.dir") + "\\conf");
        PropertyConfigurator.configure(System.getProperty("user.dir") + "\\conf\\log4j.properties");
        //SPARK_CONF_DIR
        //SPARK_HOME
        String masterUrl = "local[1]";
        SparkConf conf = new SparkConf()
                .setMaster(masterUrl)
                .setAppName("Riga public transport")
                .set("SPARK_HOME", System.getProperty("user.dir"))
                .set("SPARK_CONF_DIR", System.getProperty("user.dir") + "\\conf");
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

        readRealEventsDataAndCreateViews(sqlContext);
        prepareTransportEventsForJoin(sqlContext);
        prepareValidationEventsForJoin(sqlContext);

        filterSelected(sqlContext);

        //readGTFSDataAndCreateViews(sqlContext);
        //processGTFSData(sqlContext);

        //readPreprocessedData(sqlContext);
        //processExits(sqlContext);
        //processEventsData(sqlContext);
    }

    private static void filterSelected(SQLContext sqlContext) {
        Dataset<Row> selectedTickets = sqlContext
                .sql("SELECT * FROM validation_events")
                .groupBy(col("ValidTalonaId")).count()
                .select(col("ValidTalonaId"), col("count").as("trips"))
                .groupBy(col("trips"))
                .agg(first(col("ValidTalonaId")).as("ValidTalonaId"), count(col("trips")).as("count"))
                .orderBy(desc("count"));
        Dataset<Row> tickets = sqlContext
                .sql("SELECT * FROM validation_events");
        tickets = tickets.join(selectedTickets, tickets.col("ValidTalonaId").equalTo(selectedTickets.col("ValidTalonaId")), "inner")
                .drop("ValidTalonaId", "count");
        tickets = tickets.withColumn("ValidTalonaId", tickets.col("trips")).drop("trips");
/*
        tickets.coalesce(1).write()
                .option("header", "true").csv(System.getProperty("user.dir") + "\\result\\validations\\" + UUID.randomUUID().toString());*/
        //VehicleID;VehicleCompanyCode
        UserDefinedFunction garageNumber = udf(
                (Integer i) -> {
                    while (i > 9999) {
                        i = i / 10;
                    }
                    return i;
                }, DataTypes.IntegerType
        );
        tickets = tickets.withColumn("GarNr",
                garageNumber.apply(tickets.col("GarNr")));
        Dataset<Row> vehicles = sqlContext
                .sql("SELECT * FROM vehicles");
        Dataset<Row> selectedVehiclesNumbers = tickets.select(col("GarNr"))
                .groupBy(col("GarNr")).count().join(vehicles, tickets.col("GarNr").equalTo(vehicles.col("VehicleCompanyCode")));
        Dataset<Row> selectedVehicles = sqlContext
                .sql("SELECT * FROM routes");
        selectedVehicles = selectedVehicles.join(selectedVehiclesNumbers, new Set.Set1<>("VehicleID").toSeq(), "inner").drop("count", "VehicleCompanyCode", "GarNr");
        selectedVehicles.filter(col("SendingReason").equalTo(6));
        selectedVehicles.coalesce(1).write()
                .option("header", "true").csv(System.getProperty("user.dir") + "\\result\\transport\\" + UUID.randomUUID().toString());
    }

    private static void readPreprocessedData(SQLContext sqlContext) {
        ClassLoader classLoader = SparkAnalysis.class.getClassLoader();
        Dataset<Row> data = sqlContext.read()
                .option("inferSchema", "true")
                .option("header", "true")
                .option("delimiter", ",")
                .csv(classLoader.getResource("real/preprocessed_data.csv").getPath());
        data.createOrReplaceTempView("preprocessed_data");
    }

    private static void readGTFSDataAndCreateViews(SQLContext sqlContext) {
        ClassLoader classLoader = SparkAnalysis.class.getClassLoader();
        Dataset<Row> routes = sqlContext.read()
                .option("inferSchema", "true")
                .option("header", "true")
                .option("delimiter", ",")
                .option("dateFormat", "HH:mm:ss")
                .csv(classLoader.getResource("real/GTFS/routes.txt").getPath());
        routes.createOrReplaceTempView("gtfs_routes");
        Dataset<Row> stop_times = sqlContext.read()
                .option("inferSchema", "true")
                .option("header", "true")
                .option("delimiter", ",")
                .option("dateFormat", "HH:mm:ss")
                .csv(classLoader.getResource("real/GTFS/stop_times.txt").getPath());
        stop_times.createOrReplaceTempView("stop_times");
        Dataset<Row> stops = sqlContext.read()
                .option("inferSchema", "true")
                .option("header", "true")
                .option("delimiter", ",")
                .option("dateFormat", "HH:mm:ss")
                .csv(classLoader.getResource("real/GTFS/stops.txt").getPath());
        stops.createOrReplaceTempView("stops");
        Dataset<Row> trips = sqlContext.read()
                .option("inferSchema", "true")
                .option("header", "true")
                .option("delimiter", ",")
                .option("dateFormat", "HH:mm:ss")
                .csv(classLoader.getResource("real/GTFS/trips.txt").getPath());
        trips.createOrReplaceTempView("trips");
        Dataset<Row> type = sqlContext.read()
                .option("inferSchema", "true")
                .option("header", "true")
                .option("delimiter", ",")
                .csv(classLoader.getResource("real/GTFS/route_types.txt").getPath());
        type.createOrReplaceTempView("route_types");
    }

    //if we process different dates, we need to group by days too
    private static void processExits(SQLContext sqlContext) {
        Dataset<Row> transactions = sqlContext.sql("SELECT * FROM preprocessed_data")
                .withColumn("transactions",
                        count("*")
                                .over(Window.partitionBy(col("ValidTalonaId"))));
        transactions.createOrReplaceTempView("transactions");
        transactions = transactions.filter(col("transactions").gt(1));
        WindowSpec ws = Window
                .partitionBy(transactions.col("ValidTalonaId"))
                .orderBy(transactions.col("timestamp"));

        Dataset<Row> enters = transactions
                .withColumn("number_of_transaction", row_number().over(ws))
                .withColumn("first_stop_lat", first(col("stop_lat"), true).over(ws))
                .withColumn("first_stop_lon", first(col("stop_lon"), true).over(ws))
                .withColumn("first_stop_name", first(col("stop_name"), true).over(ws))
                .withColumn("first_route", first(col("route"), true).over(ws))
                .withColumn("first_direction", first(col("direction"), true).over(ws))
                .withColumn("next_stop_timestamp", lag(col("timestamp"), -1, null).over(ws))
                .withColumn("next_stop_lat", lag(col("stop_lat"), -1, null).over(ws))
                .withColumn("next_stop_lon", lag(col("stop_lon"), -1, null).over(ws))
                .withColumn("next_stop_name", lag(col("stop_name"), -1, null).over(ws))
                .withColumn("next_route", lag(col("route"), -1, null).over(ws))
                .withColumn("next_direction", lag(col("direction"), -1, null).over(ws))
                .orderBy(col("ValidTalonaId"), col("timestamp"));
        enters.createOrReplaceTempView("enters");

        Dataset<Row> breadCrumbs = sqlContext
                .sql("SELECT route as schedule_route, direction as schedule_direction, stop_sequence, " +
                        "stop_id as exit_stop_id, stop_name as exit_stop_name, stop_lat as exit_stop_lat, stop_lon as exit_stop_lon FROM bread_crumbs");
        Dataset<Row> intermediate = enters.join(breadCrumbs,
                enters.col("route").equalTo(breadCrumbs.col("schedule_route"))
                        .and(enters.col("direction").equalTo(breadCrumbs.col("schedule_direction")))
                        .and(enters.col("stop_sequence").lt(breadCrumbs.col("stop_sequence"))),
                "left");

        Dataset<Row> result = intermediate.orderBy(col("ValidTalonaId"), col("timestamp"));

        //lat degree (56.9) = 111.3 km, lon degree (24) = 60.8 km, coefficient = 60.8 / 111.3
        double coefficient = 0.5462713387241689;
        result = result.withColumn("diff_enter",
                abs(result.col("exit_stop_lat")
                        .$minus(result.col("next_stop_lat"))).$times(coefficient).
                        plus(abs(result.col("exit_stop_lon")
                                .$minus(result.col("next_stop_lon")))));
        result = result.withColumn("diff_last_enter",
                abs(result.col("exit_stop_lat")
                        .$minus(result.col("first_stop_lat"))).$times(coefficient).
                        plus(abs(result.col("exit_stop_lon")
                                .$minus(result.col("first_stop_lon")))));

        result = result.withColumn("diff", coalesce(col("diff_enter"), col("diff_last_enter")))
                .drop("diff_enter", "diff_last_enter");

        Dataset<Row> minimals = result.groupBy(
                col("ValidTalonaId"), col("timestamp"))
                .agg(min(col("diff")).as("min"));
        result = minimals.join(result,
                new Set.Set2<>("ValidTalonaId", "timestamp").toSeq())
                .where(minimals.col("min")
                        .equalTo(result.col("diff")))
                .drop("min", "diff");
        //recalculate distance again in kilometers for minimal distances
        result = result.withColumn("diff_enter",
                sqrt(pow((result.col("exit_stop_lat")
                        .minus(result.col("next_stop_lat")).multiply(111.3)), 2).
                        plus(pow((result.col("exit_stop_lon")
                                .minus(result.col("next_stop_lon")).multiply(60.8)), 2))));
        result = result.withColumn("diff_last_enter",
                sqrt(pow((result.col("exit_stop_lat")
                        .minus(result.col("first_stop_lat")).multiply(111.3)), 2).
                        plus(pow((result.col("exit_stop_lon")
                                .minus(result.col("first_stop_lon")).multiply(60.8)), 2))));
        result = result.withColumn("distance_between_exit_and_enter", coalesce(col("diff_enter"), col("diff_last_enter")))
                .drop("diff_enter", "diff_last_enter");
        result = result.select(
                col("route"),
                col("count").as("passengers_count"),
                col("transactions").as("transactions_count"),
                col("number_of_transaction"),
                col("direction"),
                col("VehicleID"),
                col("GarNr"),
                col("ValidTalonaId"),
                col("timestamp"),
                col("stop_id").as("enter_stop_id"),
                col("stop_name").as("enter_stop_name"),
                col("stop_lat").as("enter_stop_lat"),
                col("stop_lon").as("enter_stop_lon"),
                col("exit_stop_id"),
                col("exit_stop_name"),
                col("exit_stop_lat"),
                col("exit_stop_lon"),
                col("distance_between_exit_and_enter")
        ).orderBy(col("ValidTalonaId"), col("timestamp"));
        String dir = UUID.randomUUID().toString();
        result.coalesce(1).write()
                .option("header", "true").csv(System.getProperty("user.dir") + "/result/" + dir);
    }

    private static void processGTFSData(SQLContext sqlContext) {
        Dataset<Row> schedule = sqlContext
                .sql("SELECT CONCAT(route_types.short_name, ' ', route_short_name) AS route, direction_id, " +
                        "trips.route_id, stop_name, stop_lat, stop_lon, stops.stop_id as stop_id, arrival_time, departure_time, stop_sequence, stop_times.trip_id as trip FROM stop_times " +
                        "INNER JOIN stops ON stop_times.stop_id=stops.stop_id " +
                        "INNER JOIN trips on stop_times.trip_id=trips.trip_id " +
                        "INNER JOIN gtfs_routes on gtfs_routes.route_id=trips.route_id " +
                        "INNER JOIN route_types ON route_types.route_type=gtfs_routes.route_type");
        //direction 0 is Forth, 1 is Back
        schedule = schedule.withColumn("direction",
                when(col("direction_id").equalTo(0), "Forth")
                        .when(col("direction_id").equalTo(1), "Back"));
        schedule = schedule.orderBy(
                col("route"),
                col("direction"),
                col("arrival_time"));
        schedule.createOrReplaceTempView("schedule");
        Dataset<Row> points = schedule.groupBy(
                col("route"),
                col("direction"),
                col("stop_name"),
                col("stop_id"),
                col("stop_lat"),
                col("stop_lon"))
                .agg(first(col("stop_sequence")).as("stop_sequence"), first(col("trip")).as("trip"))
                .orderBy(
                        col("route"),
                        col("direction"),
                        col("stop_sequence"));
        points.createOrReplaceTempView("bread_crumbs");
    }

    private static void processEventsData(SQLContext sqlContext) {
        Dataset<Row> transportEvents = sqlContext
                .sql("SELECT * FROM transport_events");
        Dataset<Row> validationEvents = sqlContext
                        .sql("SELECT * FROM validation_events");
        Dataset<Row> vehiclesOnRoute = sqlContext
                .sql("SELECT first(TMarsruts) as route, GarNr, vehicles.VehicleID, min(time) "
                        + "as first_time, max(time) as last_time, count(time) as events FROM validation_events "
                        + "INNER JOIN vehicles ON validation_events.GarNr=vehicles.VehicleCompanyCode");
        //In Validations table for 'Tr 101' we have 119 records garage numbers of vehicles,
        //in Vehicles table we have only 93 records with connected VehicleID
        vehiclesOnRoute.createOrReplaceTempView("vehicles_on_route");
        Seq<String> vehicleIDColumn = new Set.Set1<>("VehicleID").toSeq();
        transportEvents = transportEvents
                .join(vehiclesOnRoute, vehicleIDColumn, "inner");
        transportEvents = transportEvents
                .withColumn("timestamp_of_transport_event", transportEvents.col("SentDate"));
        transportEvents.createOrReplaceTempView("transport_events");
        Seq<String> garageNumberColumn = new Set.Set1<>("GarNr").toSeq();
        validationEvents = validationEvents
                .join(vehiclesOnRoute, garageNumberColumn, "inner");
        validationEvents.createOrReplaceTempView("validation_events");

        StructType validationSchema = validationEvents.schema();
        List<String> transportFields = Arrays.asList(transportEvents.schema().fieldNames());
        transportEvents = balanceDataset(transportEvents, validationSchema, transportFields);
        StructType transportSchema = transportEvents.schema();
        List<String> validationFields = Arrays.asList(validationEvents.schema().fieldNames());
        validationEvents = balanceDataset(validationEvents, transportSchema, validationFields);
        transportEvents = transportEvents
                .withColumn("event_source",
                        lit("vehicle"));
        validationEvents = validationEvents
                .withColumn("event_source",
                        lit("passenger"));
        transportEvents = transportEvents.unionByName(validationEvents);
        transportEvents = transportEvents
                .withColumn("timestamp", coalesce(col("time_stamp"), col("SentDate")))
                .drop("time_stamp", "SentDate");
        transportEvents = transportEvents
                .sort(transportEvents.col("GarNr"),
                        transportEvents.col("timestamp"));
        transportEvents.createOrReplaceTempView("transport_events");

        Dataset<Row> consolidated = sqlContext
                .sql("SELECT VehicleID, GarNr, TMarsruts, route, Virziens, Code, WGS84Fi, WGS84La, " +
                        "Odometer, VehicleMessageID, event_source, ValidTalonaId, timestamp, timestamp_of_transport_event " +
                        "FROM transport_events")
                .withColumn("route", coalesce(col("TMarsruts"), col("route")))
                .drop("TMarsruts");
        consolidated.createOrReplaceTempView("transport_events");

        //Fill missed values with Window function after union two different sets
        WindowSpec ws = Window
                .partitionBy(consolidated.col("GarNr"))
                .orderBy(consolidated.col("timestamp"))
                .rowsBetween(Integer.MIN_VALUE + 1, Window.currentRow());
        Column hypotheticalFi = last(consolidated.col("WGS84Fi"), true).over(ws);
        Column hypotheticalLa = last(consolidated.col("WGS84La"), true).over(ws);
        Column timePassed = last(consolidated.col("timestamp_of_transport_event"), true).over(ws);
        Column vehicleMessageId = last(consolidated.col("VehicleMessageID"), true).over(ws);
        consolidated = consolidated
                .withColumn("hypothetical_fi", hypotheticalFi)
                .withColumn("hypothetical_la", hypotheticalLa)
                .withColumn("time_of_last_transport_event", timePassed)
                .withColumn("vehicle_message_id", vehicleMessageId);
        consolidated = consolidated.withColumn("time_between_validation_and_transport_event",
                unix_timestamp(consolidated.col("timestamp"))
                        .minus(unix_timestamp(consolidated.col("time_of_last_transport_event"))))
                .drop("timestamp_of_transport_event");
        Dataset<Row> breadCrumbs = sqlContext.sql("SELECT route, direction, stop_name, stop_id, trip, stop_sequence, stop_lat, stop_lon FROM bread_crumbs");
        String dir = UUID.randomUUID().toString();

        Dataset<Row> vehicles = consolidated
                .filter(col("event_source").eqNullSafe("vehicle"))
                .select(col("VehicleID"),
                        col("vehicle_message_id"),
                        col("route").as("vehicle_route"),
                        col("WGS84Fi").as("transport_gps_fi"),
                        col("WGS84La").as("transport_gps_la"),
                        col("timestamp"));
        //Here we need to calculate minimal distance for both directions, because we don't know it for a vehicle
        Dataset<Row> doorsOpenedPositionsAndScheduledStops = vehicles
                .join(breadCrumbs,
                        vehicles.col("vehicle_route").equalTo(breadCrumbs.col("route")),
                        "inner");
        //lat degree (56.9) = 111.3 km, lon degree (24) = 60.8 km, coefficient = 60.8 / 111.3
        double coefficient = 0.5462713387241689;
        doorsOpenedPositionsAndScheduledStops = doorsOpenedPositionsAndScheduledStops.withColumn("diff",
                abs(doorsOpenedPositionsAndScheduledStops.col("stop_lat")
                        .minus(doorsOpenedPositionsAndScheduledStops.col("transport_gps_fi"))).multiply(coefficient).
                        plus(abs(doorsOpenedPositionsAndScheduledStops.col("stop_lon")
                                .minus(doorsOpenedPositionsAndScheduledStops.col("transport_gps_la")))));

        Dataset<Row> closest = doorsOpenedPositionsAndScheduledStops.groupBy(
                col("direction"),
                col("vehicle_message_id"),
                col("transport_gps_fi"),
                col("transport_gps_la"))
                .agg(min(col("diff")).as("min"));
        doorsOpenedPositionsAndScheduledStops = doorsOpenedPositionsAndScheduledStops.drop(col("direction"));
        doorsOpenedPositionsAndScheduledStops = closest.join(doorsOpenedPositionsAndScheduledStops,
                new Set.Set3<>("vehicle_message_id", "transport_gps_fi", "transport_gps_la").toSeq())
                .where(closest.col("min")
                        .equalTo(doorsOpenedPositionsAndScheduledStops.col("diff")))
                .drop("min")
                .orderBy(col("trip"), col("direction"), col("stop_sequence"), col("timestamp"));
        doorsOpenedPositionsAndScheduledStops.coalesce(1).write()
                .option("header", "true").csv(System.getProperty("user.dir") + "/result/vehicles/" + dir);

        Dataset<Row> passengers = consolidated.filter(col("event_source").eqNullSafe("passenger"));
        passengers.createOrReplaceTempView("passenger_events");
        passengers = sqlContext
                .sql("SELECT route, " +
                        "event_source, " +
                        "vehicle_message_id, " +
                        "VehicleID, " +
                        "GarNr, " +
                        "hypothetical_fi, " +
                        "hypothetical_la, " +
                        "Virziens, " +
                        "ValidTalonaId, " +
                        "timestamp, " +
                        "time_between_validation_and_transport_event " +
                        "FROM passenger_events");
        Dataset<Row> actualStopsAndEvents = passengers.groupBy(
                col("vehicle_message_id"),
                col("route"),
                col("Virziens").as("direction"),
                col("hypothetical_fi"),
                col("hypothetical_la")
        ).count();
        Dataset<Row> hypotheticalStopsAndScheduledStops = actualStopsAndEvents
                .join(breadCrumbs, new Set.Set2<>("route", "direction").toSeq(), "inner");

        hypotheticalStopsAndScheduledStops = hypotheticalStopsAndScheduledStops.withColumn("diff",
                abs(hypotheticalStopsAndScheduledStops.col("stop_lat")
                        .minus(hypotheticalStopsAndScheduledStops.col("hypothetical_fi"))).multiply(coefficient).
                        plus(abs(hypotheticalStopsAndScheduledStops.col("stop_lon")
                                .minus(hypotheticalStopsAndScheduledStops.col("hypothetical_la")))));

        Dataset<Row> minimals = hypotheticalStopsAndScheduledStops.groupBy(
                col("vehicle_message_id"),
                col("direction"),
                col("hypothetical_fi"),
                col("hypothetical_la"))
                .agg(min(col("diff")).as("min"));
        hypotheticalStopsAndScheduledStops = minimals.join(hypotheticalStopsAndScheduledStops,
                new Set.Set4<>("vehicle_message_id", "direction", "hypothetical_fi", "hypothetical_la").toSeq())
                .where(minimals.col("min")
                        .equalTo(hypotheticalStopsAndScheduledStops.col("diff")))
                .drop("min");
        //Since the distance is relatively small, we can use the rectangular distance approximation using formula
        //SQRT(POW((stop_lat - hypothetical_fi), 2) + POW((stop_lon - hypothetical_la), 2))
        //but we need to translate grades into km.
        //This approximation is faster than using the Haversine formula.
        //But for comparing distances we can compare squares of coordinate differences without square root calculation.
        hypotheticalStopsAndScheduledStops = hypotheticalStopsAndScheduledStops.withColumn("distance_kilometers",
                sqrt(pow((hypotheticalStopsAndScheduledStops.col("stop_lat")
                        .minus(hypotheticalStopsAndScheduledStops.col("hypothetical_fi")).multiply(111.3)), 2).
                        plus(pow((hypotheticalStopsAndScheduledStops.col("stop_lon")
                                .minus(hypotheticalStopsAndScheduledStops.col("hypothetical_la")).multiply(60.8)), 2))));
        hypotheticalStopsAndScheduledStops = hypotheticalStopsAndScheduledStops.drop("hypothetical_fi", "hypothetical_la", "route");
        Seq<String> minimalDistanceColumn = new Set.Set1<>("vehicle_message_id").toSeq();
        passengers = passengers.join(hypotheticalStopsAndScheduledStops.as("s"), minimalDistanceColumn, "inner")
                .drop("vehicle_message_id", "diff");

        passengers.coalesce(1).write()
                .option("header", "true").csv(System.getProperty("user.dir") + "/result/passengers/" + dir);
    }

    /**
     * creates view transport_events:
     +----------------+-----+-------------------+---------+--------+--------+--------+----------+----------+
     |VehicleMessageID|GarNr|           SentDate|     Code| WGS84Fi| WGS84La|Odometer|      date|      time|
     +----------------+-----+-------------------+---------+--------+--------+--------+----------+----------+
     |       868718306| 6422|2018-11-23 04:48:14|DoorsOpen| 56.9707| 24.0316|     367|2018-11-23|4:48:14 AM|
     */
    private static void prepareTransportEventsForJoin(SQLContext sqlContext) {
        Dataset<Row> transportEvents = sqlContext
                .sql("SELECT VehicleMessageID, VehicleCompanyCode AS GarNr, SentDate, event_type.Code, " +
                        "WGS84Fi, WGS84La, Odometer FROM routes " +
                        "INNER JOIN event_type ON routes.SendingReason=event_type.SendingReason " +
                        "LEFT JOIN vehicles ON routes.VehicleID=vehicles.VehicleID");
        //filter open doors only and broken data
        transportEvents = transportEvents
                .filter(transportEvents.col("Code").equalTo("DoorsOpen")
                        .and(transportEvents.col("WGS84La").notEqual(0.0))
                        .and(transportEvents.col("WGS84Fi").notEqual(0.0))
                );
        transportEvents = transportEvents.withColumn("date",
                transportEvents.col("SentDate").cast(DataTypes.DateType));
        transportEvents = transportEvents.withColumn("time",
                date_format(transportEvents.col("SentDate"), "h:m:s a"));
        transportEvents.createOrReplaceTempView("transport_events");
    }

    /**
     * creates view validation_events:
     * +---------+--------+-------------+-----+-------------------+----------+-----------+
     * |TMarsruts|Virziens|ValidTalonaId|GarNr|         time_stamp|      date|       time|
     * +---------+--------+-------------+-----+-------------------+----------+-----------+
     * |     A 16|    Back|            3| 7688|2018-11-23 09:53:37|2018-11-23| 9:53:37 AM|
     */
    private static void prepareValidationEventsForJoin(SQLContext sqlContext) {
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
        UserDefinedFunction timeStamp = udf(
                (String s) -> new Timestamp(dateFormat.parse(s).getTime()), DataTypes.TimestampType
        );
        validationEvents = validationEvents.withColumn("time_stamp",
                timeStamp.apply(validationEvents.col("Laiks"))).drop(validationEvents.col("Laiks"));
        validationEvents = validationEvents.withColumn("date",
                validationEvents.col("time_stamp").cast(DataTypes.DateType));
        validationEvents = validationEvents.withColumn("time",
                date_format(validationEvents.col("time_stamp"), "h:m:s a"));
        validationEvents.createOrReplaceTempView("validation_events");
    }

    private static Dataset<Row> balanceDataset(Dataset<Row> events, StructType schema, List<String> fields) {
        for (StructField e : schema.fields()) {
            if (!fields.contains(e.name())) {
                events = events
                        .withColumn(e.name(),
                                lit(null));
                events = events.withColumn(e.name(),
                        events.col(e.name()).cast(Optional.ofNullable(e.dataType()).orElse(StringType)));
            }
        }
        return events;
    }

    private static void readSelectedEventsDataAndCreateViews(SQLContext sqlContext) {
        //transport_events
        //validation_events
    }

    private static void readRealEventsDataAndCreateViews(SQLContext sqlContext) {
        ClassLoader classLoader = SparkAnalysis.class.getClassLoader();
        //ValidDati23_11_18.txt is real, selected.csv is test
        String valFile = "real/selected.csv";
        Dataset<Row> tickets = sqlContext.read()
                .option("inferSchema", "true")
                .option("header", "true")
                .option("dateFormat", "dd.MM.yyyy HH:mm:ss")
                .csv(classLoader.getResource(valFile).getPath());
        tickets.createOrReplaceTempView("tickets");
        //VehicleMessages20181123d1.csv and VehicleMessages20181123d2.csv are real,
        //VehicleMessagesSelected.csv is test
        Dataset<Row> routes = sqlContext.read()
                .option("inferSchema", "true")
                .option("header", "true")
                .option("nullValue", "NULL")
                .option("delimiter", ";")
                .option("dateFormat", "yyyy-MM-dd HH:mm:ss.SSS")
                .csv(classLoader.getResource("real/VehicleMessages20181123d1.csv").getPath());
/*        routes = routes.union(sqlContext.read()
                .option("inferSchema", "true")
                .option("header", "true")
                .option("nullValue", "NULL")
                .option("delimiter", ";")
                .option("dateFormat", "yyyy-MM-dd HH:mm:ss.SSS")
                .csv(classLoader.getResource("real/VehicleMessages20181123d2.csv").getPath()));*/
        routes.createOrReplaceTempView("routes");
        Dataset<Row> vehicles = sqlContext.read()
                .option("header", "true")
                .option("delimiter", ";")
                .option("inferSchema", "true")
                .csv(classLoader.getResource("real/Vehicles.csv").getPath());
        vehicles.createOrReplaceTempView("vehicles");
        Dataset<Row> eventType = sqlContext.read()
                .option("header", "true")
                .option("delimiter", ";")
                .option("inferSchema", "true")
                .csv(classLoader.getResource("real/SendingReasons.csv").getPath());
        eventType.createOrReplaceTempView("event_type");
    }
}
