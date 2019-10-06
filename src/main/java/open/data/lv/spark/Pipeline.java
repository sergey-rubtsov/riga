package open.data.lv.spark;

import org.apache.log4j.PropertyConfigurator;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.*;
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

import static org.apache.spark.sql.functions.abs;
import static org.apache.spark.sql.functions.coalesce;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.concat;
import static org.apache.spark.sql.functions.count;
import static org.apache.spark.sql.functions.date_format;
import static org.apache.spark.sql.functions.first;
import static org.apache.spark.sql.functions.lag;
import static org.apache.spark.sql.functions.last;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.max;
import static org.apache.spark.sql.functions.min;
import static org.apache.spark.sql.functions.pow;
import static org.apache.spark.sql.functions.sqrt;
import static org.apache.spark.sql.functions.udf;
import static org.apache.spark.sql.functions.unix_timestamp;
import static org.apache.spark.sql.functions.when;
import static org.apache.spark.sql.types.DataTypes.StringType;

public class Pipeline {

    private static String MASTER_URL = "local[1]";

    private static List<String> TICKET_VALIDATIONS_FILES = new ArrayList<>();

    private static List<String> VEHICLE_MESSAGES_FILES = new ArrayList<>();

    private static String VEHICLE_MAPPING_FILE;

    private static String MESSAGE_TYPE_FILE;

    private static String ROUTES;

    private static String STOP_TIMES;

    private static String STOPS;

    private static String TRIPS;

    private static String ROUTE_TYPES;

    private static void initPipelineParameters() {
        VEHICLE_MAPPING_FILE = "real/Vehicles.csv";
        MESSAGE_TYPE_FILE ="real/SendingReasons.csv";
        TICKET_VALIDATIONS_FILES.add("real/TestValidations.csv");
        VEHICLE_MESSAGES_FILES.add("real/TestVehicleMessages.csv");

        ROUTES = "real/GTFS/routes.txt";
        STOP_TIMES = "real/GTFS/stop_times.txt";
        STOPS = "real/GTFS/stops.txt";
        TRIPS = "real/GTFS/trips.txt";
        ROUTE_TYPES = "real/GTFS/route_types.txt";
    }

    private static Dataset<Row> readFiles(SQLContext sqlContext,
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

    private static Dataset<Row> readFiles(SQLContext sqlContext,
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
/*        Dataset<Row> routes = readFiles(sqlContext, ROUTES, "HH:mm:ss", null, ",");
        Dataset<Row> stopTimes = readFiles(sqlContext, STOP_TIMES, "HH:mm:ss", null, ",");
        Dataset<Row> stops = readFiles(sqlContext, STOPS, "HH:mm:ss", null, ",");
        Dataset<Row> trips = readFiles(sqlContext, TRIPS, "HH:mm:ss", null, ",");
        Dataset<Row> routeTypes = readFiles(sqlContext, ROUTE_TYPES, "HH:mm:ss", null, ",");
        Dataset<Row> buildGTFSSchedule = buildGTFSSchedule(routes, stopTimes, stops, trips, routeTypes);*/

        Dataset<Row> tickets = readFiles(sqlContext, TICKET_VALIDATIONS_FILES, "dd.MM.yyyy HH:mm:ss", null, null);
        Dataset<Row> vehicleMessages = readFiles(sqlContext, VEHICLE_MESSAGES_FILES, "yyyy-MM-dd HH:mm:ss.SSS", "NULL", null); //real delimeter is ;
        Dataset<Row> vehicleAndCompanyMapping = readFiles(sqlContext, VEHICLE_MAPPING_FILE, null, null, ";");
        Dataset<Row> eventTypes = readFiles(sqlContext, MESSAGE_TYPE_FILE, null, null, ";");

        Dataset<Row> validationEvents = prepareValidationEventsForJoin(tickets);
        vehicleMessages.show();
        eventTypes.show();
        Dataset<Row> transportEvents = prepareTransportEventsForJoin(vehicleMessages, eventTypes, vehicleAndCompanyMapping);

        Dataset<Row> events = unionDataSets(validationEvents, vehicleAndCompanyMapping, transportEvents);

        events = proposeCoordinateOfValidations(events);
        events.orderBy("VehicleID", "timestamp");
        events.show(300);
        events.show();
    }

    private static Dataset<Row> buildGTFSSchedule(Dataset<Row> routes,
                                                  Dataset<Row> stopTimes,
                                                  Dataset<Row> stops,
                                                  Dataset<Row> trips,
                                                  Dataset<Row> routeTypes) {
        Dataset<Row> schedule = stopTimes
                .join(stops, new Set.Set1<>("stop_id").toSeq(), "inner")
                .join(trips, new Set.Set1<>("trip_id").toSeq(), "inner")
                .join(routes, new Set.Set1<>("route_id").toSeq(),"inner")
                .join(routeTypes, new Set.Set1<>("route_type").toSeq(), "inner")
                .select(
                        concat(col("short_name"), lit(" "), col("route_short_name")).as("route"),
                        col("direction_id"),
                        col("route_id"),
                        col("stop_name"),
                        col("stop_lat"),
                        col("stop_lon"),
                        col("stop_id"),
                        col("arrival_time"),
                        col("departure_time"),
                        col("stop_sequence"),
                        col("trip_id"));
        //direction 0 is Forth, 1 is Back
        schedule = schedule.withColumn("direction",
                when(col("direction_id").equalTo(0), "Forth")
                        .when(col("direction_id").equalTo(1), "Back")).drop("direction_id");
        return schedule.orderBy(
                col("route"),
                col("direction"),
                col("arrival_time"));
    }

    /**
     * creates dataset transport_events:
     +-----+-------------------+---------+--------+--------+----------+----------+
     |GarNr|           SentDate|     Code| WGS84Fi| WGS84La|      date|      time|
     +-----+-------------------+---------+--------+--------+----------+----------+
     | 6422|2018-11-23 04:48:14|DoorsOpen| 56.9707| 24.0316|2018-11-23|4:48:14 AM|
     */
    private static Dataset<Row> prepareTransportEventsForJoin(Dataset<Row> vehicleMessages,
                                                      Dataset<Row> eventTypes,
                                                      Dataset<Row> vehicleAndCompanyMapping) {
        Dataset<Row> transportEvents = vehicleMessages
                .join(eventTypes, new Set.Set1<>("SendingReason").toSeq(), "inner")
                .join(vehicleAndCompanyMapping, new Set.Set1<>("VehicleID").toSeq(), "left")
                .select(col("VehicleCompanyCode").as("GarNr"),
                        col("VehicleMessageID"),
                        col("TripID"),
                        col("VehicleID"),
                        col("SentDate"),
                        col("Code"),
                        col("WGS84Fi"),
                        col("WGS84La"));
        //filter open doors only and broken data
        transportEvents = transportEvents
                .filter(transportEvents.col("Code").equalTo("DoorsOpen")
                        .and(transportEvents.col("WGS84La").notEqual(0.0))
                        .and(transportEvents.col("WGS84Fi").notEqual(0.0))
                );
        transportEvents = transportEvents.withColumn("date",
                transportEvents.col("SentDate").cast(DataTypes.DateType));
        return transportEvents.withColumn("time",
                date_format(transportEvents.col("SentDate"), "h:m:s a"));
    }

    /**
     * creates dataset validation_events:
     * +---------+--------+-------------+-----+-------------------+----------+-----------+
     * |TMarsruts|Virziens|ValidTalonaId|GarNr|         time_stamp|      date|       time|
     * +---------+--------+-------------+-----+-------------------+----------+-----------+
     * |     A 16|    Back|            3| 7688|2018-11-23 09:53:37|2018-11-23| 9:53:37 AM|
     */
    private static Dataset<Row> prepareValidationEventsForJoin(Dataset<Row> tickets) {
        Dataset<Row> validationEvents = tickets.select(
                col("GarNr").as("GN"),
                col("TMarsruts"),
                col("Virziens"),
                col("ValidTalonaId"),
                col("Laiks"));
        validationEvents = validationEvents.withColumn("GarNr",
                mapGarageNumberFunction().apply(validationEvents.col("GN"))).drop("GN");
        validationEvents = validationEvents.withColumn("time_stamp",
                parseTimeStampFunction().apply(validationEvents.col("Laiks"))).drop(validationEvents.col("Laiks"));
        validationEvents = validationEvents.withColumn("date",
                validationEvents.col("time_stamp").cast(DataTypes.DateType));
        return validationEvents.withColumn("time",
                date_format(validationEvents.col("time_stamp"), "h:m:s a"));
    }

    private static UserDefinedFunction mapGarageNumberFunction() {
        return udf(
                (Integer i) -> {
                    while (i > 9999) {
                        i = i / 10;
                    }
                    return i;
                }, DataTypes.IntegerType
        );
    }

    private static UserDefinedFunction parseTimeStampFunction() {
        SimpleDateFormat dateFormat = new SimpleDateFormat("dd.MM.yyyy HH:mm:ss");
        return udf(
                (String s) -> new Timestamp(dateFormat.parse(s).getTime()), DataTypes.TimestampType
        );
    }

    private static Dataset<Row> unionDataSets(Dataset<Row> validations,
                                              Dataset<Row> companyMapping,
                                              Dataset<Row> vehicle) {
        Dataset<Row> vehiclesOnRoute = validations.join(companyMapping,
                validations.col("GarNr").equalTo(companyMapping.col("VehicleCompanyCode")),
                "inner")
                .groupBy(col("GarNr"), col("VehicleID")).agg(
                        first(col("TMarsruts")).as("route"),
                        min(col("time")).as("first_time"),
                        max(col("time")).as("last_time"),
                        count(col("time")).as("events")
                );
        Seq<String> vehicleIDColumn = new Set.Set1<>("VehicleID").toSeq();
        vehicle = vehicle
                .drop(col("Delay"))
                .drop(col("Gyro"))
                .drop(col("Speed"))
                .drop(col("ShiftID"))
                .drop(col("Odometer"));
        vehicle = vehicle
                .join(vehiclesOnRoute
                        .select(col("VehicleID"), col("route")), vehicleIDColumn, "inner");
        vehicle = vehicle
                .withColumn("timestamp_of_transport_event", vehicle.col("SentDate"));
        Seq<String> garageNumberColumn = new Set.Set1<>("GarNr").toSeq();
        validations = validations
                .join(vehiclesOnRoute
                        .select(col("VehicleID"), col("GarNr")), garageNumberColumn, "inner");
        StructType validationSchema = validations.schema();
        List<String> transportFields = Arrays.asList(vehicle.schema().fieldNames());
        vehicle = balanceDataset(vehicle, validationSchema, transportFields);
        StructType transportSchema = vehicle.schema();
        List<String> validationFields = Arrays.asList(validations.schema().fieldNames());
        validations = balanceDataset(validations, transportSchema, validationFields);
        vehicle = vehicle
                .withColumn("event_source",
                        lit("vehicle"));
        validations = validations
                .withColumn("event_source",
                        lit("passenger"));
        Dataset<Row> consolidated = vehicle.unionByName(validations);
        consolidated = consolidated
                .withColumn("timestamp", coalesce(col("time_stamp"), col("SentDate")))
                .drop("time_stamp", "SentDate");
        consolidated = consolidated
                .sort(consolidated.col("GarNr"),
                        consolidated.col("timestamp"));
        return consolidated.select(
                col("VehicleID"),
                col("VehicleMessageID"),
                col("GarNr"),
                col("TMarsruts"),
                col("route"),
                col("Virziens").as("direction"),
                col("TripID"),
                col("Code"),
                col("WGS84Fi"),
                col("WGS84La"),
                col("event_source"),
                col("ValidTalonaId"),
                col("time"),
                col("date"),
                col("timestamp"),
                col("timestamp_of_transport_event"))
                .withColumn("route", coalesce(col("TMarsruts"), col("route")))
                .drop("TMarsruts");
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

    private static Dataset<Row> proposeCoordinateOfValidations(Dataset<Row> events) {
        //Fill missed values with Window function after union two different sets
        WindowSpec ws = Window
                .partitionBy(events.col("GarNr"))
                .orderBy(events.col("timestamp"))
                .rowsBetween(Integer.MIN_VALUE + 1, Window.currentRow());
        Column hypotheticalFi = last(events.col("WGS84Fi"), true).over(ws);
        Column hypotheticalLa = last(events.col("WGS84La"), true).over(ws);
        Column timePassed = last(events.col("timestamp_of_transport_event"), true).over(ws);
        Column vehicleMessageId = last(events.col("VehicleMessageID"), true).over(ws);
        events = events
                .withColumn("hypothetical_fi", hypotheticalFi)
                .withColumn("hypothetical_la", hypotheticalLa)
                .withColumn("time_of_last_transport_event", timePassed)
                .withColumn("vehicle_message_id", vehicleMessageId);
        return events.withColumn("time_between_validation_and_transport_event",
                unix_timestamp(events.col("timestamp"))
                        .minus(unix_timestamp(events.col("time_of_last_transport_event"))))
                .drop("timestamp_of_transport_event")
                .drop("VehicleMessageID");
    }


    /**
     * Try to use k-nearest neighbor or Hidden Markov Model
     */
    private static void classifyTransportEventsUsingSchedule() {

    }

        //Dataset<Row> breadCrumbs = sqlContext.sql("SELECT route, direction, stop_name, stop_id, trip, stop_sequence, stop_lat, stop_lon FROM bread_crumbs");
    private void proposeStopIdUsingCoordinateAndSchedule(Dataset<Row> breadCrumbs, Dataset<Row> events) {
        Dataset<Row> vehicles = events
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
        String dir = UUID.randomUUID().toString();
        doorsOpenedPositionsAndScheduledStops.coalesce(1).write()
                .option("header", "true").csv(System.getProperty("user.dir") + "/result/vehicles/" + dir);
    }

}
