package open.data.lv.spark;

import open.data.lv.spark.kd.KDTreeStopClassifier;
import open.data.lv.spark.utils.DatasetReader;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.apache.spark.SparkConf;
import org.apache.spark.ml.classification.NaiveBayes;
import org.apache.spark.ml.classification.NaiveBayesModel;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.sql.*;
import org.apache.spark.sql.api.java.UDF3;
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
import java.util.Optional;
import java.util.UUID;

import static org.apache.spark.sql.functions.abs;
import static org.apache.spark.sql.functions.coalesce;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.concat;
import static org.apache.spark.sql.functions.count;
import static org.apache.spark.sql.functions.date_format;
import static org.apache.spark.sql.functions.first;
import static org.apache.spark.sql.functions.hour;
import static org.apache.spark.sql.functions.lag;
import static org.apache.spark.sql.functions.last;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.max;
import static org.apache.spark.sql.functions.min;
import static org.apache.spark.sql.functions.minute;
import static org.apache.spark.sql.functions.monotonically_increasing_id;
import static org.apache.spark.sql.functions.trim;
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

    final private static int IMPOSSIBLE_TIME = 2880;
    final private static int IMPOSSIBLE_COORDINATE = 0;

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

    public static void main(String[] args) {
        System.setProperty("hadoop.home.dir", System.getProperty("user.dir") + "\\hadoop");
        System.setProperty("SPARK_CONF_DIR", System.getProperty("user.dir") + "\\conf");
        PropertyConfigurator.configure(System.getProperty("user.dir") + "\\conf\\log4j.properties");
        //This two lines hide spark logs
        Logger.getLogger("org").setLevel(Level.ERROR);
        Logger.getLogger("akka").setLevel(Level.ERROR);
        SparkConf conf = new SparkConf()
                .setMaster(MASTER_URL)
                .setAppName("Riga public transport")
                .set("SPARK_HOME", System.getProperty("user.dir"))
                .set("SPARK_CONF_DIR", System.getProperty("user.dir") + "\\conf");
        SparkSession spark = SparkSession
                .builder()
                .config(conf)
                .config("spark.executor.memory", "70g")
                .config("spark.driver.memory", "50g")
                .config("spark.memory.offHeap.enabled", true)
                .config("spark.memory.offHeap.size", "16g")
                .getOrCreate();
        SQLContext sqlContext = new SQLContext(spark);
        sqlContext.setConf("spark.sql.caseSensitive", "true");
        initPipelineParameters();
        //Prepare training set
        Dataset<Row> routes = DatasetReader.readFiles(sqlContext, ROUTES, "HH:mm:ss", null, ",");
        Dataset<Row> routeTypes = DatasetReader.readFiles(sqlContext, ROUTE_TYPES, "HH:mm:ss", null, ",");
        Dataset<Row> routeMapping = buildRouteMapping(routes, routeTypes);
        Dataset<Row> stopTimes = DatasetReader.readFiles(sqlContext, STOP_TIMES, "HH:mm:ss", null, ",");
        Dataset<Row> stops = DatasetReader.readFiles(sqlContext, STOPS, "HH:mm:ss", null, ",");

        Dataset<Row> trips = DatasetReader.readFiles(sqlContext, TRIPS, "HH:mm:ss", null, ",");

        Dataset<Row> dailySchedule = buildDailySchedule(routes, stopTimes, stops, trips, routeMapping);
        Dataset<Row> regularRoutesFromSchedule = buildRegularRoutesFromSchedule(dailySchedule);

        Dataset<Stop> coordinates = regularRoutesFromSchedule
                .select(col("route"),
                        col("direction_id").as("dir"),
                        col("stop_id").as("id"),
                        col("stop_lat").as("lat"),
                        col("stop_lon").as("lon"))
                .as(Encoders.bean(Stop.class));
        List<Stop> points = coordinates.collectAsList();
        KDTreeStopClassifier kdTreeStopClassifier = new KDTreeStopClassifier(points);

        //Prepare test set
        Dataset<Row> tickets = DatasetReader.readFiles(sqlContext, TICKET_VALIDATIONS_FILES, "dd.MM.yyyy HH:mm:ss", null, null);
        Dataset<Row> vehicleMessages = DatasetReader.readFiles(sqlContext, VEHICLE_MESSAGES_FILES, "yyyy-MM-dd HH:mm:ss.SSS", "NULL", null); //real delimeter is ;
        Dataset<Row> vehicleAndCompanyMapping = DatasetReader.readFiles(sqlContext, VEHICLE_MAPPING_FILE, null, null, ";");
        Dataset<Row> eventTypes = DatasetReader.readFiles(sqlContext, MESSAGE_TYPE_FILE, null, null, ";");
        Dataset<Row> validationEvents = prepareValidationEventsForJoin(tickets, routeMapping);
        Dataset<Row> transportEvents = prepareTransportEventsForJoin(vehicleMessages, eventTypes, vehicleAndCompanyMapping);
        Dataset<Row> events = unionDataSets(validationEvents, vehicleAndCompanyMapping, transportEvents);
        events = events
                .withColumn("nearest_stop_id", findNearestStopFunction(kdTreeStopClassifier)
                .apply(col("route"),
                        col("WGS84Fi"),
                        col("WGS84La")));
        events = events
                .withColumn("euclidean_nearest_stop_id", findNearestStopFunction(kdTreeStopClassifier)
                        .apply(col("route"),
                                col("WGS84Fi"),
                                col("WGS84La")));
        events.show(200);
        prepareTestDataset(events).orderBy(col("TripID"), col("timestamp")).show(100);
        spark.stop();
    }

    private static Dataset<Row> buildRouteMapping(Dataset<Row> routes, Dataset<Row> routeTypes) {
        return routes.join(routeTypes, new Set.Set1<>("route_type").toSeq(), "inner")
                .withColumn("numeric_route_id", monotonically_increasing_id())
                .select(col("route_id"),
                        trim(concat(col("short_name"), lit(" "), col("route_short_name"))).as("route"),
                        col("numeric_route_id"));
    }


    private static Dataset<Row> prepareTestDataset(Dataset<Row> events) {
        return events.filter(col("event_source").equalTo("vehicle"))
                .withColumnRenamed("WGS84La","stop_lon")
                .withColumnRenamed("WGS84Fi","stop_lat")
                .withColumn("timestamp", hour(col("timestamp_of_transport_event"))
                .multiply(60)
                .plus(minute(col("timestamp_of_transport_event"))))
                .groupBy(col("TripID"))
                .agg(
                        first(col("route")).as("route"),
                        first(col("nearest_stop_id")).as("nearest_stop_id"),
                        first(col("stop_lat")).as("stop_lat"),
                        first(col("stop_lon")).as("stop_lon"),
                        first(col("timestamp")).as("timestamp"))
                .orderBy(col("timestamp"));
/*        WindowSpec ws = Window
                .partitionBy(events.col("TripID"))
                .orderBy(events.col("time"));
        return events
                .withColumn("next_stop_lat", lag(col("stop_lat"), -1, IMPOSSIBLE_COORDINATE).over(ws))
                .withColumn("next_stop_lon", lag(col("stop_lon"), -1, IMPOSSIBLE_COORDINATE).over(ws))
                .withColumn("previous_stop_lat", lag(col("stop_lat"), 1, IMPOSSIBLE_COORDINATE).over(ws))
                .withColumn("previous_stop_lon", lag(col("stop_lon"), 1, IMPOSSIBLE_COORDINATE).over(ws))
                .withColumn("next_timestamp", lag(col("timestamp"), -1, IMPOSSIBLE_TIME).over(ws))
                .withColumn("previous_timestamp", lag(col("timestamp"), 1, IMPOSSIBLE_TIME).over(ws));*/
    }

    /**
     * +-----+---------+--------------------+-------+--------+--------+-------------+---------------------+
     * |route|direction|           stop_name|stop_id|stop_lat|stop_lon|stop_sequence|first(trip_id, false)|
     * +-----+---------+--------------------+-------+--------+--------+-------------+---------------------+
     * |  A 1|     Back|          Berģuciems|   0342|56.97801|24.30995|            1|                   99|
     */
    private static Dataset<Row> buildRegularRoutesFromSchedule(Dataset<Row> schedule) {
        return schedule.groupBy(
                col("route"),
                col("direction_id"),
                col("numeric_route_id"),
                col("direction"),
                col("stop_name"),
                col("stop_id"),
                col("stop_lat"),
                col("stop_lon"))
                //.agg(first(col("stop_sequence")).as("stop_sequence"), first(col("trip_id")));
                .agg(first(col("stop_sequence")).as("stop_sequence"));
    }

    private static Dataset<Row> prepareTrainDataset(Dataset<Row> schedule) {
        schedule = schedule.withColumn("timestamp", hour(col("arrival_time"))
                .multiply(60)
                .plus(minute(col("arrival_time"))));
                WindowSpec ws = Window
                .partitionBy(schedule.col("trip_id"))
                .orderBy(schedule.col("stop_sequence"));
        return schedule
                .withColumn("next_stop_lat", lag(col("stop_lat"), -1, IMPOSSIBLE_COORDINATE).over(ws))
                .withColumn("next_stop_lon", lag(col("stop_lon"), -1, IMPOSSIBLE_COORDINATE).over(ws))
                .withColumn("previous_stop_lat", lag(col("stop_lat"), 1, IMPOSSIBLE_COORDINATE).over(ws))
                .withColumn("previous_stop_lon", lag(col("stop_lon"), 1, IMPOSSIBLE_COORDINATE).over(ws))
                .withColumn("next_timestamp", lag(col("timestamp"), -1, IMPOSSIBLE_TIME).over(ws))
                .withColumn("previous_timestamp", lag(col("timestamp"), 1, IMPOSSIBLE_TIME).over(ws));
    }

    /**
     * +------------+-----+------------+------------------+-------------------+--------+--------+-------+------------+--------------+-------------+-------+---------+
     * |       label|route|direction_id|  numeric_route_id|          stop_name|stop_lat|stop_lon|stop_id|arrival_time|departure_time|stop_sequence|trip_id|direction|
     * +------------+-----+------------+------------------+-------------------+--------+--------+-------+------------+--------------+-------------+-------+---------+
     * |214748365657|  A 1|           1|                 1|         Berģuciems|56.97801|24.30995|   0342|    05:45:00|      05:45:00|            1|     99|     Back|
     * |214748365699|  A 1|           1|                 1|        Mākoņu iela|56.97986|24.30494|   0258|    05:46:00|      05:46:00|            2|     99|     Back|
     */
    private static Dataset<Row> buildDailySchedule(Dataset<Row> routes,
                                                   Dataset<Row> stopTimes,
                                                   Dataset<Row> stops,
                                                   Dataset<Row> trips,
                                                   Dataset<Row> numericRouteIdMapping) {
        Dataset<Row> schedule = stopTimes
                .join(stops, new Set.Set1<>("stop_id").toSeq(), "inner")
                .join(trips, new Set.Set1<>("trip_id").toSeq(), "inner")
                .join(routes, new Set.Set1<>("route_id").toSeq(),"inner")
                .join(numericRouteIdMapping, new Set.Set1<>("route_id").toSeq(),"inner")
                .select(
                        col("route"),
                        col("direction_id"),
                        col("numeric_route_id"),
                        col("route_id"),
                        col("stop_name"),
                        col("stop_lat"),
                        col("stop_lon"),
                        col("stop_id"),
                        col("arrival_time"),
                        col("departure_time"),
                        col("stop_sequence"),
                        col("trip_id"));
        return schedule
                .withColumn("direction", when(col("direction_id").equalTo(0), "Forth")
                        .when(col("direction_id").equalTo(1), "Back"))
                .withColumn("label", monotonically_increasing_id());
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

    private static Dataset<Row> prepareValidationEventsForJoin(Dataset<Row> tickets, Dataset<Row> routeMapping) {
        Dataset<Row> validationEvents = tickets.select(
                col("GarNr").as("GN"),
                trim(col("TMarsruts")).as("TMarsruts"),
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
                date_format(validationEvents.col("time_stamp"), "h:m:s a"))
                .join(routeMapping, validationEvents.col("TMarsruts").equalTo(routeMapping.col("route")), "inner")
                .drop("TMarsruts");
    }

    private static UserDefinedFunction findNearestStopFunction(KDTreeStopClassifier classifier) {
        return udf(
                (UDF3<String, Double, Double, Object>) classifier::findNearestNeighbourId, StringType
        );
    }

    private static UserDefinedFunction findNearestUsingEuclideanFunction(KDTreeStopClassifier classifier) {
        return udf(
                (UDF3<String, Double, Double, Object>) classifier::findNearestNeighbourId, StringType
        );
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
                        first(col("route")).as("route"),
                        first(col("numeric_route_id")).as("numeric_route_id"),
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
                        .select(
                                col("VehicleID"),
                                col("route"),
                                col("numeric_route_id")), vehicleIDColumn, "inner");
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
        consolidated = consolidated.select(
                col("VehicleID"),
                col("VehicleMessageID"),
                col("GarNr"),
                col("route"),
                col("numeric_route_id"),
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
                col("timestamp_of_transport_event"));
        return consolidated;
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
