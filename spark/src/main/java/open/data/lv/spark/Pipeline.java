package open.data.lv.spark;

import open.data.lv.spark.kd.KDTreeStopClassifier;
import open.data.lv.spark.utils.DatasetReader;
import org.apache.log4j.PropertyConfigurator;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
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
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static org.apache.spark.sql.functions.coalesce;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.concat;
import static org.apache.spark.sql.functions.count;
import static org.apache.spark.sql.functions.date_format;
import static org.apache.spark.sql.functions.first;
import static org.apache.spark.sql.functions.lag;
import static org.apache.spark.sql.functions.last;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.monotonically_increasing_id;
import static org.apache.spark.sql.functions.to_timestamp;
import static org.apache.spark.sql.functions.trim;
import static org.apache.spark.sql.functions.udf;
import static org.apache.spark.sql.functions.when;
import static org.apache.spark.sql.types.DataTypes.IntegerType;
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

    private static String SHAPES;

    private static String TRIPS;

    private static String SWARCO_TRIPS_MATCHING;

    private static String SWARCO_TRIP_COMPANY_MATCHING;

    private static String ROUTE_TYPES;

    private static void initPipelineParameters() {
        VEHICLE_MAPPING_FILE = "real/Vehicles.csv";
        MESSAGE_TYPE_FILE ="real/SendingReason.csv";
        TICKET_VALIDATIONS_FILES.add("real/ValidDati25_09_19.txt");
        VEHICLE_MESSAGES_FILES.add("real/VehicleMessages20190925d1.csv");
        VEHICLE_MESSAGES_FILES.add("real/VehicleMessages20190925d2.csv");

        ROUTES = "real/GTFS/routes.txt";
        STOP_TIMES = "real/GTFS/stop_times.txt";
        STOPS = "real/GTFS/stops.txt";
        SHAPES = "real/GTFS/shapes.txt";
        TRIPS = "real/GTFS/trips.txt";
        ROUTE_TYPES = "real/GTFS/route_types.txt";

        SWARCO_TRIPS_MATCHING = "real/transiti_v3.dta";
        SWARCO_TRIP_COMPANY_MATCHING = "real/Trips.csv";
    }

    public static void main(String[] args) {
        System.setProperty("hadoop.home.dir", System.getProperty("user.dir") + "\\spark\\hadoop");
        System.setProperty("SPARK_CONF_DIR", System.getProperty("user.dir") + "\\spark\\conf");
        PropertyConfigurator.configure(System.getProperty("user.dir") + "\\spark\\conf\\log4j.properties");
        //Logger.getLogger("org").setLevel(Level.WARN);
        //Logger.getLogger("akka").setLevel(Level.WARN);
        SparkConf conf = new SparkConf()
                .setMaster(MASTER_URL)
                .setAppName("Riga public transport")
                .set("SPARK_HOME", System.getProperty("user.dir"))
                .set("SPARK_CONF_DIR", System.getProperty("user.dir") + "\\spark\\conf");
        SparkSession spark = SparkSession
                .builder()
                .config(conf)
                .config("spark.executor.memory", "70g")
                .config("spark.driver.memory", "50g")
                .config("spark.memory.offHeap.enabled", true)
                .config("spark.memory.offHeap.size", "16g")
                .config("spark.debug.maxToStringFields", 100)
                .getOrCreate();
        SQLContext sqlContext = new SQLContext(spark);
        sqlContext.setConf("spark.sql.caseSensitive", "true");
        initPipelineParameters();
        Dataset<Row> routes = DatasetReader.readFiles(sqlContext, ROUTES, "HH:mm:ss", null, ",");
        Dataset<Row> routeTypes = DatasetReader.readFiles(sqlContext, ROUTE_TYPES, "HH:mm:ss", null, ",");
        Dataset<Row> routeMapping = buildRouteMapping(routes, routeTypes);
        Dataset<Row> stopTimes = DatasetReader.readFiles(sqlContext, STOP_TIMES, "HH:mm:ss", null, ",");
        Dataset<Row> stops = DatasetReader.readFiles(sqlContext, STOPS, "HH:mm:ss", null, ",");
        //Dataset<Row> shapes = DatasetReader.readFiles(sqlContext, SHAPES, "HH:mm:ss", null, ",");

        Dataset<Row> trips = DatasetReader.readFiles(sqlContext, TRIPS, "HH:mm:ss", null, ",");
        Dataset<Row> idealScheduleOfTrips = stopTimes.select(col("trip_id"),
                col("arrival_time").as("planned_time"),
                col("stop_id"),
                col("stop_sequence"))
                .join(trips.select("route_id" ,"service_id", "trip_id", "direction_id", "block_id" , "shape_id"),
                        new Set.Set1<>("trip_id").toSeq(), "left");

        Dataset<Row> swarcoTripsMatching = DatasetReader.readFiles(sqlContext, SWARCO_TRIPS_MATCHING, "HH:mm:ss", "", "|", false);

        //Trips.csv: TripID(*) -> TripCompanyCode(1)
        Dataset<Row> swarcoTripCompanyMatching = DatasetReader.readFiles(sqlContext, SWARCO_TRIP_COMPANY_MATCHING, null, null, ";");
        swarcoTripsMatching = swarcoTripsMatching.toDF(
                "number",
                "block_id",
                "stage_id",
                "not_used_0",
                "not_used_1",
                "planned_time",
                "not_used_2",
                "not_used_3",
                "direction_id",
                "TripCompanyCode",
                "empty");
        swarcoTripsMatching = swarcoTripsMatching.select(
                        col("block_id"),
                        col("planned_time"),
                        col("TripCompanyCode"));
        swarcoTripsMatching = swarcoTripsMatching.filter(col("TripCompanyCode").isNotNull());
        List<Stay> stays = idealScheduleOfTrips
                .join(swarcoTripsMatching,
                new Set.Set2<>("block_id", "planned_time").toSeq(), "left")
                .join(stops, new Set.Set1<>("stop_id").toSeq(), "left")
                .select(col("block_id"),
                        col("planned_time"),
                        col("trip_id"),
                        col("stop_id"),
                        col("stop_sequence"),
                        col("route_id"),
                        col("service_id"),
                        col("direction_id"),
                        col("shape_id"),
                        col("TripCompanyCode").as("tripCompanyCode"),
                        col("stop_id"),
                        col("stop_lat").as("lat"),
                        col("stop_lon").as("lon")).as(Encoders.bean(Stay.class)).collectAsList();
        KDTreeStopClassifier swarcoStopClassifier = new KDTreeStopClassifier();
        swarcoStopClassifier.addStays(stays);
        Dataset<Row> vehicleMessages = DatasetReader.readFiles(sqlContext, VEHICLE_MESSAGES_FILES, "yyyy-MM-dd HH:mm:ss.SSS", "NULL", ";");
        Dataset<Row> vehicleAndCompanyMapping = DatasetReader.readFiles(sqlContext, VEHICLE_MAPPING_FILE, null, null, ";");
        Dataset<Row> eventTypes = DatasetReader.readFiles(sqlContext, MESSAGE_TYPE_FILE, null, null, ";");
        Dataset<Row> transportEvents = prepareTransportEventsForJoin(vehicleMessages, eventTypes, vehicleAndCompanyMapping);
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
        KDTreeStopClassifier routeStopClassifier = new KDTreeStopClassifier();
        routeStopClassifier.addStops(points);
        Dataset<Row> tickets = DatasetReader.readFiles(sqlContext, TICKET_VALIDATIONS_FILES, "dd.MM.yyyy HH:mm:ss", null, null);
        Dataset<Row> validationEvents = prepareValidationEventsForJoin(tickets, routeMapping);
        transportEvents = transportEvents
                .join(swarcoTripCompanyMatching, new Set.Set1<>("TripID").toSeq(),"left")
                .withColumn("time", to_timestamp(col("time"), "HH:mm:ss"));
        transportEvents = transportEvents
                .withColumn("event_source",
                        lit("vehicle"));
        validationEvents = validationEvents
                .withColumn("event_source",
                        lit("passenger"));
        Dataset<Row> events = unionDatasets(validationEvents, vehicleAndCompanyMapping, transportEvents);
        events = events
                .withColumn("nearest_stop_id", findNearestStopFunction(routeStopClassifier)
                .apply(col("route"),
                        col("WGS84Fi"),
                        col("WGS84La")));
        WindowSpec ws = Window
                .partitionBy(events.col("GarNr"))
                .orderBy(events.col("timestamp"))
                .rowsBetween(Integer.MIN_VALUE + 1, Window.currentRow());
        events = events
                .withColumn("TripCompanyCode",
                        coalesce(col("TripCompanyCode"), last(events.col("TripCompanyCode"), true).over(ws)))
                .withColumn("stop_id",
                        coalesce(col("nearest_stop_id"), last(events.col("nearest_stop_id"), true).over(ws)))
                .withColumn("stop_lat",
                        coalesce(col("WGS84Fi"), last(events.col("WGS84Fi"), true).over(ws)))
                .withColumn("stop_lon",
                        coalesce(col("WGS84La"), last(events.col("WGS84La"), true).over(ws)))
                .withColumn("TripID",
                        coalesce(col("TripID"), last(events.col("TripID"), true).over(ws)))
                .withColumn("route_id",
                        coalesce(col("route_id"), last(events.col("route_id"), true).over(ws)))
        .drop("WGS84Fi", "WGS84La", "nearest_stop_id");
        events = events.withColumn("swarco_stop_id", nearestScheduledStopIdFunction(swarcoStopClassifier).apply(
                col("TripCompanyCode"),
                col("stop_lat"),
                col("stop_lon")));
        events = events.withColumn("swarco_arrive_time", nearestScheduledPlannedTimeFunction(swarcoStopClassifier).apply(
                col("TripCompanyCode"),
                col("stop_lat"),
                col("stop_lon")));
        events = events.withColumn("swarco_direction", nearestScheduledDirectionFunction(swarcoStopClassifier).apply(
                col("TripCompanyCode"),
                col("stop_lat"),
                col("stop_lon")));
        events = events.filter(col("event_source").equalTo("passenger"))
                .drop("VehicleMessageID", "Code", "event_source");
        Dataset<Row> predicted = predictExitsForTwoOrMoreTransactions(events, routeStopClassifier, swarcoStopClassifier);
        predicted = predicted.join(events,
                new Set.Set2<>("ValidTalonaId", "timestamp").toSeq(), "right");
        predicted.orderBy("ValidTalonaId", "timestamp").repartition(1).write()
                .option("header", "true").csv(System.getProperty("user.dir") + "\\result\\" + LocalDateTime.now()
                .format(DateTimeFormatter.ofPattern("yyyy-MM-dd-HH-mm-ss-SSS")));
        //predicted.write().json(System.getProperty("user.dir") + "\\json\\");
        spark.stop();
    }

    private static Dataset<Row> predictExitsForTwoOrMoreTransactions(Dataset<Row> enters,
                                                                     KDTreeStopClassifier kdTreeStopClassifier,
                                                                     KDTreeStopClassifier swarcoStopClassifier) {
        WindowSpec ws = Window
                .partitionBy(enters.col("ValidTalonaId"))
                .orderBy(enters.col("timestamp"));
        return enters
                .withColumn("exit_stop_id",
                        findNearestStopFunction(kdTreeStopClassifier).apply(
                                col("route"),
                                lag(col("stop_lat"), -1, null).over(ws),
                                lag(col("stop_lon"), -1, null).over(ws)))
                .withColumn("exit_swarco_stop_id",
                        nearestScheduledStopIdFunction(swarcoStopClassifier).apply(
                            col("TripCompanyCode"),
                            lag(col("stop_lat"), -1, null).over(ws),
                            lag(col("stop_lon"), -1, null).over(ws)))
                .withColumn("exit_swarco_arrive_time",
                        nearestScheduledPlannedTimeFunction(swarcoStopClassifier).apply(
                            col("TripCompanyCode"),
                            lag(col("stop_lat"), -1, null).over(ws),
                            lag(col("stop_lon"), -1, null).over(ws)))
                .select(
                        col("ValidTalonaId"),
                        col("timestamp"),
                        col("exit_stop_id"),
                        col("exit_swarco_stop_id"),
                        col("exit_swarco_arrive_time"));
    }

    private static Dataset<Row> buildRouteMapping(Dataset<Row> routes, Dataset<Row> routeTypes) {
        return routes.join(routeTypes, new Set.Set1<>("route_type").toSeq(), "inner")
                .withColumn("numeric_route_id", monotonically_increasing_id())
                .select(col("route_id"),
                        trim(concat(col("short_name"), lit(" "), col("route_short_name"))).as("route"),
                        col("numeric_route_id"));
    }

    private static Dataset<Row> buildRegularRoutesFromSchedule(Dataset<Row> schedule) {
        return schedule.groupBy(
                col("route"),
                col("direction_id"),
                col("direction"),
                col("stop_name"),
                col("stop_id"),
                col("stop_lat"),
                col("stop_lon"))
                .agg(first(col("stop_sequence")).as("stop_sequence"),
                        first(col("trip_id")).as("trip_id"));
    }

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
                        col("WGS84La")
                );
        //filter open doors only and broken data
        transportEvents = transportEvents
                .filter(transportEvents.col("Code").equalTo("DoorsOpen")
                        .and(transportEvents.col("WGS84La").notEqual(0.0))
                        .and(transportEvents.col("WGS84Fi").notEqual(0.0))
                );
        transportEvents = transportEvents.withColumn("date",
                transportEvents.col("SentDate").cast(DataTypes.DateType));
        return transportEvents.withColumn("time",
                date_format(transportEvents.col("SentDate"), "HH:mm:ss"));
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
                date_format(validationEvents.col("time_stamp"), "HH:mm:ss"))
                .join(routeMapping, validationEvents.col("TMarsruts").equalTo(routeMapping.col("route")), "inner")
                .drop("TMarsruts");
    }

    private static UserDefinedFunction findNearestStopFunction(KDTreeStopClassifier classifier) {
        return udf(
                (UDF3<String, Double, Double, Object>) classifier::findNearestNeighbourId, StringType
        );
    }

    private static UserDefinedFunction nearestScheduledStopIdFunction(KDTreeStopClassifier classifier) {
        return udf(
                (UDF3<String, Double, Double, Object>) classifier::nearestScheduledStopIdFunction, StringType
        );
    }

    private static UserDefinedFunction nearestScheduledPlannedTimeFunction(KDTreeStopClassifier classifier) {
        return udf(
                (UDF3<String, Double, Double, Object>) classifier::nearestScheduledPlannedTimeFunction, StringType
        );
    }

    private static UserDefinedFunction nearestScheduledDirectionFunction(KDTreeStopClassifier classifier) {
        return udf(
                (UDF3<String, Double, Double, Object>) classifier::nearestScheduledDirectionFunction, StringType
        );
    }

    private static UserDefinedFunction nearestScheduledDirectionIdFunction(KDTreeStopClassifier classifier) {
        return udf(
                (UDF3<String, Double, Double, Object>) classifier::nearestScheduledDirectionIdFunction, IntegerType
        );
    }

    private static UserDefinedFunction nearestScheduledStopSequenceFunction(KDTreeStopClassifier classifier) {
        return udf(
                (UDF3<String, Double, Double, Object>) classifier::nearestScheduledStopSequenceFunction, IntegerType
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

    private static Dataset<Row> unionDatasets(Dataset<Row> validations,
                                              Dataset<Row> companyMapping,
                                              Dataset<Row> vehicle) {
        // here we map vehicles event with VehicleID and validation events with route,
        // we will lose events, if there is no mapping in companyMapping table
        Dataset<Row> vehiclesOnRoute = validations.join(companyMapping,
                validations.col("GarNr").equalTo(companyMapping.col("VehicleCompanyCode")),
                "inner")
                .groupBy(col("GarNr"), col("VehicleID")).agg(
                        first(col("route")).as("route"),
                        first(col("numeric_route_id")).as("numeric_route_id"),
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
        Seq<String> garageNumberColumn = new Set.Set1<>("GarNr").toSeq();
        validations = validations
                .join(vehiclesOnRoute
                        .select(
                                col("VehicleID"),
                                col("GarNr")),
                        garageNumberColumn, "left");
        Dataset<Row> consolidated = unionDatasets(validations, vehicle);
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
                col("route_id"),
                col("Virziens").as("direction"),
                col("TripID"),
                col("Code"),
                col("WGS84Fi"),
                col("WGS84La"),
                col("event_source"),
                col("ValidTalonaId"),
                col("TripCompanyCode"),
                col("time"),
                col("timestamp"));
        return consolidated;
    }

    private static Dataset<Row> unionDatasets(Dataset<Row> one, Dataset<Row> another) {
        StructType firstSchema = one.schema();
        List<String> transportFields = Arrays.asList(another.schema().fieldNames());
        another = balanceDataset(another, firstSchema, transportFields);
        StructType secondSchema = another.schema();
        List<String> validationFields = Arrays.asList(one.schema().fieldNames());
        one = balanceDataset(one, secondSchema, validationFields);
        return another.unionByName(one);
    }

    private static Dataset<Row> balanceDataset(Dataset<Row> dataset, StructType schema, List<String> fields) {
        for (StructField e : schema.fields()) {
            if (!fields.contains(e.name())) {
                dataset = dataset
                        .withColumn(e.name(),
                                lit(null));
                dataset = dataset.withColumn(e.name(),
                        dataset.col(e.name()).cast(Optional.ofNullable(e.dataType()).orElse(StringType)));
            }
        }
        return dataset;
    }

}
