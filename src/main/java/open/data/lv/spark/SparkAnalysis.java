package open.data.lv.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Row;

public class SparkAnalysis {

    public static void main(String[] args) {
        ClassLoader classLoader = SparkAnalysis.class.getClassLoader();
        String masterUrl = "local[1]";
        if (args.length > 0) {
            masterUrl = args[0];
        } else if (args.length > 1) {
        }
        SparkConf conf = new SparkConf().setMaster(masterUrl).setAppName("Riga public transport");
        SparkSession spark = SparkSession
                .builder()
                .config(conf)
                .getOrCreate();
        SQLContext sqlContext = new SQLContext(spark);


        Dataset<Row> tickets = sqlContext.read().csv(classLoader.getResource("tickets_validation_2.csv").getPath());
        tickets.printSchema();
        tickets.createOrReplaceTempView("tickets");
        Dataset<Row> routes = sqlContext.read().csv(classLoader.getResource("GPS_routes.csv").getPath());
        routes.printSchema();
        routes.createOrReplaceTempView("routes");
        Dataset<Row> reasons = sqlContext.read().csv(classLoader.getResource("SendingReasons.csv").getPath());
        reasons.printSchema();
        reasons.createOrReplaceTempView("reasons");
        Dataset<Row> vehicles = sqlContext.read().csv(classLoader.getResource("Vehicles.csv").getPath());
        vehicles.printSchema();
        vehicles.createOrReplaceTempView("vehicles");

        Dataset<Row> result = sqlContext.sql("SELECT * FROM tickets");
        result.show(60);
        result.coalesce(1).write().option("header", "true").csv(System.getProperty("user.dir") + "/data/");
//
//        Dataset<Row> repos = sqlContext
//                .sql("SELECT name, full_name, description, explode(collaborators) as collaborator FROM repositories");
//        repos.createOrReplaceTempView("repositories");
//
//
//        Dataset<Row> result = sqlContext.sql("SELECT * FROM people");
//        result.show(60);
//
//
//        repos = sqlContext
//                .sql("SELECT repositories.name, repositories.full_name, repositories.description, people.login, people.name FROM repositories " +
//                        "INNER JOIN people ON repositories.collaborator=people.id");
//        repos.show(100);
        /*
        SELECT Orders.OrderID, Customers.CustomerName, Orders.OrderDate FROM Orders
        INNER JOIN Customers ON Orders.CustomerID=Customers.CustomerID;
         */

        //result.coalesce(1).write().option("header", "true").csv(System.getProperty("user.dir") + "/data/" + System.currentTimeMillis());


    }
}
