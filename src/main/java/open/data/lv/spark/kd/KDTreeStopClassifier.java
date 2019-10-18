package open.data.lv.spark.kd;

import open.data.lv.spark.Stop;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static java.lang.Math.pow;
import static java.lang.Math.sqrt;

public class KDTreeStopClassifier implements Serializable {

    private Map<String, KDTree> routes = new HashMap<>();

    public KDTreeStopClassifier(List<Stop> points) {
        points.forEach(p -> {
            double[] key = new double[2];
            key[0] = p.getLat();
            key[1] = p.getLon();
            routes.computeIfAbsent(p.getRoute(), t ->
                new KDTree(2)
            ).insert(key, p);
        });
    }

    public String findNearestNeighbourId(String route, Double lat, Double lon) {
        if (route == null || lat == null || lon == null) {
            return null;
        }
        if (routes.containsKey(route)) {
            double[] key = new double[2];
            key[0] = lat;
            key[1] = lon;
            return ((Stop) routes.get(route).nearest(key)).getId();
        }
        return null;
    }

    public String findNearestUsingEuclidean(String route, Double lat, Double lon) {
        if (route == null || lat == null || lon == null) {
            return null;
        }
        if (routes.containsKey(route)) {
            double[] key = new double[2];
            key[0] = lat;
            key[1] = lon;
            Object[] found = routes.get(route).nearest(key, 3);
            return closest(lat, lon, (Stop[]) found).getId();
        }
        return null;
    }

    private Stop closest(Double lat, Double lon, Stop[] stops) {
        Stream<Stop> stream = Arrays.stream(stops);
        return stream.min(Comparator.comparing(stop -> calculateDistance(lat, lon, stop))).orElse(stops[0]);
    }

    //Since the distance is relatively small, we can use the rectangular distance approximation using formula
    //SQRT(POW((stop_lat - hypothetical_fi), 2) + POW((stop_lon - hypothetical_la), 2))
    //but we need to translate grades into km.
    //This approximation is faster than using the Haversine formula.
    //But for comparing distances we can compare squares of coordinate differences without square root calculation.
    private Double calculateDistance(Double lat, Double lon, Stop stop) {
        return sqrt(pow((stop.getLat() - lat) * 111.3, 2) + pow((stop.getLon() - lon) * 60.8, 2));
    }

}
