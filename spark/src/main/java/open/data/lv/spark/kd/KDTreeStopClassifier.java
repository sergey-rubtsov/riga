package open.data.lv.spark.kd;

import open.data.lv.spark.Stay;
import open.data.lv.spark.Stop;

import java.io.Serializable;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static java.lang.Math.pow;
import static java.lang.Math.sqrt;

public class KDTreeStopClassifier implements Serializable {

    private Map<String, KDTree> routes = new HashMap<>();

    public KDTreeStopClassifier() {
    }

    public void addStops(List<Stop> points) {
        points.forEach(p -> {
            double[] key = new double[2];
            key[0] = p.getLat();
            key[1] = p.getLon();
            routes.computeIfAbsent(p.getKey(), t ->
                    new KDTree(2)
            ).insert(key, p);
        });
    }

    public void addStays(List<Stay> points) {
        points.forEach(p -> {
            double[] key = new double[2];
            key[0] = p.getLat();
            key[1] = p.getLon();
            routes.computeIfAbsent(p.getKey(), t ->
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

    private Optional<Stay> findNearestStay(String tripCompanyCode, Double lat, Double lon) {
        if (tripCompanyCode == null || lat == null || lon == null) {
            return Optional.empty();
        }
        if (routes.containsKey(tripCompanyCode)) {
            double[] key = new double[2];
            key[0] = lat;
            key[1] = lon;
            return Optional.ofNullable((Stay) routes.get(tripCompanyCode).nearest(key));
        }
        return Optional.empty();
    }

    public String nearestScheduledStopIdFunction(String tripCompanyCode, Double lat, Double lon) {
        return findNearestStay(tripCompanyCode, lat, lon).map(Stay::getStop_id).orElse(null);
    }

    public String nearestScheduledPlannedTimeFunction(String tripCompanyCode, Double lat, Double lon) {
        return findNearestStay(tripCompanyCode, lat, lon).map(Stay::getPlanned_time).orElse(null);
    }

    public Integer nearestScheduledDirectionIdFunction(String tripCompanyCode, Double lat, Double lon) {
        return findNearestStay(tripCompanyCode, lat, lon).map(Stay::getDirection_id).orElse(null);
    }

    public String nearestScheduledDirectionFunction(String tripCompanyCode, Double lat, Double lon) {
        return findNearestStay(tripCompanyCode, lat, lon).map(stay -> {
            if (stay.getDirection_id() == 0) {
                return "Forth";
            } else if (stay.getDirection_id() == 1) {
                return "Back";
            } else return null;
        }).orElse(null);
    }

    public Integer nearestScheduledStopSequenceFunction(String tripCompanyCode, Double lat, Double lon) {
        return findNearestStay(tripCompanyCode, lat, lon).map(Stay::getStop_sequence).orElse(null);
    }

    public String findNearestAndThenNearestButInAnotherDirectionId(String route,
                                                                   Double lat,
                                                                   Double lon) {
        if (route == null || lat == null || lon == null) {
            return null;
        }
        if (routes.containsKey(route)) {
            double[] key = new double[2];
            key[0] = lat;
            key[1] = lon;
            KDTree routeTree = routes.get(route);
            int dir = ((Stop)routeTree.nearest(key)).getDir();
            for (int i = 2; i < routeTree.getNodesCount(); i++) {
                Object[] found = routeTree.nearest(key, i);
                for (Object stop : found) {
                    if (((Stop)stop).getDir() != dir) {
                        return ((Stop)stop).getId();
                    }
                }
            }
        }
        return null;
    }

    public Stop closest(Double lat, Double lon, List<Stop> stops) {
        return stops.stream().min(Comparator.comparing(stop -> calculateDistance(lat, lon, stop))).orElse(stops.get(0));
    }

    //Since the distance is relatively small, we can use the rectangular distance approximation using formula
    //SQRT(POW((stop_lat - hypothetical_fi), 2) + POW((stop_lon - hypothetical_la), 2))
    //but we need to translate grades into km.
    //This approximation is faster than using the Haversine formula.
    //But for comparing distances we can compare squares of coordinate differences without square root calculation.
    private Double calculateDistance(Double lat, Double lon, Stop stop) {
        return sqrt(pow((stop.getLat() - lat) * 111.3, 2) + pow((stop.getLon() - lon) * 60.8, 2));
    }

    public String findNearestNeighbourIdOnSameDirection(String route, Double lat, Double lon) {
        if (route == null || lat == null || lon == null) {
            return null;
        }
        if (routes.containsKey(route)) {
            double[] key = new double[2];
            key[0] = lat;
            key[1] = lon;
            KDTree routeTree = routes.get(route);
            int dir = ((Stop)routeTree.nearest(key)).getDir();
            for (int i = 2; i < routeTree.getNodesCount(); i++) {
                Object[] found = routeTree.nearest(key, i);
                for (Object stop : found) {
                    if (((Stop)stop).getDir() == dir) {
                        return ((Stop)stop).getId();
                    }
                }
            }
        }
        return null;
    }
}
