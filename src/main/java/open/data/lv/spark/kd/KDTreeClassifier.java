package open.data.lv.spark.kd;

import open.data.lv.spark.Stop;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class KDTreeClassifier implements Serializable {

    private Map<String, KDTree> routes = new HashMap<>();

    public KDTreeClassifier(List<Stop> points) {
        points.forEach(p -> {
            double[] key = new double[2];
            key[0] = p.getLat();
            key[1] = p.getLon();
            routes.computeIfAbsent(p.getRoute(), t ->
                new KDTree(2)
            ).insert(key, p.getId());
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
            return routes.get(route).nearest(key).toString();
        }
        return null;
    }

}
