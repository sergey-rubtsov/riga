package open.data.lv.spark.kd;

import open.data.lv.spark.Event;

import java.io.Serializable;
import java.util.List;

public class KDTreeTripsClassifier implements Serializable {

    private KDTree trip = new KDTree(4);

    public KDTreeTripsClassifier(List<Event> trip) {
        trip.forEach(t -> {
            double[] stop = new double[4];
            stop[0] = t.getLat();
            stop[1] = t.getLon();
            stop[2] = t.getTime();
            stop[3] = t.getOrder();
            this.trip.insert(stop, t.getId());
        });
    }

    public String findNearestNeighbourId(Double lat, Double lon, Double time, Double order) {
        double[] key = new double[4];
        key[0] = lat;
        key[1] = lon;
        key[2] = time;
        key[3] = order;
        return trip.nearest(key).toString();
    }
}
