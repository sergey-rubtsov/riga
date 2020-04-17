package open.data.lv.spark;

import java.io.Serializable;

public class Stay implements Serializable, Point {

    private int block_id;

    private String planned_time;

    private int trip_id;

    private String stop_id;

    private double lat;

    private double lon;

    private int stop_sequence;

    private String route_id;

    private int service_id;

    private int direction_id;

    private String shape_id;

    private String TripCompanyCode;

    public Stay() {
    }

    public Stay(int block_id, String planned_time, int trip_id, String stop_id, double lat, double lon, int stop_sequence, String route_id, int service_id, int direction_id, String shape_id, String tripCompanyCode) {
        this.block_id = block_id;
        this.planned_time = planned_time;
        this.trip_id = trip_id;
        this.stop_id = stop_id;
        this.lat = lat;
        this.lon = lon;
        this.stop_sequence = stop_sequence;
        this.route_id = route_id;
        this.service_id = service_id;
        this.direction_id = direction_id;
        this.shape_id = shape_id;
        TripCompanyCode = tripCompanyCode;
    }

    public int getBlock_id() {
        return block_id;
    }

    public void setBlock_id(int block_id) {
        this.block_id = block_id;
    }

    public String getPlanned_time() {
        return planned_time;
    }

    public void setPlanned_time(String planned_time) {
        this.planned_time = planned_time;
    }

    public int getTrip_id() {
        return trip_id;
    }

    public void setTrip_id(int trip_id) {
        this.trip_id = trip_id;
    }

    @Override
    public String getKey() {
        return TripCompanyCode;
    }

    public double getLat() {
        return lat;
    }

    public void setLat(double lat) {
        this.lat = lat;
    }

    public double getLon() {
        return lon;
    }

    public void setLon(double lon) {
        this.lon = lon;
    }

    public String getStop_id() {
        return stop_id;
    }

    public void setStop_id(String stop_id) {
        this.stop_id = stop_id;
    }

    public int getStop_sequence() {
        return stop_sequence;
    }

    public void setStop_sequence(int stop_sequence) {
        this.stop_sequence = stop_sequence;
    }

    public String getRoute_id() {
        return route_id;
    }

    public void setRoute_id(String route_id) {
        this.route_id = route_id;
    }

    public int getService_id() {
        return service_id;
    }

    public void setService_id(int service_id) {
        this.service_id = service_id;
    }

    public int getDirection_id() {
        return direction_id;
    }

    public void setDirection_id(int direction_id) {
        this.direction_id = direction_id;
    }

    public String getShape_id() {
        return shape_id;
    }

    public void setShape_id(String shape_id) {
        this.shape_id = shape_id;
    }

    public String getTripCompanyCode() {
        return TripCompanyCode;
    }

    public void setTripCompanyCode(String tripCompanyCode) {
        TripCompanyCode = tripCompanyCode;
    }
}
