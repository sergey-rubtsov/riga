package open.data.lv.spark;

import java.io.Serializable;

public class Event implements Serializable {

    private String id;

    private double lat;

    private double lon;

    private double time;

    private double order;

    public Event(String id, double lat, double lon, double time, double order) {
        this.id = id;
        this.lat = lat;
        this.lon = lon;
        this.time = time;
        this.order = order;
    }

    public Event() {
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
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

    public double getTime() {
        return time;
    }

    public void setTime(double time) {
        this.time = time;
    }

    public double getOrder() {
        return order;
    }

    public void setOrder(double order) {
        this.order = order;
    }
}
