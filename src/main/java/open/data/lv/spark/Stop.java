package open.data.lv.spark;

import java.io.Serializable;

public class Stop implements Serializable {

    private String id;

    private double lat;

    private double lon;

    private String route;

    private int dir;

    public Stop() {
    }

    public Stop(String id, double lat, double lon, String route, int dir) {
        this.id = id;
        this.lat = lat;
        this.lon = lon;
        this.route = route;
        this.dir = dir;
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

    public String getRoute() {
        return route;
    }

    public void setRoute(String route) {
        this.route = route;
    }

    public int getDir() {
        return dir;
    }

    public void setDir(int dir) {
        this.dir = dir;
    }
}
