package open.data.lv.spark;


import java.io.Serializable;

public class Stop implements Serializable {

    private String id;

    private float lat;

    private float lon;

    public Stop() {
    }

    public Stop(String id, float lat, float lon) {
        this.id = id;
        this.lat = lat;
        this.lon = lon;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public float getLat() {
        return lat;
    }

    public void setLat(float lat) {
        this.lat = lat;
    }

    public float getLon() {
        return lon;
    }

    public void setLon(float lon) {
        this.lon = lon;
    }

}
