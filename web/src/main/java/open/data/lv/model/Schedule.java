package open.data.lv.model;

import javax.persistence.*;
import java.util.Date;

@Entity
public class Schedule {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column
    private Date arrival;

    @Column
    private Date departure;

    @Column
    private double x;

    @Column
    private double y;

    @Column
    private String route;

    public Schedule() {
    }

    public Schedule(Date arrival, Date departure, double x, double y, String route) {
        this.arrival = arrival;
        this.departure = departure;
        this.x = x;
        this.y = y;
        this.route = route;
    }

    public Date getArrival() {
        return arrival;
    }

    public void setArrival(Date arrival) {
        this.arrival = arrival;
    }

    public Date getDeparture() {
        return departure;
    }

    public void setDeparture(Date departure) {
        this.departure = departure;
    }

    public double getX() {
        return x;
    }

    public void setX(double x) {
        this.x = x;
    }

    public double getY() {
        return y;
    }

    public void setY(double y) {
        this.y = y;
    }

    public String getRoute() {
        return route;
    }

    public void setRoute(String route) {
        this.route = route;
    }
}
