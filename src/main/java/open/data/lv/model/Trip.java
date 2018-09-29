package open.data.lv.model;

import javax.persistence.*;
import java.util.Date;
import java.util.Objects;

@Entity
public class Trip {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column
    private String uuid;

    @Column
    private String route;

    @Column
    private String ticket;

    @Column
    private Date start;

    @Column
    private double x0;

    @Column
    private double y0;

    @Column
    private Date end;

    @Column
    private double x1;

    @Column
    private double y1;

    public Trip() {
    }

    public Trip(String uuid, String route, String ticket, Date start) {
        this.uuid = uuid;
        this.route = route;
        this.ticket = ticket;
        this.start = start;
    }

    public String getUuid() {
        return uuid;
    }

    public void setUuid(String uuid) {
        this.uuid = uuid;
    }

    public String getRoute() {
        return route;
    }

    public void setRoute(String route) {
        this.route = route;
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getTicket() {
        return ticket;
    }

    public void setTicket(String ticket) {
        this.ticket = ticket;
    }

    public double getX0() {
        return x0;
    }

    public void setX0(double x0) {
        this.x0 = x0;
    }

    public double getY0() {
        return y0;
    }

    public void setY0(double y0) {
        this.y0 = y0;
    }

    public double getX1() {
        return x1;
    }

    public void setX1(double x1) {
        this.x1 = x1;
    }

    public double getY1() {
        return y1;
    }

    public void setY1(double y1) {
        this.y1 = y1;
    }

    public Date getStart() {
        return start;
    }

    public void setStart(Date start) {
        this.start = start;
    }

    public Date getEnd() {
        return end;
    }

    public void setEnd(Date end) {
        this.end = end;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Trip trip = (Trip) o;
        return Double.compare(trip.x0, x0) == 0 &&
                Double.compare(trip.y0, y0) == 0 &&
                Double.compare(trip.x1, x1) == 0 &&
                Double.compare(trip.y1, y1) == 0 &&
                Objects.equals(id, trip.id) &&
                Objects.equals(uuid, trip.uuid) &&
                Objects.equals(route, trip.route) &&
                Objects.equals(ticket, trip.ticket) &&
                Objects.equals(start, trip.start) &&
                Objects.equals(end, trip.end);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, uuid, route, ticket, start, x0, y0, end, x1, y1);
    }
}
