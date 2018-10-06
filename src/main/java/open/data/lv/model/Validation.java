package open.data.lv.model;

import javax.persistence.*;
import java.util.Date;

@Entity
public class Validation {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column
    private String transportUnit;

    @Column
    private String route;

    @Column
    private String ticket;

    @Column
    private Date timestamp;

    public Validation() {
    }

    public Validation(String transportUnit, String route, String ticket, Date timestamp) {
        this.transportUnit = transportUnit;
        this.route = route;
        this.ticket = ticket;
        this.timestamp = timestamp;
    }

    public String getTransportUnit() {
        return transportUnit;
    }

    public void setTransportUnit(String transportUnit) {
        this.transportUnit = transportUnit;
    }

    public String getRoute() {
        return route;
    }

    public void setRoute(String route) {
        this.route = route;
    }

    public String getTicket() {
        return ticket;
    }

    public void setTicket(String ticket) {
        this.ticket = ticket;
    }

    public Date getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Date timestamp) {
        this.timestamp = timestamp;
    }
}
