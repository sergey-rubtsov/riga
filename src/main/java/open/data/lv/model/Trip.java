package open.data.lv.model;

import javax.persistence.*;
import java.util.Date;

@Entity
public class Trip {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @ManyToOne(fetch = FetchType.EAGER)
    @JoinColumn(name = "transport_id")
    private Transport transport;

    @ManyToOne(fetch = FetchType.EAGER)
    @JoinColumn(name = "person_id")
    private Person person;

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

    public Trip(Transport transport, Person person, Date start) {
        this.transport = transport;
        this.person = person;
        this.start = start;
    }

    public Trip(Transport transport, Person person, Date start, double x0, double y0, Date end, double x1, double y1) {
        this.transport = transport;
        this.person = person;
        this.start = start;
        this.x0 = x0;
        this.y0 = y0;
        this.end = end;
        this.x1 = x1;
        this.y1 = y1;
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public Transport getTransport() {
        return transport;
    }

    public void setTransport(Transport transport) {
        this.transport = transport;
    }

    public Person getPerson() {
        return person;
    }

    public void setPerson(Person person) {
        this.person = person;
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
}
