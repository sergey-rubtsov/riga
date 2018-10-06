package open.data.lv.message;

import java.util.Date;

public class TripMessage {

    private Date time;

    private PointMessage enter;

    private PointMessage exit;

    public TripMessage() {
    }

    public TripMessage(Date time, double enterX, double enterY, double exitX, double exitY) {
        this.time = time;
        this.enter = new PointMessage(enterX, enterY);
        this.exit = new PointMessage(exitX, exitY);
    }

    public Date getTime() {
        return time;
    }

    public void setTime(Date time) {
        this.time = time;
    }

    public PointMessage getEnter() {
        return enter;
    }

    public void setEnter(PointMessage enter) {
        this.enter = enter;
    }

    public PointMessage getExit() {
        return exit;
    }

    public void setExit(PointMessage exit) {
        this.exit = exit;
    }
}
