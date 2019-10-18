package open.data.lv.spark.kd;

import open.data.lv.spark.Stop;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class KDTreeStopClassifierTest {

    @Test
    public void findNearestNeighbourId() {
        List<Stop> stops = new ArrayList<>();
        Stop first = new Stop("first", 0.1, 0.1, "A 1", 1);
        Stop second = new Stop("second", 10, 10, "A 1", 1);
        Stop third = new Stop("third", 0.1, 10, "A 1", 1);
        Stop fourth = new Stop("fourth", 0.1, 6.1, "A 2", 1);
        stops.add(first);
        stops.add(second);
        stops.add(third);
        stops.add(fourth);
        KDTreeStopClassifier classifier = new KDTreeStopClassifier(stops);
        String id = classifier.findNearestNeighbourId("A 1", 0.1, 6.0);
        assertEquals("third", id);
    }
}
