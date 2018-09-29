package open.data.lv.translator;

import open.data.lv.message.TripMessage;
import open.data.lv.model.Trip;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.stream.Collectors;

@Component
public class TripTranslator {

    public List<TripMessage> translate(List<Trip> trips) {
        return trips.stream().map(el ->
                new TripMessage(el.getStart(),
                        el.getX0(),
                        el.getY0(),
                        el.getX1(),
                        el.getY1()))
                .collect(Collectors.toList());
    }
}
