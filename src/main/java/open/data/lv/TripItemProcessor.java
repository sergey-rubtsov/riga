package open.data.lv;

import open.data.lv.model.Trip;
import org.springframework.batch.item.ItemProcessor;

public class TripItemProcessor implements ItemProcessor<Trip, Trip> {

    @Override
    public Trip process(Trip item) throws Exception {
        return item;
    }

}
