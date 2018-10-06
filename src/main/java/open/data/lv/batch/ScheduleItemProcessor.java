package open.data.lv.batch;

import open.data.lv.model.Schedule;
import org.springframework.batch.item.ItemProcessor;

public class ScheduleItemProcessor implements ItemProcessor<Schedule, Schedule> {

    @Override
    public Schedule process(Schedule item) throws Exception {
        return item;
    }
}
