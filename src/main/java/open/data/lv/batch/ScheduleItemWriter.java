package open.data.lv.batch;

import open.data.lv.model.Schedule;
import open.data.lv.repository.ScheduleRepository;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionListener;
import org.springframework.batch.item.ItemWriter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;

//@Component
public class ScheduleItemWriter implements ItemWriter<Schedule>, JobExecutionListener {

    @Autowired
    private ScheduleRepository scheduleRepository;

    @Override
    public void beforeJob(JobExecution jobExecution) {
    }

    @Override
    public void afterJob(JobExecution jobExecution) {
    }

    @Override
    public void write(List<? extends Schedule> items) throws Exception {
        scheduleRepository.save(items);
    }
}
