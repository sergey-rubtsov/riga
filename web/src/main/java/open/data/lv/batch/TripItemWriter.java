package open.data.lv.batch;

import open.data.lv.model.Validation;
import open.data.lv.repository.TripRepository;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionListener;
import org.springframework.batch.item.ItemWriter;
import org.springframework.beans.factory.annotation.Autowired;

import javax.transaction.Transactional;
import java.util.List;

public class TripItemWriter implements ItemWriter<Validation>, JobExecutionListener {

    @Autowired
    private TripRepository tripRepository;

    @Override
    @Transactional
    public void write(List<? extends Validation> items) throws Exception {
        tripRepository.save(items);
    }

    @Override
    public void beforeJob(JobExecution jobExecution) {

    }

    @Override
    public void afterJob(JobExecution jobExecution) {

    }
}
