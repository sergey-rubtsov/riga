package open.data.lv.controller;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecutionException;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Scope;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping(value = "/api")
@Scope(value = "request")
public class BatchController {

    private static final Logger LOGGER = LoggerFactory.getLogger(BatchController.class);

    @Autowired
    private JobLauncher jobLauncher;

    @Autowired
    @Qualifier("schedule")
    private Job schedule;

    @Autowired
    @Qualifier("trip")
    private Job trip;

    @RequestMapping("/trip")
    public String trip() {
        try {
            JobParameters jobParameters = new JobParametersBuilder().addLong("time", System.currentTimeMillis())
                    .toJobParameters();
            jobLauncher.run(trip, jobParameters);
        } catch (JobExecutionException e) {
            LOGGER.error("job completed with error", e);
        }
        return "Validation Done";
    }

    @RequestMapping("/schedule")
    public String schedule() {
        try {
            JobParameters jobParameters = new JobParametersBuilder().addLong("time", System.currentTimeMillis())
                    .toJobParameters();
            jobLauncher.run(schedule, jobParameters);
        } catch (JobExecutionException e) {
            LOGGER.error("job completed with error", e);
        }
        return "Schedule Done";
    }
}
