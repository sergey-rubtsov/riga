package open.data.lv.batch;

import open.data.lv.model.Schedule;
import open.data.lv.model.Validation;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.JobRegistry;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.explore.JobExplorer;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.launch.JobOperator;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.launch.support.SimpleJobOperator;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.LineMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.mapping.FieldSetMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;

@Configuration
@ComponentScan
public class BatchConfig {

    private static final int CHUNK_SIZE = 5_000;

    @Autowired
    private JobBuilderFactory jobBuilderFactory;

    @Autowired
    private StepBuilderFactory stepBuilderFactory;

    @Autowired
    private JobRepository jobRepository;

    @Autowired
    private JobRegistry jobRegistry;

    @Autowired
    private JobLauncher jobLauncher;

    @Autowired
    private JobExplorer jobExplorer;

    private final String fileTrip;

    private final String fileSchedule;

    private final Boolean strict;

    private final String charset;

    public BatchConfig(@Value("${batch.file.trip}") String fileTrip,
                       @Value("${batch.file.schedule}") String fileSchedule,
                       @Value("${strict.line.size}") Boolean strict,
                       @Value("${batch.charset}") String charset) {
        this.fileTrip = fileTrip;
        this.fileSchedule = fileSchedule;
        this.strict = strict;
        this.charset = charset;
    }

    @Bean
    public JobOperator jobOperator() {
        SimpleJobOperator jobOperator = new SimpleJobOperator();
        jobOperator.setJobExplorer(jobExplorer);
        jobOperator.setJobLauncher(jobLauncher);
        jobOperator.setJobRegistry(jobRegistry);
        jobOperator.setJobRepository(jobRepository);
        return jobOperator;
    }

    @Bean("schedule")
    public Job schedule(@Qualifier("scheduleStep") Step scheduleStep) {
        return jobBuilderFactory.get("schedule")
                .incrementer(new RunIdIncrementer())
                .flow(scheduleStep)
                .end()
                .build();
    }

    @Bean("trip")
    public Job trip(@Qualifier("tripStep") Step tripStep) {
        return jobBuilderFactory.get("trip")
                .incrementer(new RunIdIncrementer())
                .flow(tripStep)
                .end()
                .build();
    }

    @Bean
    @StepScope
    public ItemReader<Validation> tripItemReader() {
        FlatFileItemReader<Validation> itemReader = new FlatFileItemReader<>();
        itemReader.setEncoding(charset);
        itemReader.setLinesToSkip(1);
        itemReader.setResource(new ClassPathResource(fileTrip));
        itemReader.setLineMapper(tripLineMapper());
        itemReader.open(new ExecutionContext());
        return itemReader;
    }

    @Bean
    public ItemWriter<Validation> tripItemWriter() {
        return new TripItemWriter();
    }

    @Bean
    public LineMapper<Validation> tripLineMapper() {
        DefaultLineMapper<Validation> defaultLineMapper = new DefaultLineMapper<>();
        DelimitedLineTokenizer tokenizer = new DelimitedLineTokenizer();
        tokenizer.setStrict(strict);
        defaultLineMapper.setLineTokenizer(tokenizer);
        FieldSetMapper<Validation> fieldSetMapper = new TripFieldSetMapper();
        defaultLineMapper.setFieldSetMapper(fieldSetMapper);
        return defaultLineMapper;
    }

    @Bean
    public Step tripStep(@Qualifier("tripItemReader") ItemReader<Validation> reader,
                      @Qualifier("tripItemWriter") ItemWriter<Validation> writer) {
        return stepBuilderFactory.get("trip")
                .<Validation, Validation> chunk(CHUNK_SIZE)
                .reader(reader)
                .processor(new TripItemProcessor())
                .writer(writer)
                .build();
    }

    @Bean
    @StepScope
    public ItemReader<Schedule> scheduleItemReader() {
        FlatFileItemReader<Schedule> itemReader = new FlatFileItemReader<>();
        itemReader.setEncoding(charset);
        itemReader.setLinesToSkip(1);
        itemReader.setResource(new ClassPathResource(fileSchedule));
        itemReader.setLineMapper(scheduleLineMapper());
        itemReader.open(new ExecutionContext());
        return itemReader;
    }

    @Bean
    public ItemWriter<Schedule> scheduleItemWriter() {
        return new ScheduleItemWriter();
    }

    @Bean
    public LineMapper<Schedule> scheduleLineMapper() {
        DefaultLineMapper<Schedule> defaultLineMapper = new DefaultLineMapper<>();
        DelimitedLineTokenizer tokenizer = new DelimitedLineTokenizer();
        tokenizer.setStrict(strict);
        defaultLineMapper.setLineTokenizer(tokenizer);
        FieldSetMapper<Schedule> fieldSetMapper = new ScheduleFieldSetMapper();
        defaultLineMapper.setFieldSetMapper(fieldSetMapper);
        return defaultLineMapper;
    }

    @Bean
    public Step scheduleStep(@Qualifier("scheduleItemReader") ItemReader<Schedule> reader,
                        @Qualifier("scheduleItemWriter") ItemWriter<Schedule> writer) {
        return stepBuilderFactory.get("schedule")
                .<Schedule, Schedule> chunk(CHUNK_SIZE)
                .reader(reader)
                .processor(new ScheduleItemProcessor())
                .writer(writer)
                .build();
    }

}
