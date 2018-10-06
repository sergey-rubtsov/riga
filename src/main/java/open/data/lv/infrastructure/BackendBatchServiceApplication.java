package open.data.lv.infrastructure;

import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Configuration;
import org.springframework.format.FormatterRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurerAdapter;

@SpringBootApplication
@EnableBatchProcessing
@Configuration
public class BackendBatchServiceApplication extends WebMvcConfigurerAdapter {

    @Override
    public void addFormatters(FormatterRegistry registry) {
        registry.addConverter(new LocalTimeConverter());
        registry.addConverter(new LocalDateTimeConverter());
    }

    public static void main(String[] args) {
        SpringApplication.run(BackendBatchServiceApplication.class, args);
    }
}
