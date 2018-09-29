package open.data.lv;

import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
@EnableBatchProcessing
public class BackendBatchServiceApplication {

    public static void main(String[] args) {
        SpringApplication.run(BackendBatchServiceApplication.class, args);
    }
}
