package open.data.lv.infrastructure;

import org.springframework.boot.autoconfigure.jdbc.DataSourceBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

import javax.sql.DataSource;
import java.net.URISyntaxException;

@Configuration
@ComponentScan
public class DBConfig {

    @Bean
    @Primary
    public DataSource dataSource() throws URISyntaxException {
        return DataSourceBuilder.create().url("jdbc:h2:file:~/riga2;DB_CLOSE_DELAY=500;DB_CLOSE_ON_EXIT=FALSE,MV_STORE=FALSE")
                .username("sa").password("").driverClassName("org.h2.Driver").build();
    }


}
