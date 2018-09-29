package open.data.lv;

import org.springframework.boot.autoconfigure.jdbc.DataSourceBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.security.config.annotation.authentication.builders.AuthenticationManagerBuilder;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.config.annotation.web.configuration.WebSecurityConfigurerAdapter;

import javax.sql.DataSource;
import java.net.URISyntaxException;

@Configuration
@ComponentScan
public class DBConfig {

    @Bean
    @Primary
    public DataSource dataSource() throws URISyntaxException {
        return DataSourceBuilder.create().url("jdbc:h2:file:~/riga;DB_CLOSE_DELAY=500;DB_CLOSE_ON_EXIT=FALSE")
                .username("sa").password("").driverClassName("org.h2.Driver").build();
    }


}
