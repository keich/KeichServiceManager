package ru.keich.mon.servicemanager;

import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class TestConfig {

    @Bean
    public ApiWrapper apiWrapper(TestRestTemplate restTemplate) {
        return new ApiWrapper(restTemplate);
    }

}
