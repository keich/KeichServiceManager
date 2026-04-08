package ru.keich.mon.servicemanager;

import org.springframework.boot.resttestclient.autoconfigure.AutoConfigureRestTestClient;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.test.web.servlet.client.RestTestClient;

@Configuration
@AutoConfigureRestTestClient
public class TestConfig {

    @Bean
    public ApiWrapper apiWrapper(RestTestClient restTestClient) {
        return new ApiWrapper(restTestClient);
    }

}
