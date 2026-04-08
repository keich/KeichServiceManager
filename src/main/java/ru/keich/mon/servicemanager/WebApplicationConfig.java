package ru.keich.mon.servicemanager;

import org.springframework.boot.jackson.autoconfigure.JsonMapperBuilderCustomizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.web.SecurityFilterChain;
import org.springframework.web.servlet.config.annotation.ResourceHandlerRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

import lombok.extern.java.Log;
import tools.jackson.databind.DeserializationFeature;
import tools.jackson.databind.ser.std.SimpleFilterProvider;

/*
 * Copyright 2024 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

@Configuration
@Log
public class WebApplicationConfig implements WebMvcConfigurer {

	@Override
	public void addResourceHandlers(ResourceHandlerRegistry registry) {
        registry
        .addResourceHandler("/static/**")
        .addResourceLocations("classpath:/static/");	
	}
	
	@Bean
	public SecurityFilterChain filterChain(HttpSecurity http) throws Exception {
		http.authorizeHttpRequests(a -> a.requestMatchers(m -> {
			log.info("Client: " + m.getRemoteAddr() + ":"+ m.getRemotePort() + " Request: " + m.getRequestURI());
			return true;
		}).permitAll());
		http.csrf(csrf -> csrf.disable());
		return http.build();
	}

	@Bean
	JsonMapperBuilderCustomizer jacksonCustomizer() {
	    return builder -> builder
	    		.filterProvider(new SimpleFilterProvider().setFailOnUnknownId(false))
	    		.disable(DeserializationFeature.FAIL_ON_NULL_FOR_PRIMITIVES);
	}

}
