package ru.keich.mon.servicemanager.replication;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import javax.net.ssl.SSLException;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.http.client.reactive.ReactorClientHttpConnector;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.ExchangeStrategies;
import org.springframework.web.reactive.function.client.WebClient;

import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import lombok.extern.java.Log;
import reactor.netty.http.client.HttpClient;
import ru.keich.mon.servicemanager.event.EventReplication;
import ru.keich.mon.servicemanager.event.EventService;
import ru.keich.mon.servicemanager.item.ItemReplication;
import ru.keich.mon.servicemanager.item.ItemService;

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

@Service
@Log
@ConditionalOnProperty(name = "replication.neighbor")
public class ReplicationService {
	
	final private List<Runnable> tasks;

	public ReplicationService(EventService eventService, ItemService itemService,
			@Value("${replication.nodename}") String nodeName,
			@Value("${replication.neighbor}#{T(java.util.Collections).emptyList()}") List<String> replicationNeighbor)
			throws SSLException, IllegalArgumentException, IllegalAccessException, NoSuchFieldException {
		tasks = new ArrayList<Runnable>();
		for (var neighbor : replicationNeighbor) {
			log.info("Enable replication from " + neighbor);
			var webClient = getWebClient(neighbor);
			var eventReplication = new EventReplication(webClient, nodeName, eventService::addOrUpdate);
			var itemReplication = new ItemReplication(webClient, nodeName, itemService::addOrUpdate);
			tasks.add(() -> itemReplication.doReplication(() -> eventReplication.doReplication()));
		}
	}

	private WebClient getWebClient(String url) throws SSLException {
		final ExchangeStrategies strategies = ExchangeStrategies.builder()
				.codecs(codecs -> codecs.defaultCodecs().maxInMemorySize(2621440)).build();
		var sslContext = SslContextBuilder.forClient().trustManager(InsecureTrustManagerFactory.INSTANCE).build();
		var httpClient = HttpClient.create().secure(t -> t.sslContext(sslContext));
		return WebClient
				.builder().baseUrl(url)
				.clientConnector(new ReactorClientHttpConnector(httpClient))
				.exchangeStrategies(strategies).build();
	}
	
	//TODO to params
	@Scheduled(fixedRate = 5, timeUnit = TimeUnit.SECONDS)
	public void replicationScheduled() {
		for (var task : tasks) {
			task.run();
		}
	}

}
