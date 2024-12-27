package ru.keich.mon.servicemanager.entity;

import java.net.URI;

import javax.net.ssl.SSLException;

import org.springframework.http.MediaType;
import org.springframework.http.client.reactive.ReactorClientHttpConnector;
import org.springframework.web.reactive.function.client.ExchangeStrategies;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.util.UriBuilder;

import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import lombok.extern.java.Log;
import reactor.core.publisher.Flux;
import reactor.netty.http.client.HttpClient;
import ru.keich.mon.servicemanager.AddResponseHeaderFilter;

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

@Log
public class EntityReplication<K, T extends Entity<K>> {

	private final String nodeName;
	private final String replicationNeighbor;
	private final String path;
	private final Class<T> elementClass;
	
	private final EntityService<K, T> entityService;
	private final WebClient webClient;
	
	private final EntityReplicationState state = new EntityReplicationState();
	
	public EntityReplication(EntityService<K, T> entityService, String nodeName, String replicationNeighbor, String path, Class<T> elementClass) throws SSLException {
		this.entityService = entityService;
		this.nodeName = nodeName;
		this.replicationNeighbor = replicationNeighbor;
		this.path = path;
		this.elementClass = elementClass;
		final ExchangeStrategies strategies = ExchangeStrategies.builder()
				.codecs(codecs -> codecs.defaultCodecs().maxInMemorySize(2621440)).build();
		var sslContext = SslContextBuilder.forClient().trustManager(InsecureTrustManagerFactory.INSTANCE).build();
		var httpClient = HttpClient.create().secure(t -> t.sslContext(sslContext));
		webClient = WebClient
				.builder().baseUrl(replicationNeighbor + path)
				.clientConnector(new ReactorClientHttpConnector(httpClient))
				.exchangeStrategies(strategies).build();
	}
	
	public URI getUri(UriBuilder uriBuilder) {
		if(state.isFirstRun()) {
			return uriBuilder.queryParam("fromHistory", "ni:" + nodeName).build();
		}
		return uriBuilder.queryParam("version", "gt:" + state.getMaxVersion())
		.queryParam("fromHistory", "ni:" + nodeName).build();
	}
	
	public void doReplication() {
		doReplication(() -> {});
	}
	
	public void doReplication(Runnable onFinally) {
		if ("none".equals(replicationNeighbor)) {
			return;
		}
		
		if(state.isActive()) {
			log.info("Entity " + path + ". Replication still active. State [ " + state.toString() + " ]");
			return;
		}

		state.reset();

		webClient.get()
				.uri(this::getUri)
				.accept(MediaType.APPLICATION_JSON)
				.exchangeToFlux(response -> {
					var startTime = response.headers().header(AddResponseHeaderFilter.HEADER_START_TIME).stream()
							.findFirst().orElse("");
					if (state.isFirstRun()) {
						state.setNeighborStartTime(startTime);
					} else {
						if (!state.getNeighborStartTime().equals(startTime)) {
							var exception = new ChangedNeighborStartTimeException(
									"NeighborStartTime is changed from " + state.getNeighborStartTime() + " to " + startTime);
							state.setFirstRunTrue();
							return Flux.error(exception);
						}
					}
					return response.bodyToFlux(elementClass);
				})
				.doFirst(() -> {
					state.setActiveTrue();
				})
				.doOnComplete(() -> {
					state.setFirstRunFalse();
					log.info("Entity " + path + ". Replication is completed. State [ " + state.toString() + " ]");
				})
				.doFinally(s -> {
					state.setActiveFalse();
					onFinally.run();
				})
				.doOnNext(entity -> {
					state.updateVersion(entity.getVersion());
					state.incrementCounters(entity.getDeletedOn());
					entityService.addOrUpdate(entity);
				})
				.subscribe();
	}
}
