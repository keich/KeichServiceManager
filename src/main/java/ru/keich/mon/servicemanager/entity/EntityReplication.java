package ru.keich.mon.servicemanager.entity;

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

import java.util.Objects;
import javax.net.ssl.SSLException;

import org.springframework.http.MediaType;
import org.springframework.http.client.reactive.ReactorClientHttpConnector;
import org.springframework.web.reactive.function.client.ExchangeStrategies;
import org.springframework.web.reactive.function.client.WebClient;

import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import lombok.extern.java.Log;
import reactor.core.publisher.Mono;
import reactor.netty.http.client.HttpClient;

@Log
public class EntityReplication<K, T extends Entity<K>> {

	private final String nodeName;
	private final String replicationNeighborHost;
	private final Integer replicationNeighborPort;
	private final String path;
	private final Class<T> elementClass;
	
	private final EntityService<K, T> entityService;
	private final WebClient webClient;
	
	private volatile boolean active = false;
	private volatile boolean first = true;
	private Long maxVersion = 0L;
	private Long minVersion = Long.MAX_VALUE;
	private Long added = 0L;
	private Long deleted = 0L;
	private boolean hasEntity = false;

	
	public EntityReplication(EntityService<K, T> entityService, String nodeName, String replicationNeighborHost,
			Integer replicationNeighborPort, String path, Class<T> elementClass) throws SSLException {
		this.entityService = entityService;
		this.nodeName = nodeName;
		this.replicationNeighborHost = replicationNeighborHost;
		this.replicationNeighborPort = replicationNeighborPort;
		this.path = path;
		this.elementClass = elementClass;
		final ExchangeStrategies strategies = ExchangeStrategies.builder()
				.codecs(codecs -> codecs.defaultCodecs().maxInMemorySize(2621440)).build();
		var sslContext = SslContextBuilder.forClient().trustManager(InsecureTrustManagerFactory.INSTANCE).build();
		var httpClient = HttpClient.create().secure(t -> t.sslContext(sslContext));
		webClient = WebClient.builder().clientConnector(new ReactorClientHttpConnector(httpClient))
				.exchangeStrategies(strategies).build();
	}
	
	public void doReplication() {
		doReplication(() -> {});
	}
	
	public void doReplication(Runnable onFinally) {
		if ("none".equals(nodeName)) {
			return;
		}
		
		if ("none".equals(replicationNeighborHost)) {
			return;
		}
		
		if(active) {
			log.info("Entity " + path + " replication still active. Version:" + " min=" + minVersion 
					+ " max=" + maxVersion + " Entity: added=" + added + " deleted=" + deleted);
			return;
		}
		
		//First load
		final String tmpNodeName;
		final Long fromVersion;
		if(first) {
			tmpNodeName = "";
			fromVersion = 0L;
		} else {
			tmpNodeName = nodeName;
			fromVersion = maxVersion + 1L;
		}

		minVersion = Long.MAX_VALUE;
		added = 0L;
		deleted = 0L;
		
		webClient.get().uri(uriBuilder -> uriBuilder.scheme("https").host(replicationNeighborHost).port(replicationNeighborPort).path(path)
		.queryParam("version", "gt:" + fromVersion).queryParam("fromHistory", "nc:" + tmpNodeName).build())
		.accept(MediaType.APPLICATION_JSON).retrieve()
		.bodyToFlux(elementClass)
		.onErrorResume(e -> {
			log.warning("Entity " + path + " replication client error: "+e.toString());
			return Mono.empty();
		})
		.doFirst(() -> {
			active = true;
			hasEntity = false;
		})
		.doFinally(s -> {
			active = false;
			first = false;
			if(hasEntity) {
				log.info("Entity " + path + " replication is completed. Version:" + " min=" + minVersion 
						+ " max=" + maxVersion + " Entity: added=" + added + " deleted=" + deleted);
			}
			onFinally.run();
		})
		.doOnNext(entity -> {
			hasEntity = true;
			final var version  = entity.getVersion();
			if(minVersion > version) {
				minVersion = version;
			}
			if(maxVersion < version) {
				maxVersion = version;
			}
			if(Objects.nonNull(entity.getDeletedOn())) {
				deleted++;
			}
			added++;
			entityService.addOrUpdate(entity);
		})
		.doOnError(e -> {
            log.info("Entity " + path + " replication error. Message: " + e.getMessage());
        })
		.subscribe();
	}
}
