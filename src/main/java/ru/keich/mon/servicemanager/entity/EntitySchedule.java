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
import java.util.concurrent.TimeUnit;

import javax.net.ssl.SSLException;

import org.springframework.http.MediaType;
import org.springframework.http.client.reactive.ReactorClientHttpConnector;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.web.reactive.function.client.ExchangeStrategies;
import org.springframework.web.reactive.function.client.WebClient;

import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import lombok.extern.java.Log;
import reactor.core.publisher.Mono;
import reactor.netty.http.client.HttpClient;

@Log
public class EntitySchedule<K, T extends Entity<K>> {

	private final String nodeName;
	private final String replicationNeighborHost;
	private final Integer replicationNeighborPort;
	private final String path;
	private final Class<T> elementClass;
	
	private final EntityService<K, T> entityService;
	private final WebClient webClient;
	
	private volatile boolean isReplicationShdulerActive = false;
	private Long maxVersion = 0L;
	private Long minVersion = 0L;
	private boolean hasEntity = false;
	private boolean init = true;
	
	public EntitySchedule(EntityService<K, T> entityService, String nodeName, String replicationNeighborHost,
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
	
	//TODO to params
	@Scheduled(fixedRate = 5, timeUnit = TimeUnit.SECONDS)
	public void ReplicationScheduled() {
		if ("none".equals(nodeName)) {
			return;
		}
		if ("none".equals(replicationNeighborHost)) {
			return;
		}
		if(isReplicationShdulerActive) {
			//log.info("Entity " + path + " replication still active");
			return;
		}
		//First load
		final String tmpNodeName;
		if(init) {
			tmpNodeName = "";
		} else {
			tmpNodeName = nodeName;
		}

		minVersion = Long.MAX_VALUE;
		
		webClient.get().uri(uriBuilder -> uriBuilder.scheme("https").host(replicationNeighborHost).port(replicationNeighborPort).path(path)
		//.queryParam("version", replicationLastVersion).queryParam("ignoreNodeName", tmpNodeName).build())
		.queryParam("version", "gt:" + maxVersion).queryParam("fromHistory", "nc:" + tmpNodeName).build())
		.accept(MediaType.APPLICATION_JSON).retrieve()
		.bodyToFlux(elementClass)
		.onErrorResume(e -> {
			log.warning("Entity " + path + " replication client error: "+e.toString());
			return Mono.empty();
		})
		.doFinally(s -> {
			isReplicationShdulerActive = false;
			init = false;
			if(hasEntity) {
				log.info("Entity " + path + " replication is completed. Version:" + " min=" + minVersion 
						+ " max=" + maxVersion + " diff=" + (maxVersion - minVersion));
			}
		})
		.doFirst(() -> {
			isReplicationShdulerActive = true;
			hasEntity = false;
		})
		.doOnNext(entity -> {//TODO sort by version?
			if(minVersion > entity.getVersion()) {
				minVersion = entity.getVersion();
			}
			if(maxVersion < entity.getVersion()) {
				hasEntity = true;
				maxVersion = entity.getVersion();
			}
			if(Objects.isNull(entity.getDeletedOn())) {
				entityService.addOrUpdate(entity);
			} else {
				entityService.deleteById(entity.getId());
			}
		})
		.doOnError(e -> {
            log.info("Entity " + path + " replication error. Message: " + e.getMessage());
        })
		.subscribe();
	}
}
