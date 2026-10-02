package ru.keich.mon.servicemanager.entity;

import java.net.URI;
import java.util.function.Consumer;
import java.util.logging.Logger;

import org.springframework.http.MediaType;
import org.springframework.web.reactive.function.client.ClientResponse;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.util.UriBuilder;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
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

public class EntityReplication<T extends Entity> {

	private final String nodeName;
	private final String path;
	private final Class<T> elementClass;
	private final Consumer<T> consumer;

	private final WebClient webClient;
	private final Logger log;

	private final EntityReplicationState state = new EntityReplicationState();
	
	public EntityReplication(WebClient webClient, String nodeName, String path, Class<T> elementClass, Consumer<T> consumer, Logger log) throws IllegalArgumentException, IllegalAccessException, NoSuchFieldException {
		this.webClient = webClient;
		this.nodeName = nodeName;
		this.path = path;
		this.elementClass = elementClass;
		this.consumer = consumer;
		this.log = log;	
	}

	private URI getUri(UriBuilder uriBuilder) {
		uriBuilder.path(path);
		if(state.isFirstRun()) {
			return uriBuilder.queryParam(Entity.FIELD_VERSION, "gt:0").build();
		}
		return uriBuilder.queryParam(Entity.FIELD_VERSION, "gt:" + state.getMaxVersion())
		.queryParam(Entity.FIELD_FROMHISTORY, "ni:" + nodeName).build();
	}

	public void doReplication() {
		doReplication(() -> {});
	}

	private Mono<EntitySchema> getEntitySchema(ClientResponse response) {
		var startTime = response.headers().header(AddResponseHeaderFilter.HEADER_START_TIME).stream()
				.findFirst().orElse("");
		if (state.isFirstRun()) {
			state.setNeighborStartTime(startTime);
		} else {
			if (!state.getNeighborStartTime().equals(startTime)) {
				var exception = new ChangedNeighborStartTimeException(
						"NeighborStartTime is changed from " + state.getNeighborStartTime() + " to " + startTime);
				state.setFirstRunTrue();
				return Mono.error(exception);
			}
		}
		return response.bodyToMono(EntitySchema.class);
	}

	private Flux<T> getEntities(ClientResponse response) {
		var startTime = response.headers().header(AddResponseHeaderFilter.HEADER_START_TIME).stream()
				.findFirst().orElse("");
		if (!state.getNeighborStartTime().equals(startTime)) {
			var exception = new ChangedNeighborStartTimeException(
					"NeighborStartTime is changed from " + state.getNeighborStartTime() + " to " + startTime);
			state.setFirstRunTrue();
			return Flux.error(exception);
		}
		return response.bodyToFlux(elementClass);
	}

	public void doReplication(Runnable onFinally) {	
		if (state.isActive()) {
			log.info("Aactive.   State [ " + state.toString() + " ]");
			return;
		}

		state.reset();

		webClient.get()
				.uri(b -> b.path(path + "Schema").build())
				.accept(MediaType.APPLICATION_JSON)
				.exchangeToMono(this::getEntitySchema)
				.flatMapMany(schema -> {
					return webClient.get()
							.uri(b -> {
								var url = getUri(b);
								state.setMaxVersion(schema.getMaxVersion());
								return url;
							})
							.accept(MediaType.APPLICATION_JSON)
							.exchangeToFlux(this::getEntities);
				})
				.doFirst(() -> {
					state.setActiveTrue();
					log.info("Start.     State [ " + state.toString() + " ]");
				})
				.doOnComplete(() -> {
					state.setFirstRunFalse();
					log.info("Completed. State [ " + state.toString() + " ]");
				})
				.doFinally(s -> {
					state.setActiveFalse();
					onFinally.run();
				})
				.doOnNext(entity -> {
					state.incrementCounters(entity.getDeletedOn());
					consumer.accept(entity);
				})
				.subscribe();
	}

}
