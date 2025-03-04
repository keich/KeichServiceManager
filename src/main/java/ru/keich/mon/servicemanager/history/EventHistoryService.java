package ru.keich.mon.servicemanager.history;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.net.ssl.SSLException;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestClientBuilder;
import org.opensearch.client.json.jackson.JacksonJsonpMapper;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.client.opensearch._types.FieldValue;
import org.opensearch.client.opensearch._types.OpenSearchException;
import org.opensearch.client.opensearch._types.query_dsl.TermsQueryField;
import org.opensearch.client.opensearch.core.BulkRequest;
import org.opensearch.client.transport.OpenSearchTransport;
import org.opensearch.client.transport.rest_client.RestClientTransport;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.databind.ObjectMapper;

import lombok.extern.java.Log;
import ru.keich.mon.servicemanager.event.Event;

/*
 * Copyright 2025 the original author or authors.
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
public class EventHistoryService {
	public static final String HISTORY = "History: ";
	ObjectMapper objectMapper;
	private final String osUrl;
	private final String osIndexName;
	private final OpenSearchClient openSearchClient;
	public static final Integer groupLimit = 1000;
	public static final Integer openSearchLimit = 10000;
	private Lock lock = new ReentrantLock();
	private Long maxVersion = 0L;

	public EventHistoryService(ObjectMapper objectMapper,
			@Value("${opensearch.url:none}") String osurl,
			@Value("${opensearch.user:none}") String osuser,
			@Value("${opensearch.password:none}") String ospassword,
			@Value("${opensearch.eventindexname:ksm-events}") String osIndexName) throws SSLException {
		this.objectMapper = objectMapper;
		this.osUrl = osurl;
		this.osIndexName = osIndexName;
		
	    final HttpHost host = HttpHost.create(osurl);
	    final BasicCredentialsProvider credentialsProvider = new BasicCredentialsProvider();
	    credentialsProvider.setCredentials(new AuthScope(host), new UsernamePasswordCredentials(osuser, ospassword));

		final RestClient restClient = RestClient.builder(host)
				.setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
					@Override
					public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
						return httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
					}
				}).build();

	    final OpenSearchTransport transport = new RestClientTransport(restClient, new JacksonJsonpMapper(objectMapper));
	    openSearchClient = new OpenSearchClient(transport);
	}


	private void sendEvents(List<Event> events) throws OpenSearchException, IOException {
		if (events.size() == 0) {
			return;
		}
		BulkRequest.Builder br = new BulkRequest.Builder();
		events.forEach(event -> {
			br.operations(op -> {
				return op.create(idx -> {
					return idx.index(osIndexName).document(event);
				});
			});
		});
		var result = openSearchClient.bulk(br.build());
		
		if(result.errors()) {
			log.info(HISTORY + " OpenSearch bulk operation has errors");
		}
		
	}
	
	private Map<String, Event> getEventsByIdsInternal(List<String> eventIds) {
		final String fieldName = "id";
		try {
			TermsQueryField Idsterms = new TermsQueryField.Builder()
				    .value(eventIds.stream().map(FieldValue::of).toList())
				    .build();
			return openSearchClient.search(s -> s.size(openSearchLimit).index(osIndexName)
					.query(q -> 
						q.terms(t -> 
							t.field(fieldName).terms(Idsterms)
						)
					),
					Event.class)
					.hits()
					.hits()
					.stream()
					.map(h -> h.source())
					.collect(Collectors.toMap(Event::getId, Function.identity(), (e1, e2) -> {
						if (Objects.nonNull(e1.getDeletedOn())) {
							return e1;
						}
						return e2;
					}));

		} catch (IOException | OpenSearchException e) {
			log.warning(HISTORY + " getEventsByIdsInternal exception: " + e.getMessage());
		}
		return Collections.emptyMap();
	}
	
	public Map<String, Event> getEventsByIds(List<String> eventIds) {
		if(!isEnable()) {
			return Collections.emptyMap();
		}
		if(eventIds.size() == 0) {
			return Collections.emptyMap();
		}
		AtomicInteger index = new AtomicInteger(0);
		return eventIds.stream()
				.collect(Collectors.groupingBy(s -> index.getAndIncrement() / groupLimit))
				.values().stream()
				.map(l -> getEventsByIdsInternal(l))
				.flatMap(m -> m.entrySet().stream())
				.collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (v1, v2) -> v1));
	}

	public void sendHistory(Function<Long, List<Event>> fun) {
		if(!isEnable()) {
			return;
		}
		
		if (lock.tryLock()) {
			try {
				sendHistoryInternal(fun);
			} finally {
				lock.unlock();
			}
		} else {
			log.info(HISTORY + " Still active. State [ maxVersion: " + maxVersion + " ]");
		}
	}
	
	private void sendHistoryInternal(Function<Long, List<Event>> fun) {
		var inputEvents = fun.apply(maxVersion);

		if (inputEvents.size() == 0) {
			log.info(HISTORY + " Event list is empty. State [ maxVersion: " + maxVersion + " ]");
			return;
		}
		
		maxVersion = inputEvents.stream().mapToLong(Event::getVersion).max().orElse(0);
		AtomicInteger index = new AtomicInteger(0);
		var existsEvents = inputEvents.stream()
				.map(Event::getId)
				.collect(Collectors.groupingBy(s -> index.getAndIncrement() / groupLimit))
				.values().stream()
				.map(l -> getEventsByIds(l))
				.flatMap(m -> m.entrySet().stream())
				.collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (v1, v2) -> v1));
		
		var filteredInputEvents = inputEvents.stream()
				.filter(event -> {
					var oldEvent = existsEvents.get(event.getId());
					if(Objects.isNull(oldEvent)){
						return true;
					}
					if(Objects.nonNull(oldEvent.getDeletedOn())) {
						return false;
					}
					if(Objects.nonNull(event.getDeletedOn())) {
						return true;
					}
					return false;
				})
				.toList();
		
		try {
			sendEvents(filteredInputEvents);
		} catch (IOException | OpenSearchException e) {
			log.warning(HISTORY + " sendHistoryInternal exception: " + e.getMessage());
		}

		log.info(HISTORY + " Is completed. State [ maxVersion: " + maxVersion + " Input eventd: " + inputEvents.size() 
				+ " ExistsEvents: " +  existsEvents.size() 
				+ " Documents: " + filteredInputEvents.size()
				+ " ] ");
		
	}

	public boolean isEnable() {
		return !"none".equals(osUrl);
	}
	
}
