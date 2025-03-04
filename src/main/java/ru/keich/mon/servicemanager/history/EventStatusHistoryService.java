package ru.keich.mon.servicemanager.history;

import java.io.IOException;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import javax.net.ssl.SSLException;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestClientBuilder;
import org.opensearch.client.json.JsonData;
import org.opensearch.client.json.jackson.JacksonJsonpMapper;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.client.opensearch._types.FieldValue;
import org.opensearch.client.opensearch._types.OpenSearchException;
import org.opensearch.client.opensearch._types.query_dsl.RangeQuery;
import org.opensearch.client.opensearch._types.query_dsl.TermsQuery;
import org.opensearch.client.opensearch._types.query_dsl.TermsQueryField;
import org.opensearch.client.opensearch.core.BulkRequest;
import org.opensearch.client.opensearch.core.BulkResponse;
import org.opensearch.client.transport.OpenSearchTransport;
import org.opensearch.client.transport.rest_client.RestClientTransport;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;

import lombok.extern.java.Log;
import ru.keich.mon.servicemanager.BaseStatus;
import ru.keich.mon.servicemanager.event.Event;
import ru.keich.mon.servicemanager.item.Item;

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
public class EventStatusHistoryService {
	public static final String HISTORY = "Status History: ";
	private final EventHistoryService eventHistoryService;
	ObjectMapper objectMapper;
	private final String osUrl;
	private final String osStatusIndexName;
	private final OpenSearchClient openSearchClient;
	public static final Integer groupLimit = 1000;
	public static final Integer openSearchLimit = 1000;
	private Lock lock = new ReentrantLock();

	public EventStatusHistoryService(ObjectMapper objectMapper,
			EventHistoryService eventHistoryService,
			@Value("${opensearch.url:none}") String osurl,
			@Value("${opensearch.user:none}") String osuser,
			@Value("${opensearch.password:none}") String ospassword,
			@Value("${opensearch.eventstatusindexname:ksm-statusevents}") String osStatusIndexName) throws SSLException {
		this.objectMapper = objectMapper;
		this.eventHistoryService  = eventHistoryService;
		this.osUrl = osurl;
		this.osStatusIndexName = osStatusIndexName;
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
    
	public static class OSEventsStatus {
		
		public String itemId;
		public String eventId;
		public BaseStatus status;
		@JsonProperty("@timestamp")
		public Instant timestamp = Instant.now();

		public OSEventsStatus(String itemId, String eventId, BaseStatus status) {
			this.itemId = itemId;
			this.eventId = eventId;
			this.status = status;
		}
		
	}	

	public void sendStatusHistory( Supplier<List<Item>> fun) {
		if(!isEnable()) {
			return;
		}
		if (lock.tryLock()) {
			try {
				sendStatusHistoryInternal(fun);
			} finally {
				lock.unlock();
			}
		} else {
			log.info(HISTORY + " Still active");
		}
	}
	
	private void sendStatusHistoryInternal( Supplier<List<Item>> fun) {
		var items = fun.get();
		
		if(items.size() == 0) {
			log.info(HISTORY + " Items list is empty");
			return;
		}
		
		var documents = items.stream()
				.flatMap(item -> {
					var itemId = item.getId();
					return item.getAggEventsStatus()
					.getAggEventsStatus()
					.entrySet()
					.stream()
					.map(s -> new OSEventsStatus(itemId, s.getKey(), s.getValue()));
				}).toList();
		
		BulkRequest.Builder br = new BulkRequest.Builder();
		documents.forEach(status -> {
					br.operations(op -> {
						return op.create(idx -> {
							return idx.index(osStatusIndexName).document(status);
						});
					});
				});
		
		BulkResponse result;
		try {
			result = openSearchClient.bulk(br.build());
			if(result.errors()) {
				log.info(HISTORY + " OpenSearch bulk operation has errors");
			}
		} catch (OpenSearchException | IOException e) {
			log.warning("sendStatusHistoryInternal exception: " + e.getMessage());
		}
		
		log.info(HISTORY + " Is completed. State [ Items: " + items.size() + " Documents: " + documents.size() + " ]");
	}
	
	private List<Event> getEventsByItemIdInternal(List<String> itemIds, Instant from, Instant to) {
		final String aggName = "events";
		final String aggByField = "eventId";
		final String fieldName = "itemId";
		final String fieldTime = "@timestamp";
		
		try {			
			var itemIdsTerms = new TermsQueryField.Builder()
				    .value(itemIds.stream().map(FieldValue::of).toList())
				    .build();
			var range = new RangeQuery.Builder().field(fieldTime).gte(JsonData.of(from)).to(JsonData.of(to)).build().toQuery();
			var terms = new TermsQuery.Builder().field(fieldName).terms(itemIdsTerms).build().toQuery();
			var eventIds = openSearchClient.search(s ->
				s.aggregations(aggName, f -> f.terms(t -> t.size(openSearchLimit).field(aggByField)))
				.size(0)
				.index(osStatusIndexName).query(q -> 
					q.bool(b -> 
					     b.must(range, terms)
					)
		
				)
			, OSEventsStatus.class)
			.aggregations()
			.get(aggName)
			.sterms()
			.buckets()
			.array()
			.stream()
			.map(b -> b.key())
			.toList();
			return eventHistoryService.getEventsByIds(eventIds).values().stream().toList();
		} catch (IOException | OpenSearchException e) {
			log.warning(HISTORY + " getEventsByItemIdInternal exception: " + e.getMessage());
		}
		return Collections.emptyList();
	}
	
	public List<Event> getEventsByItemId(List<String> itemIds, Instant from, Instant to) {
		if("none".equals(osUrl)) {
			return Collections.emptyList();
		}
		if(itemIds.size() == 0) {
			return Collections.emptyList();
		}
		AtomicInteger index = new AtomicInteger(0);
		return itemIds.stream()
				.collect(Collectors.groupingBy(s -> index.getAndIncrement() / groupLimit))
				.values().stream()
				.flatMap(l -> getEventsByItemIdInternal(l, from, to).stream())
				.toList();
	}
	
	public boolean isEnable() {
		return !"none".equals(osUrl);
	}
	
}

