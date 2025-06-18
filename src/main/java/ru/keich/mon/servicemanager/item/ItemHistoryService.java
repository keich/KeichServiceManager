package ru.keich.mon.servicemanager.item;

import java.io.IOException;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.assertj.core.util.Arrays;
import org.opensearch.client.json.JsonData;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.client.opensearch._types.FieldValue;
import org.opensearch.client.opensearch._types.OpenSearchException;
import org.opensearch.client.opensearch._types.aggregations.Aggregation;
import org.opensearch.client.opensearch._types.query_dsl.RangeQuery;
import org.opensearch.client.opensearch._types.query_dsl.TermsQuery;
import org.opensearch.client.opensearch._types.query_dsl.TermsQueryField;
import org.opensearch.client.opensearch.core.BulkRequest;
import org.opensearch.client.opensearch.core.BulkResponse;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;

import lombok.extern.java.Log;
import ru.keich.mon.servicemanager.BaseStatus;
import ru.keich.mon.servicemanager.event.Event;
import ru.keich.mon.servicemanager.event.EventHistoryService;
import ru.keich.mon.servicemanager.history.HistoryQueue;
import ru.keich.mon.servicemanager.history.HistoryQueueDummyImp;
import ru.keich.mon.servicemanager.history.HistoryQueueImp;
import ru.keich.mon.servicemanager.history.OpenSearchClientBuilder;

@Service
@Log
public class ItemHistoryService {
	private final EventHistoryService eventHistoryService;
	private final HistoryQueue<Item> queue;
	private final OpenSearchClient openSearchClient;
	private final String osStatusIndexName;
	private final Integer openSearchLimit = 10000;
	private final Integer groupLimit = 500;
	private final Integer eventSearchLimit = 1000;
	private final boolean enable;
	private final Map<String, Aggregation> aggFields;
	
	public ItemHistoryService(EventHistoryService eventHistoryService
			,@Value("${item.history.limit:1000}") Integer limit
			,@Value("${item.history.enable:false}") boolean historyEnable
			,ObjectMapper objectMapper
			,@Value("${opensearch.url:none}") String osurl
			,@Value("${opensearch.user:none}") String osuser
			,@Value("${opensearch.password:none}") String ospassword
			,@Value("${item.history.indexname:ksm-statusevents}") String osStatusIndexName) {

		if (!"none".equals(osurl) && !"none".equals(osuser)  && !"none".equals(ospassword)) {
			enable = true;
			
		} else {
			enable = false;
		}
		
		if (enable && historyEnable) {
			this.queue = new HistoryQueueImp<Item>(limit);
		} else {
			this.queue = new HistoryQueueDummyImp<Item>();
		}
		this.eventHistoryService = eventHistoryService;
		this.osStatusIndexName = osStatusIndexName;
		this.openSearchClient = OpenSearchClientBuilder.create(osurl, osuser, ospassword, objectMapper);
		
		aggFields = Arrays.asList(BaseStatus.values())
				.stream()
				.map(s -> s.toString())
				.map(s -> "events.".concat(s))
				.collect(Collectors.toMap(s -> s, s -> new Aggregation.Builder().terms(t -> t.field(s).size(openSearchLimit)).build()));
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

		@Override
		public int hashCode() {
			return Objects.hash(eventId, itemId);
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			OSEventsStatus other = (OSEventsStatus) obj;
			return eventId.equals(other.eventId) && itemId.equals(other.itemId);
		}
		
	}
	
	public static class OsItemStatus {
		public final String itemId;
		public final BaseStatus itemStatus;
		public final Map<BaseStatus,List<String>> events;
		@JsonProperty("@timestamp")
		public final Instant timestamp = Instant.now();

		public OsItemStatus(String itemId, BaseStatus itemStatus, Map<BaseStatus,List<String>> events) {
			this.itemId = itemId;
			this.itemStatus = itemStatus;
			this.events = events;
		}
	}
	
	public void add(Item value) {
		queue.add(value);
	}

	@Scheduled(fixedRateString = "${item.history.fixedrate:10}", timeUnit = TimeUnit.SECONDS)
	public void schedule() {
		queue.poll(this::sendStatusHistory);
	}

	private void sendStatusHistory(List<Item> items) {
		
		var documents = items.stream().map(item -> {
			var itemId = item.getId();
			var itemStatus = item.getAggStatus().getMax();
			var map = item.getEventsStatus()
					.entrySet()
					.stream()
					.collect(Collectors.groupingBy(Map.Entry::getValue))
					.entrySet()
					.stream()
					.collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().stream().map(s -> s.getKey()).toList()));	
			return new OsItemStatus(itemId, itemStatus, map);
		}).filter(r -> r.events.size() > 0).toList();
		
		if(documents.size() == 0) {
			return;
		}
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
			if (result.errors()) {
				log.info("OpenSearch bulk operation has errors");
			}
		} catch (OpenSearchException | IOException e) {
			log.warning("sendStatusHistory exception: " + e.getMessage());
		}
		log.info("Item history process is completed. State [Input items: " + items.size() 
		+ " Documents: " + documents.size()
		+ " ] ");
	}

	private List<Event> getEventsByItemIdInternal(List<String> itemIds, Instant from, Instant to) {
		final String fieldName = "itemId";
		final String fieldTime = "@timestamp";
		
		try {			
			var itemIdsTerms = new TermsQueryField.Builder()
				    .value(itemIds.stream().map(FieldValue::of).toList())
				    .build();
			var range = new RangeQuery.Builder().field(fieldTime).gte(JsonData.of(from)).to(JsonData.of(to)).build().toQuery();
			var terms = new TermsQuery.Builder().field(fieldName).terms(itemIdsTerms).build().toQuery();
			var eventIds = openSearchClient.search(s ->
				s.aggregations(aggFields)
						.size(0)
						.index(osStatusIndexName)
						.query(q -> q.bool(b -> b.must(range, terms)))
			, OsItemStatus.class)
			.aggregations()
			.entrySet()
			.stream()
			.flatMap(e -> e.getValue().sterms().buckets().array().stream().map(b -> b.key()))
			.toList();
			return eventHistoryService.getEventsByIds(eventIds, eventSearchLimit).values().stream().toList();
		} catch (IOException | OpenSearchException e) {
			log.warning("getEventsByItemIdInternal exception: " + e.getMessage());
		}
		return Collections.emptyList();
	}
	
	public List<Event> getEventsByItemId(List<String> itemIds, Instant from, Instant to) {
		if (itemIds.size() == 0 || !enable) {
			return Collections.emptyList();
		}
		AtomicInteger index = new AtomicInteger(0);
		return itemIds.stream()
				.collect(Collectors.groupingBy(s -> index.getAndIncrement() / groupLimit))
				.values().stream()
				.flatMap(l -> getEventsByItemIdInternal(l, from, to).stream())
				.toList();
	}
	
}
