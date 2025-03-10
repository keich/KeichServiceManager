package ru.keich.mon.servicemanager.event;

import java.io.IOException;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.opensearch.client.json.JsonData;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.client.opensearch._types.FieldValue;
import org.opensearch.client.opensearch._types.OpenSearchException;
import org.opensearch.client.opensearch._types.query_dsl.FieldAndFormat;
import org.opensearch.client.opensearch._types.query_dsl.TermsQueryField;
import org.opensearch.client.opensearch.core.BulkRequest;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.databind.ObjectMapper;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.java.Log;
import ru.keich.mon.servicemanager.history.HistoryQueue;
import ru.keich.mon.servicemanager.history.HistoryQueueDummyImp;
import ru.keich.mon.servicemanager.history.HistoryQueueImp;
import ru.keich.mon.servicemanager.history.OpenSearchClientBuilder;

@Service
@Log
public class EventHistoryService {
	private final HistoryQueue<Event> queue;
	private final OpenSearchClient openSearchClient;
	private final String osIndexName;
	private final Integer openSearchLimit = 10000;
	private final Integer groupLimit = 500;
	private final boolean enable;
	
	public EventHistoryService(@Value("${event.history.limit:1000}") Integer limit
			,@Value("${event.history.enable:false}") boolean historyEnable
			,ObjectMapper objectMapper
			,@Value("${opensearch.url:none}") String osurl
			,@Value("${opensearch.user:none}") String osuser
			,@Value("${opensearch.password:none}") String ospassword
			,@Value("${event.history.indexname:ksm-events}") String osIndexName) {
		if(!"none".equals(osurl) && !"none".equals(osuser)  && !"none".equals(ospassword)) {
			enable = true;
			
		} else {
			enable = false;
		}
		if(enable && historyEnable) {
			this.queue = new HistoryQueueImp<Event>(limit);
		} else {
			this.queue = new HistoryQueueDummyImp<Event>();
		}
		this.osIndexName = osIndexName;
		this.openSearchClient = OpenSearchClientBuilder.create(osurl, osuser, ospassword, objectMapper);
	}
	
	public void add(Event value) {
		queue.add(value);
	}

	@Scheduled(fixedRateString = "${event.history.fixedrate:10}", timeUnit = TimeUnit.SECONDS)
	public void schedule() {
		queue.poll(this::sendHistory);
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
			log.info("OpenSearch bulk operation has errors");
		}
		
	}
	
	@Getter
	@Setter
	private static class SimpleEvent {
		final String id;
		Instant deletedOn;
		static List<String> getFields() {
			return List.of("id", "deletedOn");
		}
		
		public SimpleEvent(Map<String, JsonData> map) {
			if(map.containsKey("id")) {
				this.id = map.get("id").toJson().asJsonArray().getString(0);
			} else {
				this.id = "none";
			}
			if(map.containsKey("deletedOn")) {
				this.deletedOn = Instant.parse(map.get("deletedOn").toJson().asJsonArray().getString(0));
			} else {
				this.deletedOn = null;
			}
		}
		
	}
	
	private Map<String, SimpleEvent> getSimpleEventsByIdsInternal(List<String> eventIds, Integer limit) {
		var afields = SimpleEvent.getFields().stream().map(f -> FieldAndFormat.of(b -> b.field(f))).toList();
		final String fieldName = "id";
		try {
			TermsQueryField Idsterms = new TermsQueryField.Builder()
				    .value(eventIds.stream().map(FieldValue::of).toList())
				    .build();
			return openSearchClient.search(s -> 
						s.fields(afields)
						.source(g -> g.fetch(false))
						.size(openSearchLimit)
						.index(osIndexName)
						.query(q -> 
							q.terms(t -> 
								t.field(fieldName).terms(Idsterms)
							)
						),
					Event.class)
					.hits()
					.hits()
					.stream()
					.map(h -> {
						return new SimpleEvent(h.fields());
					})
					.collect(Collectors.toMap(SimpleEvent::getId, Function.identity(), (e1, e2) -> {
						if (Objects.nonNull(e1.getDeletedOn())) {
							return e1;
						}
						return e2;
					}));

		} catch (IOException | OpenSearchException e) {
			log.warning("getEventsByIdsInternal exception: " + e.getMessage());
		}
		return Collections.emptyMap();
	}
	
	public Stream<Map.Entry<String, SimpleEvent>> getExistsEventsId(List<String> eventIds, Integer limit) {
		AtomicInteger index = new AtomicInteger(0);
		return eventIds.stream()
				.collect(Collectors.groupingBy(s -> index.getAndIncrement() / groupLimit))
				.values().stream()
				.map(l -> getSimpleEventsByIdsInternal(l, limit))
				.flatMap(m -> m.entrySet().stream());
	}
	
	private Map<String, Event> getEventsByIdsInternal(List<String> eventIds, Integer limit) {
		final String fieldName = "id";
		try {
			TermsQueryField Idsterms = new TermsQueryField.Builder()
				    .value(eventIds.stream().map(FieldValue::of).toList())
				    .build();
			return openSearchClient.search(s -> 
						s.size(openSearchLimit)
						.index(osIndexName)
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
						if (Objects.nonNull(e2.getDeletedOn())) {
							return new Event.Builder(e1).deletedOn(e2.getDeletedOn()).build();
						}
						return new Event.Builder(e2).deletedOn(e1.getDeletedOn()).build();
					}));

		} catch (IOException | OpenSearchException e) {
			log.warning("getEventsByIdsInternal exception: " + e.getMessage());
		}
		return Collections.emptyMap();
	}
	
	public Map<String, Event> getEventsByIds(List<String> eventIds, Integer limit) {
		if(!enable) {
			return Collections.emptyMap();
		}
		if(eventIds.size() == 0) {
			return Collections.emptyMap();
		}
		AtomicInteger index = new AtomicInteger(0);
		return eventIds.stream()
				.collect(Collectors.groupingBy(s -> index.getAndIncrement() / groupLimit))
				.values().stream()
				.map(l -> getEventsByIdsInternal(l, limit))
				.flatMap(m -> m.entrySet().stream())
				.collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (v1, v2) -> v1));
	}
	
	private void sendHistory(List<Event> inputEvents) {		
		AtomicInteger index = new AtomicInteger(0);
		var existsEvents = inputEvents.stream()
				.map(Event::getId)
				.collect(Collectors.groupingBy(s -> index.getAndIncrement() / groupLimit))
				.values().stream()
				.flatMap(l -> getExistsEventsId(l, openSearchLimit))
				.collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (v1, v2) -> v1));
		
		var filteredInputEvents = inputEvents.stream()
				.filter(event -> {
					var oldEvent = existsEvents.get(event.getId());
					if(oldEvent == null){
						return true;
					}
					if(oldEvent.getDeletedOn() != null) {
						return false;
					}
					if(event.getDeletedOn() != null) {
						return true;
					}
					return false;
				})
				.toList();
		try {
			sendEvents(filteredInputEvents);
		} catch (IOException | OpenSearchException e) {
			log.warning("sendHistoryInternal exception: " + e.getMessage());
		}

		log.info("Event history process is completed. State [Input eventd: " + inputEvents.size() 
				+ " ExistsEvents: " +  existsEvents.size() 
				+ " Documents: " + filteredInputEvents.size()
				+ " ] ");
	}
	
}
