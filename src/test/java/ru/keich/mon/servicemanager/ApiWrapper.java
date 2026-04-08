package ru.keich.mon.servicemanager;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.HttpMethod;
import org.springframework.test.web.servlet.client.RestTestClient;
import org.springframework.util.MultiValueMap;

import ru.keich.mon.servicemanager.alert.Alert;
import ru.keich.mon.servicemanager.event.Event;
import ru.keich.mon.servicemanager.item.Item;
import ru.keich.mon.servicemanager.query.QueryParamsParser;

public class ApiWrapper {

	private final RestTestClient restTestClient;

	public ApiWrapper(RestTestClient restTestClient) {
		this.restTestClient = restTestClient;
	}

	private <T> void entityAdd(String path, List<T> entities) {
		restTestClient
				.post()
				.uri("/api/v1" + path)
				.body(entities)
				.exchange()
				.expectStatus()
				.isOk();
	}

	private <T> void entityDel(String path, List<String> ids) {
		restTestClient
				.method(HttpMethod.DELETE)
				.uri("/api/v1" + path)
				.body(ids)
				.exchange()
				.expectStatus()
				.isOk();
	}
	
	private <T> List<T> entitySearch(String path, String search, ParameterizedTypeReference<List<T>> responseType) {
		var reqParam = Map.of(QueryParamsParser.QUERY_SEARCH, Collections.singletonList(search));
		return entitySearch(path, MultiValueMap.fromMultiValue(reqParam), responseType);
	}
	
	private <T> List<T> entitySearch(String path, MultiValueMap<String, String> reqParam, ParameterizedTypeReference<List<T>> responseType) {
		return restTestClient.get()
				.uri(uriBuilder  -> uriBuilder
						.path("/api/v1" + path)
						.queryParams(reqParam)
						.build())
				.exchangeSuccessfully()
				.expectBody(responseType)
				.returnResult()
				.getResponseBody();
	}
	
	private <T> T entityGet(String path, String id, ParameterizedTypeReference<T> responseType) {
		return restTestClient.get()
				.uri("/api/v1" + path + "/" + id)
				.exchangeSuccessfully()
				.expectBody(responseType)
				.returnResult()
				.getResponseBody();
	}

	public void itemAdd(List<Item> items) {
		entityAdd("/item", items);
	}
	
	public Item itemGet(String id) {
		return entityGet("/item", id, new ParameterizedTypeReference<Item>() {});
	}
	
	public List<Event> itemGetEvents(String id) {
		return restTestClient.get()
				.uri("/api/v1/item/" + id + "/events")
				.exchangeSuccessfully()
				.expectBody(new ParameterizedTypeReference<List<Event>>() {})
				.returnResult()
				.getResponseBody();
	}
	
	public List<Item> itemGetChildren(String id) {
		return restTestClient.get()
				.uri("/api/v1/item/" + id + "/children")
				.exchangeSuccessfully()
				.expectBody(new ParameterizedTypeReference<List<Item>>() {})
				.returnResult()
				.getResponseBody();
	}
	
	public Item itemGetTree(String id) {
		return restTestClient.get()
				.uri("/api/v1/item/" + id + "/tree")
				.exchangeSuccessfully()
				.expectBody(new ParameterizedTypeReference<Item>() {})
				.returnResult()
				.getResponseBody();
	}
	
	public Item itemGetParentsTree(String id) {
		return restTestClient.get()
				.uri("/api/v1/item/" + id + "/parents/tree")
				.exchangeSuccessfully()
				.expectBody(new ParameterizedTypeReference<Item>() {})
				.returnResult()
				.getResponseBody();
	}

	public List<Item> itemSearch(String search) {
		return entitySearch("/item", search, new ParameterizedTypeReference<List<Item>>() {});
	}
	
	public List<Item> itemSearch(MultiValueMap<String, String> reqParam) {
		return entitySearch("/item", reqParam, new ParameterizedTypeReference<List<Item>>() {});
	}

	public void itemDel(List<String> ids) {
		entityDel("/item", ids);
	}

	public void eventAdd( List<Event> items) {
		entityAdd("/event", items);
	}

	public List<Event> eventSearch(String search) {
		return entitySearch("/event", search, new ParameterizedTypeReference<List<Event>>() {});
	}
	
	public List<Event> eventSearch(MultiValueMap<String, String> reqParam) {
		return entitySearch("/event", reqParam, new ParameterizedTypeReference<List<Event>>() {});
	}

	public void eventDel(List<String> ids) {
		entityDel("/event", ids);
	}

	public void alertAdd(List<Alert> alerts) {
		restTestClient
				.post()
				.uri("/api/v2/alerts")
				.body(alerts)
				.exchange()
				.expectStatus()
				.isOk();
	}

	public Event eventGet(String id) {
		return entityGet("/event", id, new ParameterizedTypeReference<Event>() {});
	}

	public static final String PREFIX_NAME = "name_";
	public static final String PREFIX_NODE = "node_";
	public static final String PREFIX_SOURCE = "src_";
	public static final String PREFIX_SOURCEKEY = "src_key_";
	
	public void addItems(int size, String keyName) {
		var items = new ArrayList<Item>();
		
		for (int i = 0; i < size; i++) {
			Map<String, String> fields = Collections.emptyMap();
			if(i < 2) {
				fields = new HashMap<String, String>();
				fields.put(keyName + "_field0" + i, keyName + "_value0" + i);
				fields.put(keyName + "_field1" + i, keyName + "_value1" + i);
			}
			Instant deletedOn = null;
			if(i == 3) {
				deletedOn = Instant.now();
			}
			final var item = Item.Builder.getDefault(keyName + "_" + i)
					.name(PREFIX_NAME + i)
					.source(PREFIX_SOURCE + keyName)
					.sourceKey(PREFIX_SOURCEKEY + keyName)
					.fields(fields)
					.deletedOn(deletedOn)
					.build();
			items.add(item);
		}
		itemAdd(items);
	}

	public void addEvents(int size, String keyName) {
		var events = new ArrayList<Event>();
		for (int i = 0; i < size; i++) {
			Map<String, String> fields = Collections.emptyMap();
			if(i < 2) {
				fields = new HashMap<String, String>();
				fields.put(keyName + "_field0" + i, keyName + "_value0" + i);
				fields.put(keyName + "_field1" + i, keyName + "_value1" + i);
			}
			Instant deletedOn = null;
			if(i == 3) {
				deletedOn = Instant.now();
			}
			final var item = Event.Builder.getDefault(keyName + "_" + i)
					.node(PREFIX_NODE + i)
					.source(PREFIX_SOURCE + keyName)
					.sourceKey(PREFIX_SOURCEKEY + keyName)
					.deletedOn(deletedOn)
					.fields(fields)
					.build();
			events.add(item);
		}
		eventAdd(events);
	}

}
