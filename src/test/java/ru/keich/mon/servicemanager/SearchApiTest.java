package ru.keich.mon.servicemanager;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import com.fasterxml.jackson.core.JsonProcessingException;

import ru.keich.mon.servicemanager.event.Event;
import ru.keich.mon.servicemanager.item.Item;
import ru.keich.mon.servicemanager.item.ItemFilter;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
public class SearchApiTest {

	@Autowired
	private ApiWrapper apiWrapper;

	private void addItems(int size, String keyName) {
		var items = new ArrayList<Item>();
		for (int i = 0; i < size; i++) {
			final var item = Item.Builder.getDefault(keyName + "_" + i)
					.name("name_" + i)
					.source("src_" + keyName)
					.sourceKey("src_key_" + keyName)
					.build();
			items.add(item);
		}
		apiWrapper.itemAdd(items);
	}

	private void addEvents(int size, String keyName) {
		var events = new ArrayList<Event>();
		for (int i = 0; i < size; i++) {
			final var item = Event.Builder.getDefault(keyName + "_" + i)
					.node("name_" + i)
					.source("src_" + keyName)
					.sourceKey("src_key_" + keyName)
					.build();
			events.add(item);
		}
		apiWrapper.eventAdd(events);
	}

	@Test
	public void itemIdEqual() {
		addItems(10, "itemIdEqual");
		var id = "itemIdEqual_3";
		var result = apiWrapper.itemSeach("id = \"" + id + "\"");
		assertEquals(1, result.size());
		assertEquals(id, result.get(0).getId());
	}

	@Test
	public void itemNameEqual() {
		var id = "itemNameEqual_t";
		var name = "itemNameEqual_name";
		addItems(10, "itemNameEqual");
		var items = new ArrayList<Item>();
		final var item = Item.Builder.getDefault(id)
				.name(name)
				.source("src_itemNameEqual")
				.sourceKey("src_key_itemNameEqual")
				.build();
		items.add(item);
		apiWrapper.itemAdd(items);
		var result = apiWrapper.itemSeach("name = \"" + name + "\"");
		assertTrue(result.stream().filter(i -> id.equals(i.getId())).findAny().isPresent());
	}

	@Test
	public void itemVersionEqual() {
		var id = "itemVersionEqual_1";
		addItems(10, "itemVersionEqual");
		var result = apiWrapper.itemSeach("id = \"" + id + "\"");
		assertEquals(1, result.size());
		var ver = result.get(0).getVersion();
		var result1 = apiWrapper.itemSeach("version = " + ver);
		assertTrue(result1.stream().filter(i -> id.equals(i.getId())).findAny().isPresent());
	}

	@Test
	public void itemSourceEqual() {
		addItems(10, "itemSourceEqual");
		addItems(10, "itemSourceEqual_");
		var source = "src_itemSourceEqual";
		var result = apiWrapper.itemSeach("source = \"" + source + "\"");
		assertEquals(10, result.size());
	}

	@Test
	public void itemSourceKeyEqual() {
		addItems(10, "itemSourceKeyEqual");
		addItems(10, "itemSourceKeyEqual_");
		var source = "src_key_itemSourceKeyEqual";
		var result = apiWrapper.itemSeach("sourceKey = \"" + source + "\"");
		assertEquals(10, result.size());
	}

	@Test
	public void itemSourceTypeEqual() {
		var id = "itemSourceTypeEqual_t";
		addItems(10, "itemSourceTypeEqual");
		var items = new ArrayList<Item>();
		final var item  = Item.Builder.getDefault(id)
				.name("name")
				.source("src_itemSourceTypeEqual")
				.sourceKey("src_key_itemSourceTypeEqual")
				.sourceType(SourceType.ZABBIX)
				.build();
		items.add(item);
		apiWrapper.itemAdd(items);
		var result = apiWrapper.itemSeach("sourceType = \"ZABBIX\"");
		assertTrue(result.stream().filter(i -> id.equals(i.getId())).findAny().isPresent());
	}

	@Test
	public void itemStatusEqual() throws InterruptedException {
		addItems(10, "itemStatusEqual");
		
		var itemRule = new ItemFilter(null, false, Collections.singletonMap("filter", "itemStatusEqual"));
		Map<String, ItemFilter> filters = new HashMap<>();
		filters.put("filter_itemStatusEqual", itemRule);
		
		var items = new ArrayList<Item>();
		final var item = Item.Builder.getDefault("itemStatusEqual_t")
				.name("name")
				.source("src_itemStatusEqual")
				.sourceKey("src_key_itemStatusEqual")
				.filters(filters)
				.build();
		items.add(item);
		apiWrapper.itemAdd(items);
		
		var events = new ArrayList<Event>();
		final var event = new Event
				.Builder("itemStatusEqual_t")
				.node("name")
				.source("src_itemStatusEqual")
				.sourceKey("src_key_itemStatusEqual")
				.status(BaseStatus.WARNING)
				.fields(Collections.singletonMap("filter", "itemStatusEqual"))
				.build();
		events.add(event);
		apiWrapper.eventAdd(events);
	
		for (int i = 0; i < 3; i++) {
			var result = apiWrapper.itemSeach("source = \"src_itemStatusEqual\" and status = \"WARNING\"");
			if(result.size() == 1) {
				return;
			}
			Thread.sleep(1000);
		}
		for (int i = 0; i < 3; i++) {
			var result = apiWrapper.itemSeach("source = \"src_itemStatusEqual\" and aggStatus = \"WARNING\"");
			if(result.size() == 1) {
				return;
			}
			Thread.sleep(1000);
		}
		assertTrue(false);
	}

	@Test
	public void itemFieldsEqual() {
		var id = "itemFieldsEqual_t";
		var items = new ArrayList<Item>();
		final var item = Item.Builder.getDefault(id)
				.name("name")
				.source("src_itemFieldsEqual")
				.sourceKey("src_key_itemFieldsEqual")
				.fields(Collections.singletonMap("it\\\"emFieldsEqual_name", "itemFieldsEqual_value"))
				.build();
		items.add(item);
		apiWrapper.itemAdd(items);
		var result = apiWrapper.itemSeach("fields.\"it\\\"emFieldsEqual_name\" = \"itemFieldsEqual_value\"");
		assertTrue(result.stream().filter(i -> id.equals(i.getId())).findAny().isPresent());
	}

	@Test
	public void itemFromHistoryEqual() {
		var id = "itemFromHistoryEqual_t";
		var items = new ArrayList<Item>();
		var fromHistory = new HashSet<String>();
		fromHistory.add("itemFromHistoryEqual_node1");
		fromHistory.add("itemFromHistoryEqual_node2");
		final var item = Item.Builder.getDefault(id)
				.name("name")
				.source("src_itemFromHistoryEqual")
				.sourceKey("src_key_itemFromHistoryEqual")
				.fromHistory(fromHistory)
				.build();
		items.add(item);
		apiWrapper.itemAdd(items);
		var result = apiWrapper.itemSeach("fromHistory = \"itemFromHistoryEqual_node1\"");
		assertTrue(result.stream().filter(i -> id.equals(i.getId())).findAny().isPresent());
	}

	@Test
	public void itemCreatedOnEqual() {
		var id = "itemCreatedOnEqual_1";
		addItems(10, "itemCreatedOnEqual");
		var result = apiWrapper.itemSeach("id = \"" + id + "\"");
		assertEquals(1, result.size());
		var createdOn = result.get(0).getCreatedOn();
		var result1 = apiWrapper.itemSeach("createdOn = \"" + createdOn + "\"");
		assertTrue(result1.stream().filter(i -> id.equals(i.getId())).findAny().isPresent());
	}

	@Test
	public void itemUpdatedOnEqual() {
		var id = "itemUpdatedOnEqual_1";
		addItems(10, "itemUpdatedOnEqual");
		var result = apiWrapper.itemSeach("id = \"" + id + "\"");
		assertEquals(1, result.size());
		var updatedOn = result.get(0).getUpdatedOn();
		var result1 = apiWrapper.itemSeach("updatedOn = \"" + updatedOn + "\"");
		assertTrue(result1.stream().filter(i -> id.equals(i.getId())).findAny().isPresent());
	}

	@Test
	public void itemDeletedOnEqual() throws JsonProcessingException, InterruptedException {
		var id = "itemDeletedOnEqual_1";
		addItems(10, "itemDeletedOnEqual");
		apiWrapper.itemDel(Collections.singletonList(id));
		for (int i = 0; i < 3; i++) {
			var result = apiWrapper.itemSeach("id = \"" + id + "\"");
			assertEquals(1, result.size());
			var deletedOn = result.get(0).getDeletedOn();
			if (deletedOn != null) {
				var result1 = apiWrapper.itemSeach("deletedOn = \"" + deletedOn + "\"");
				assertTrue(result1.stream().filter(item -> id.equals(item.getId())).findAny().isPresent());
				return;
			}
			Thread.sleep(1000);
		}
		assertTrue(false);
	}

	@Test
	public void eventIdEqual() {
		addEvents(10, "eventIdEqual");
		var id = "eventIdEqual_3";
		var result = apiWrapper.eventSeach("id = \"" + id + "\"");
		assertEquals(1, result.size());
		assertEquals(id, result.get(0).getId());
	}
	
	@Test
	public void eventItemIdEqual() {
		addEvents(10, "eventItemIdEqual");
		addItems(10, "eventItemIdEqual");
		
		var itemRule = new ItemFilter(null, false, Collections.singletonMap("filter", "eventItemIdEqual"));
		Map<String, ItemFilter> filters = new HashMap<>();
		filters.put("filter_eventItemIdEqual", itemRule);
		
		var items = new ArrayList<Item>();
		final var item = Item.Builder.getDefault("eventItemIdEqual_t")
				.name("name")
				.source("src_eventItemIdEqual")
				.sourceKey("src_key_eventItemIdEqual")
				.filters(filters)
				.build();
		items.add(item);
		apiWrapper.itemAdd(items);
		
		var events = new ArrayList<Event>();
		final var event = new Event
				.Builder("eventItemIdEqual_t")
				.node("name")
				.source("src_eventItemIdEqual")
				.sourceKey("src_key_eventItemIdEqual")
				.status(BaseStatus.WARNING)
				.fields(Collections.singletonMap("filter", "eventItemIdEqual"))
				.build();
		events.add(event);
		apiWrapper.eventAdd(events);
		var id = "eventItemIdEqual_t";
		var result = apiWrapper.eventSeach("item.id = \"" + id + "\"");
		assertEquals(1, result.size());
		assertEquals(id, result.get(0).getId());
	}

	@Test
	public void eventNodeEqual() {
		var id = "eventNodeEqual_t";
		var node = "eventNodeEqual_name";
		var events = new ArrayList<Event>();
		final var event = new Event
				.Builder(id)
				.node(node)
				.source("src_eventNodeEqual")
				.sourceKey("src_key_eventNodeEqual")
				.status(BaseStatus.WARNING)
				.build();
		events.add(event);
		apiWrapper.eventAdd(events);
		var result = apiWrapper.eventSeach("node = \"" + node + "\"");
		assertTrue(result.stream().filter(i -> id.equals(i.getId())).findAny().isPresent());
	}

	@Test
	public void eventVersionEqual() {
		var id = "eventVersionEqual_1";
		addEvents(10, "eventVersionEqual");
		var result = apiWrapper.eventSeach("id = \"" + id + "\"");
		assertEquals(1, result.size());
		var ver = result.get(0).getVersion();
		var result1 = apiWrapper.eventSeach("version = " + ver);
		assertTrue(result1.stream().filter(i -> id.equals(i.getId())).findAny().isPresent());
	}

	@Test
	public void eventSummaryEqual() {
		var id = "eventSummaryEqual_t";
		var summary = "eventSummaryEqual_summary";
		var events = new ArrayList<Event>();
		final var event = new Event
				.Builder(id)
				.node("name")
				.summary(summary)
				.source("src_eventSummaryEqual")
				.sourceKey("src_key_eventSummaryEqual")
				.status(BaseStatus.WARNING)
				.build();
		events.add(event);
		apiWrapper.eventAdd(events);
		var result = apiWrapper.eventSeach("summary = \"" + summary + "\"");
		assertTrue(result.stream().filter(i -> id.equals(i.getId())).findAny().isPresent());
	}

	@Test
	public void eventSourceEqual() {
		addEvents(10, "eventSourceEqual");
		addEvents(10, "eventSourceEqual_");
		var source = "src_eventSourceEqual";
		var result = apiWrapper.eventSeach("source = \"" + source + "\"");
		assertEquals(10, result.size());
	}

	@Test
	public void eventSourceKeyEqual() {
		addEvents(10, "eventSourceKeyEqual");
		addEvents(10, "eventSourceKeyEqual_");
		var source = "src_key_eventSourceKeyEqual";
		var result = apiWrapper.eventSeach("sourceKey = \"" + source + "\"");
		assertEquals(10, result.size());
	}

	@Test
	public void eventSourceTypeEqual() {
		var id = "eventSourceTypeEqual_t";
		addEvents(10, "eventSourceTypeEqual");
		var events = new ArrayList<Event>();
		final var event  = Event.Builder.getDefault(id)
				.node("node")
				.source("src_eventSourceTypeEqual")
				.sourceKey("src_key_eventSourceTypeEqual")
				.sourceType(SourceType.ZABBIX)
				.build();
		events.add(event);
		apiWrapper.eventAdd(events);
		var result = apiWrapper.eventSeach("sourceType = \"ZABBIX\"");
		assertTrue(result.stream().filter(i -> id.equals(i.getId())).findAny().isPresent());
	}

	@Test
	public void eventStatusEqual() throws InterruptedException {
		addEvents(10, "eventStatusEqual");
		var events = new ArrayList<Event>();
		var id = "eventStatusEqual_t";
		final var event = new Event
				.Builder(id)
				.node("name")
				.source("src_eventStatusEqual")
				.sourceKey("src_key_eventStatusEqual")
				.status(BaseStatus.WARNING)
				.fields(Collections.singletonMap("filter", "itemStatusEqual"))
				.build();
		events.add(event);
		apiWrapper.eventAdd(events);
		var result = apiWrapper.eventSeach("source = \"src_eventStatusEqual\" and status = \"WARNING\"");
		assertEquals(1, result.size());
		assertEquals(id, result.get(0).getId());
	}

	@Test
	public void eventFieldsEqual() {
		var id = "eventFieldsEqual_t";
		var events = new ArrayList<Event>();
		final var event = new Event
				.Builder(id)
				.node("name")
				.source("src_eventFieldsEqual")
				.sourceKey("src_key_eventFieldsEqual")
				.status(BaseStatus.WARNING)
				.fields(Collections.singletonMap("eventFieldsEqual_name", "eventFieldsEqual_value"))
				.build();
		events.add(event);
		apiWrapper.eventAdd(events);
		var result = apiWrapper.eventSeach("fields.\"eventFieldsEqual_name\" = \"eventFieldsEqual_value\"");
		assertTrue(result.stream().filter(i -> id.equals(i.getId())).findAny().isPresent());
	}

	@Test
	public void eventFromHistoryEqual() {
		var id = "eventFromHistoryEqual_t";
		var events = new ArrayList<Event>();
		var fromHistory = new HashSet<String>();
		fromHistory.add("eventFromHistoryEqual_node1");
		fromHistory.add("eventFromHistoryEqual_node2");
		final var event = Event.Builder.getDefault(id)
				.node("node")
				.source("src_eventFromHistoryEqual")
				.sourceKey("src_key_eventFromHistoryEqual")
				.fromHistory(fromHistory)
				.build();
		events.add(event);
		apiWrapper.eventAdd(events);
		var result = apiWrapper.eventSeach("fromHistory = \"eventFromHistoryEqual_node1\"");
		assertTrue(result.stream().filter(i -> id.equals(i.getId())).findAny().isPresent());
	}

}
