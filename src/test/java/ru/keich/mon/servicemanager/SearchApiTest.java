package ru.keich.mon.servicemanager;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
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
					.name("name_" + i)
					.source("src_" + keyName)
					.sourceKey("src_key_" + keyName)
					.fields(fields)
					.deletedOn(deletedOn)
					.build();
			items.add(item);
		}
		apiWrapper.itemAdd(items);
	}

	private void addEvents(int size, String keyName) {
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
					.node("name_" + i)
					.source("src_" + keyName)
					.sourceKey("src_key_" + keyName)
					.deletedOn(deletedOn)
					.fields(fields)
					.build();
			events.add(item);
		}
		apiWrapper.eventAdd(events);
	}

	@Test
	public void itemIdEqual() {
		var key = "itemIdEqual";
		addItems(10, key);
		var id = key + "_3";
		var result = apiWrapper.itemSeach("id = \"" + id + "\"");
		assertEquals(1, result.size());
		assertEquals(id, result.get(0).getId());
		result = apiWrapper.itemSeach("item.id = \"" + id + "\"");
		assertEquals(1, result.size());
		assertEquals(id, result.get(0).getId());
	}

	@Test
	public void itemIdNotEqual() {
		var key = "itemIdNotEqual";
		addItems(10, key);
		var id = key + "_3";
		var result = apiWrapper.itemSeach("id != \"" + id + "\" AND source = \"src_" + key + "\"");
		assertEquals(9, result.stream().filter(i -> !id.equals(i.getId())).count());
		assertEquals(0, result.stream().filter(i -> id.equals(i.getId())).count());
		result = apiWrapper.itemSeach("item.id != \"" + id + "\" AND item.source = \"src_" + key + "\"");
		assertEquals(9, result.stream().filter(i -> !id.equals(i.getId())).count());
		assertEquals(0, result.stream().filter(i -> id.equals(i.getId())).count());
	}

	@Test
	public void itemIdInEqual() {
		var key = "itemIdInEqual";
		addItems(10, key);
		var id1 = key + "_3";
		var id2 = key + "_8";
		var result = apiWrapper.itemSeach("id IN (\"" + id1 + "\",\"" + id2 + "\")");
		assertEquals(2, result.size());
		assertEquals(2, result.stream().filter(i -> id1.equals(i.getId()) || id2.equals(i.getId())).count());
	}

	@Test
	public void itemNameEqual() {
		var key = "itemNameEqual";
		var id = key + "_t";
		var name = key + "_name";
		addItems(10, key);
		var items = new ArrayList<Item>();
		final var item = Item.Builder.getDefault(id)
				.name(name)
				.source("src_" + key)
				.sourceKey("src_key_" + key)
				.build();
		items.add(item);
		apiWrapper.itemAdd(items);
		var result = apiWrapper.itemSeach("name = \"" + name + "\"");
		assertTrue(result.stream().filter(i -> id.equals(i.getId())).findAny().isPresent());
		result = apiWrapper.itemSeach("item.name = \"" + name + "\"");
		assertTrue(result.stream().filter(i -> id.equals(i.getId())).findAny().isPresent());
	}
	
	@Test
	public void itemNameNotEqual() {
		var key = "itemNameNotEqual";
		var id = key + "_t";
		var name = key + "_name";
		addItems(10, key);
		var items = new ArrayList<Item>();
		final var item = Item.Builder.getDefault(id)
				.name(name)
				.source("src_" + key)
				.sourceKey("src_key_" + key)
				.build();
		items.add(item);
		apiWrapper.itemAdd(items);
		var result = apiWrapper.itemSeach("name != \"" + name + "\" AND source = \"src_" + key + "\"");
		assertEquals(10, result.stream().filter(i -> !id.equals(i.getId())).count());
		assertEquals(0, result.stream().filter(i -> id.equals(i.getId())).count());
		result = apiWrapper.itemSeach("item.name != \"" + name + "\" AND item.source = \"src_" + key + "\"");
		assertEquals(10, result.stream().filter(i -> !id.equals(i.getId())).count());
		assertEquals(0, result.stream().filter(i -> id.equals(i.getId())).count());
	}
	
	@Test
	public void itemNameInEqual() {
		var key = "itemNameInEqual";
		addItems(10, key);
		var name1 = "name_3";
		var name2 = "name_8";
		var result = apiWrapper.itemSeach("name IN (\"" + name1 + "\",\"" + name2 + "\") AND item.source = \"src_" + key + "\"");
		assertEquals(2, result.size());
		assertEquals(2, result.stream().filter(i -> name1.equals(i.getName()) || name2.equals(i.getName())).count());
	}

	@Test
	public void itemVersionEqual() {
		var key = "itemVersionEqual";
		var id = key + "_1";
		addItems(10, key);
		var result = apiWrapper.itemSeach("id = \"" + id + "\"");
		assertEquals(1, result.size());
		var ver = result.get(0).getVersion();
		result = apiWrapper.itemSeach("version = " + ver);
		assertEquals(1, result.stream().filter(i -> id.equals(i.getId())).count());
	}
	
	@Test
	public void itemVersionNotEqual() {
		var key = "itemVersionNotEqual";
		var id = key + "_1";
		addItems(10, key);
		var result = apiWrapper.itemSeach("id = \"" + id + "\"");
		assertEquals(1, result.size());
		var ver = result.get(0).getVersion();
		result = apiWrapper.itemSeach("version != " + ver + " AND source = \"src_" + key + "\"");
		assertEquals(9, result.stream().filter(i -> i.getVersion() != ver).count());
		assertEquals(0, result.stream().filter(i -> i.getVersion() == ver).count());
		result = apiWrapper.itemSeach("item.version != " + ver + " AND item.source = \"src_" + key + "\"");
		assertEquals(9, result.stream().filter(i -> i.getVersion() != ver).count());
		assertEquals(0, result.stream().filter(i -> i.getVersion() == ver).count());
	}

	@Test
	public void itemSourceEqual() {
		var key = "itemSourceEqual";
		addItems(10, key);
		addItems(10, key + "_");
		var source = "src_" + key;
		var result = apiWrapper.itemSeach("source = \"" + source + "\"");
		assertEquals(10, result.size());
		result = apiWrapper.itemSeach("item.source = \"" + source + "\"");
		assertEquals(10, result.size());
	}
	
	@Test
	public void itemSourceNotEqual() {
		var key = "itemSourceNotEqual";
		addItems(10, key);
		addItems(10, key + "_");
		var source = "src_" + key;
		var result = apiWrapper.itemSeach("source != \"" + source + "\"");
		assertTrue(result.size() > 0);
		assertEquals(0, result.stream().filter(i -> i.getSource() == source).count());
		result = apiWrapper.itemSeach("item.source != \"" + source + "\"");
		assertTrue(result.size() > 0);
		assertEquals(0, result.stream().filter(i -> i.getSource() == source).count());
	}

	@Test
	public void itemSourceKeyEqual() {
		var key = "itemSourceKeyEqual";
		addItems(10, key);
		addItems(10, key + "_");
		var sourceKey = "src_key_" + key;
		var result = apiWrapper.itemSeach("sourceKey = \"" + sourceKey + "\"");
		assertEquals(10, result.size());
		result = apiWrapper.itemSeach("item.sourceKey = \"" + sourceKey + "\"");
		assertEquals(10, result.size());
	}
	
	@Test
	public void itemSourceKeyNotEqual() {
		var key = "itemSourceKeyNotEqual";
		addItems(10, key);
		addItems(10, key + "_");
		var sourceKey = "src_key_" + key;
		var result = apiWrapper.itemSeach("source != \"" + sourceKey + "\"");
		assertTrue(result.size() > 0);
		assertEquals(0, result.stream().filter(i -> i.getSourceKey() == sourceKey).count());
		result = apiWrapper.itemSeach("item.source != \"" + sourceKey + "\"");
		assertTrue(result.size() > 0);
		assertEquals(0, result.stream().filter(i -> i.getSourceKey() == sourceKey).count());
	}

	@Test
	public void itemSourceTypeEqual() {
		var key = "itemSourceTypeEqual";
		var id = key + "_t";
		addItems(10, key);
		var items = new ArrayList<Item>();
		final var item  = Item.Builder.getDefault(id)
				.name("name")
				.source("src_" + key)
				.sourceKey("src_key_" + key)
				.sourceType(SourceType.ZABBIX)
				.build();
		items.add(item);
		apiWrapper.itemAdd(items);
		var result = apiWrapper.itemSeach("sourceType = \"" + SourceType.ZABBIX.toString() + "\"");
		assertTrue(result.stream().filter(i -> id.equals(i.getId())).findAny().isPresent());
		result = apiWrapper.itemSeach("item.sourceType = \"" + SourceType.ZABBIX.toString() + "\"");
		assertTrue(result.stream().filter(i -> id.equals(i.getId())).findAny().isPresent());
	}
	
	@Test
	public void itemSourceTypeNotEqual() {
		var key = "itemSourceTypeNotEqual";
		var id = key + "_t";
		addItems(10, key);
		var items = new ArrayList<Item>();
		final var item  = Item.Builder.getDefault(id)
				.name("name")
				.source("src_" + key)
				.sourceKey("src_key_" + key)
				.sourceType(SourceType.ZABBIX)
				.build();
		items.add(item);
		apiWrapper.itemAdd(items);
		var result = apiWrapper.itemSeach("sourceType != \"" + SourceType.ZABBIX.toString() + "\"");
		assertTrue(result.size() > 0);
		assertEquals(0, result.stream().filter(i -> i.getSourceKey() == SourceType.ZABBIX.toString()).count());
		result = apiWrapper.itemSeach("item.sourceType != \"" + SourceType.ZABBIX.toString() + "\"");
		assertTrue(result.size() > 0);
		assertEquals(0, result.stream().filter(i -> i.getSourceKey() == SourceType.ZABBIX.toString()).count());
	}

	@Test
	public void itemStatus() throws InterruptedException {
		var key = "itemStatus";
		addItems(10, key);
		addEvents(10, key);
		
		var itemRule = new ItemFilter(null, false, Collections.singletonMap("filter", key));
		Map<String, ItemFilter> filters = new HashMap<>();
		filters.put("filter_" + key, itemRule);
		
		var items = new ArrayList<Item>();
		var id = key + "_t";
		final var item = Item.Builder.getDefault(id)
				.name("name")
				.source("src_" + key)
				.sourceKey("src_key_" + key)
				.filters(filters)
				.build();
		items.add(item);
		apiWrapper.itemAdd(items);
		
		var events = new ArrayList<Event>();
		final var event = new Event
				.Builder(key + "_t")
				.node("name")
				.source("src_" + key)
				.sourceKey("src_key_" + key)
				.status(BaseStatus.WARNING)
				.fields(Collections.singletonMap("filter", key))
				.build();
		events.add(event);
		apiWrapper.eventAdd(events);
	
		List<Item> result = Collections.emptyList();
		for (int i = 0; i < 3; i++) {
			result = apiWrapper.itemSeach("source = \"src_" + key + "\" and status = " + BaseStatus.WARNING.toString());
			if(result.size() == 1) {
				break;
			}
			Thread.sleep(1000);
		}
		assertEquals(1, result.size());
		assertEquals(id, result.get(0).getId());
		result = apiWrapper.itemSeach("source = \"src_" + key + "\" and status != " + BaseStatus.WARNING.toString());
		assertEquals(10, result.size());
		assertEquals(0, result.stream().filter(i -> i.getStatus() == BaseStatus.WARNING).count());
		result = apiWrapper.itemSeach("source = \"src_" + key + "\" and aggStatus = " + BaseStatus.WARNING.toString());
		assertEquals(1, result.size());
		assertEquals(id, result.get(0).getId());

	}

	@Test
	public void itemFieldsEqual() {
		var key = "itemFieldsEqual";
		addItems(10, key);
		var id1 = key + "_t1";
		var id2 = key + "_t2";
		var items = new ArrayList<Item>();
		var item = Item.Builder.getDefault(id1)
				.name("name")
				.source("src_"  + key)
				.sourceKey("src_key_" + key)
				.fields(Collections.singletonMap(key + "_name", key + "_value"))
				.build();
		items.add(item);
		item = Item.Builder.getDefault(id2)
				.name("name")
				.source("src_"  + key)
				.sourceKey("src_key_" + key)
				.fields(Collections.singletonMap(key + "_name", key + "_value1"))
				.build();
		items.add(item);
		apiWrapper.itemAdd(items);
		var result = apiWrapper.itemSeach("fields.\"" + key + "_name\" = \"" + key + "_value\" AND source = \"src_" + key + "\"");
		assertTrue(result.stream().filter(i -> id1.equals(i.getId())).findAny().isPresent());
		assertEquals(0, result.stream().filter(i -> id2.equals(i.getId())).count());
		result = apiWrapper.itemSeach("item.fields.\"" + key + "_name\" = \"" + key + "_value\" AND source = \"src_" + key + "\"");
		assertTrue(result.stream().filter(i -> id1.equals(i.getId())).findAny().isPresent());
		assertEquals(0, result.stream().filter(i -> id2.equals(i.getId())).count());
	}

	@Test
	public void itemFromHistoryEqual() {
		var key = "itemFromHistoryEqual";
		addItems(10, key);
		var id = key + "_t";
		var items = new ArrayList<Item>();
		var fromHistory = new HashSet<String>();
		fromHistory.add(key + "_node1");
		fromHistory.add(key + "_node2");
		final var item = Item.Builder.getDefault(id)
				.name("name")
				.source("src_" + key)
				.sourceKey("src_key_" + key)
				.fromHistory(fromHistory)
				.build();
		items.add(item);
		apiWrapper.itemAdd(items);
		var result = apiWrapper.itemSeach("fromHistory = \"" + key + "_node1\"");
		assertTrue(result.stream().filter(i -> id.equals(i.getId())).findAny().isPresent());
	}

	@Test
	public void itemFromHistoryNotContain() {
		var key = "itemFromHistoryNotContain";
		var id1 = key + "_t1";
		var id2 = key + "_t2";
		var items = new ArrayList<Item>();
		var fromHistory = new HashSet<String>();
		fromHistory.add(key + "_node2");
		fromHistory.add(key + "_node3");
		var event = Item.Builder.getDefault(id1)
				.name("name")
				.source("src_" + key)
				.sourceKey("src_key_" + key)
				.fromHistory(fromHistory)
				.build();
		items.add(event);
		new HashSet<String>();
		fromHistory = new HashSet<String>();
		fromHistory.add(key + "_node1");
		fromHistory.add(key + "_node3");
		event = Item.Builder.getDefault(id2)
				.name("name")
				.source("src_" + key)
				.sourceKey("src_key_" + key)
				.fromHistory(fromHistory)
				.build();
		items.add(event);
		apiWrapper.itemAdd(items);
		var result = apiWrapper.itemSeach("fromHistory HAS NOT \"" + key +"_node1\" AND source = \"src_" + key + "\"");
		assertEquals(1, result.size());
		assertEquals(1, result.stream().filter(i -> id1.equals(i.getId())).count());
		result = apiWrapper.itemSeach("fromHistory HAS NOT \"" + key +"_node2\" AND source = \"src_" + key + "\"");
		assertEquals(1, result.size());
		assertEquals(1, result.stream().filter(i -> id2.equals(i.getId())).count());
	}

	@Test
	public void itemCreatedOnEqual() {
		var key = "itemCreatedOnEqual";
		addItems(10, key);
		var id = key + "_1";
		var result = apiWrapper.itemSeach("id = \"" + id + "\"");
		assertEquals(1, result.size());
		var createdOn = result.get(0).getCreatedOn();
		var result1 = apiWrapper.itemSeach("createdOn = \"" + createdOn + "\"");
		assertTrue(result1.stream().filter(i -> id.equals(i.getId())).findAny().isPresent());
	}

	@Test
	public void itemUpdatedOnEqual() {
		var key = "itemUpdatedOnEqual";
		var id = key + "_1";
		addItems(10, key);
		var result = apiWrapper.itemSeach("id = \"" + id + "\"");
		assertEquals(1, result.size());
		var updatedOn = result.get(0).getUpdatedOn();
		var result1 = apiWrapper.itemSeach("updatedOn = \"" + updatedOn + "\"");
		assertTrue(result1.stream().filter(i -> id.equals(i.getId())).findAny().isPresent());
	}

	@Test
	public void itemDeletedOnEqual() throws JsonProcessingException, InterruptedException {
		var key = "itemDeletedOnEqual";
		var id = key + "_1";
		addItems(10, key);
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
	public void itemAndEventLink() throws InterruptedException {
		var key = "itemAndEventLink";
		addEvents(10, key);
		addItems(10, key);
		
		var itemRule = new ItemFilter(null, false, Collections.singletonMap("filter", key));
		Map<String, ItemFilter> filters = new HashMap<>();
		filters.put("filter_" + key, itemRule);
		
		var items = new ArrayList<Item>();
		var itemId = key + "_t";
		final var item = Item.Builder.getDefault(itemId)
				.name("name")
				.source("src_" + key)
				.sourceKey("src_key_" + key)
				.filters(filters)
				.build();
		items.add(item);
		apiWrapper.itemAdd(items);
		
		var eventId = key + "_t";
		var events = new ArrayList<Event>();
		final var event = new Event
				.Builder(eventId)
				.node("name")
				.source("src_" + key)
				.sourceKey("src_key_" + key)
				.status(BaseStatus.WARNING)
				.fields(Collections.singletonMap("filter", key))
				.build();
		events.add(event);
		apiWrapper.eventAdd(events);
	
		List<Item> result = Collections.emptyList();
		for (int i = 0; i < 3; i++) {
			result = apiWrapper.itemSeach("event.source = \"src_" + key + "\"");
			if(result.size() == 1) {
				break;
			}
			Thread.sleep(1000);
		}
		assertEquals(1, result.size());
		assertEquals(itemId, result.get(0).getId());
	}

	@Test
	public void itemFieldsIsNull() {
		var key = "itemFieldsIsNull";
		addItems(10, key);
		var name = key + "_field01";
		var result = apiWrapper.itemSeach("fields.\"" + name + "\" IS NULL"+ " AND source = \"src_" + key + "\"");
		assertEquals(0, result.stream().filter(i -> i.getFields().keySet().contains(name)).count());
		assertEquals(9, result.size());
	}
	
	@Test
	public void itemDeletedOnIsNull() {
		var key = "itemDeletedOnIsNull";
		addItems(10, key);
		var result = apiWrapper.itemSeach("deletedOn IS NULL" + " AND source = \"src_" + key + "\"");
		assertEquals(0, result.stream().filter(i -> i.getDeletedOn() != null).count());
		assertEquals(9, result.size());

	}
	
	@Test
	public void eventIdEqual() {
		var key = "eventIdEqual";
		addEvents(10, key);
		var id = key + "_3";
		var result = apiWrapper.eventSeach("id = \"" + id + "\"");
		assertEquals(1, result.size());
		assertEquals(id, result.get(0).getId());
		result = apiWrapper.eventSeach("event.id = \"" + id + "\"");
		assertEquals(1, result.size());
		assertEquals(id, result.get(0).getId());
	}
	
	@Test
	public void eventIdNotEqual() {
		var key = "eventIdNotEqual";
		addEvents(10, key);
		var id = key + "_3";
		var result = apiWrapper.eventSeach("id != \"" + id + "\" AND source = \"src_" + key + "\"");
		assertEquals(9, result.stream().filter(i -> !id.equals(i.getId())).count());
		assertEquals(0, result.stream().filter(i -> id.equals(i.getId())).count());
		result = apiWrapper.eventSeach("event.id != \"" + id + "\" AND event.source = \"src_" + key + "\"");
		assertEquals(9, result.stream().filter(i -> !id.equals(i.getId())).count());
		assertEquals(0, result.stream().filter(i -> id.equals(i.getId())).count());
	}
	
	@Test
	public void eventIdInEqual() {
		var key = "eventIdInEqual";
		addEvents(10, key);
		var id1 = key + "_3";
		var id2 = key + "_8";
		var result = apiWrapper.eventSeach("id IN (\"" + id1 + "\",\"" + id2 + "\")");
		assertEquals(2, result.size());
		assertEquals(2, result.stream().filter(i -> id1.equals(i.getId()) || id2.equals(i.getId())).count());
	}

	@Test
	public void eventNodeEqual() {
		var key = "eventNodeEqual";
		var id = key + "_t";
		var node = key + "_nodeName";
		addEvents(10, key);
		var events = new ArrayList<Event>();
		final var event = Event.Builder.getDefault(id)
				.node(node)
				.source("src_" + key)
				.sourceKey("src_key_" + key)
				.build();
		events.add(event);
		apiWrapper.eventAdd(events);
		var result = apiWrapper.eventSeach("node = \"" + node + "\"");
		assertTrue(result.stream().filter(i -> id.equals(i.getId())).findAny().isPresent());
		result = apiWrapper.eventSeach("event.node = \"" + node + "\"");
		assertTrue(result.stream().filter(i -> id.equals(i.getId())).findAny().isPresent());
	}
	
	@Test
	public void eventNodeNotEqual() {
		var key = "eventNodeNotEqual";
		var id = key + "_t";
		var node = key + "_nodeName";
		addEvents(10, key);
		var events = new ArrayList<Event>();
		final var event = Event.Builder.getDefault(id)
				.node(node)
				.source("src_" + key)
				.sourceKey("src_key_" + key)
				.build();
		events.add(event);
		apiWrapper.eventAdd(events);
		var result = apiWrapper.eventSeach("node != \"" + node + "\" AND source = \"src_" + key + "\"");
		assertEquals(10, result.stream().filter(i -> !id.equals(i.getId())).count());
		assertEquals(0, result.stream().filter(i -> id.equals(i.getId())).count());
		result = apiWrapper.eventSeach("event.node != \"" + node + "\" AND event.source = \"src_" + key + "\"");
		assertEquals(10, result.stream().filter(i -> !id.equals(i.getId())).count());
		assertEquals(0, result.stream().filter(i -> id.equals(i.getId())).count());
	}
	
	@Test
	public void eventNodeInEqual() {
		var key = "eventNodeInEqual";
		addEvents(10, key);
		var node1 = "name_3";
		var node2 = "name_8";
		var result = apiWrapper.eventSeach("node IN (\"" + node1 + "\",\"" + node2 + "\") AND source = \"src_" + key + "\"");
		assertEquals(2, result.size());
		assertEquals(2, result.stream().filter(i -> node1.equals(i.getNode()) || node2.equals(i.getNode())).count());
	}

	@Test
	public void eventVersionEqual() {
		var key = "eventVersionEqual";
		var id = key + "_1";
		addEvents(10, key);
		var result = apiWrapper.eventSeach("id = \"" + id + "\"");
		assertEquals(1, result.size());
		var ver = result.get(0).getVersion();
		result = apiWrapper.eventSeach("version = " + ver);
		assertEquals(1, result.stream().filter(i -> id.equals(i.getId())).count());
	}
	
	@Test
	public void eventVersionNotEqual() {
		var key = "eventVersionNotEqual";
		var id = key + "_1";
		addEvents(10, key);
		var result = apiWrapper.eventSeach("id = \"" + id + "\"");
		assertEquals(1, result.size());
		var ver = result.get(0).getVersion();
		result = apiWrapper.eventSeach("version != " + ver + " AND source = \"src_" + key + "\"");
		assertEquals(9, result.stream().filter(i -> i.getVersion() != ver).count());
		assertEquals(0, result.stream().filter(i -> i.getVersion() == ver).count());
		result = apiWrapper.eventSeach("event.version != " + ver + " AND event.source = \"src_" + key + "\"");
		assertEquals(9, result.stream().filter(i -> i.getVersion() != ver).count());
		assertEquals(0, result.stream().filter(i -> i.getVersion() == ver).count());
	}

	@Test
	public void eventSourceEqual() {
		var key = "eventSourceEqual";
		addEvents(10, key);
		addEvents(10, key + "_");
		var source = "src_" + key;
		var result = apiWrapper.eventSeach("source = \"" + source + "\"");
		assertEquals(10, result.size());
		result = apiWrapper.eventSeach("event.source = \"" + source + "\"");
		assertEquals(10, result.size());
	}
	
	@Test
	public void eventSourceNotEqual() {
		var key = "eventSourceNotEqual";
		addEvents(10, key);
		addEvents(10, key + "_");
		var source = "src_" + key;
		var result = apiWrapper.eventSeach("source != \"" + source + "\"");
		assertTrue(result.size() > 0);
		assertEquals(0, result.stream().filter(i -> i.getSource() == source).count());
		result = apiWrapper.eventSeach("event.source != \"" + source + "\"");
		assertTrue(result.size() > 0);
		assertEquals(0, result.stream().filter(i -> i.getSource() == source).count());
	}

	@Test
	public void eventSourceKeyEqual() {
		var key = "eventSourceKeyEqual";
		addEvents(10, key);
		addEvents(10, key + "_");
		var sourceKey = "src_key_" + key;
		var result = apiWrapper.eventSeach("sourceKey = \"" + sourceKey + "\"");
		assertEquals(10, result.size());
		result = apiWrapper.eventSeach("event.sourceKey = \"" + sourceKey + "\"");
		assertEquals(10, result.size());
	}
	
	@Test
	public void eventSourceKeyNotEqual() {
		var key = "eventSourceKeyNotEqual";
		addEvents(10, key);
		addEvents(10, key + "_");
		var sourceKey = "src_key_" + key;
		var result = apiWrapper.eventSeach("source != \"" + sourceKey + "\"");
		assertTrue(result.size() > 0);
		assertEquals(0, result.stream().filter(i -> i.getSourceKey() == sourceKey).count());
		result = apiWrapper.eventSeach("event.source != \"" + sourceKey + "\"");
		assertTrue(result.size() > 0);
		assertEquals(0, result.stream().filter(i -> i.getSourceKey() == sourceKey).count());
	}

	@Test
	public void eventSourceTypeEqual() {
		var key = "eventSourceTypeEqual";
		var id = key + "_t";
		addEvents(10, key);
		var events = new ArrayList<Event>();
		final var event  = Event.Builder.getDefault(id)
				.node("name")
				.source("src_" + key)
				.sourceKey("src_key_" + key)
				.sourceType(SourceType.ZABBIX)
				.build();
		events.add(event);
		apiWrapper.eventAdd(events);
		var result = apiWrapper.eventSeach("sourceType = \"" + SourceType.ZABBIX.toString() + "\"");
		assertTrue(result.stream().filter(i -> id.equals(i.getId())).findAny().isPresent());
		result = apiWrapper.eventSeach("event.sourceType = \"" + SourceType.ZABBIX.toString() + "\"");
		assertTrue(result.stream().filter(i -> id.equals(i.getId())).findAny().isPresent());
	}
	
	@Test
	public void eventSourceTypeNotEqual() {
		var key = "eventSourceTypeNotEqual";
		var id = key + "_t";
		addEvents(10, key);
		var events = new ArrayList<Event>();
		final var event  = Event.Builder.getDefault(id)
				.node("name")
				.source("src_" + key)
				.sourceKey("src_key_" + key)
				.sourceType(SourceType.ZABBIX)
				.build();
		events.add(event);
		apiWrapper.eventAdd(events);
		var result = apiWrapper.eventSeach("sourceType != \"" + SourceType.ZABBIX.toString() + "\"");
		assertTrue(result.size() > 0);
		assertEquals(0, result.stream().filter(i -> i.getSourceKey() == SourceType.ZABBIX.toString()).count());
		result = apiWrapper.eventSeach("event.sourceType != \"" + SourceType.ZABBIX.toString() + "\"");
		assertTrue(result.size() > 0);
		assertEquals(0, result.stream().filter(i -> i.getSourceKey() == SourceType.ZABBIX.toString()).count());
	}

	@Test
	public void eventStatus() throws InterruptedException {
		var key = "eventStatus";
		addEvents(10, key);
		addItems(10, key);
		
		var itemRule = new ItemFilter(null, false, Collections.singletonMap("filter", key));
		Map<String, ItemFilter> filters = new HashMap<>();
		filters.put("filter_" + key, itemRule);
		
		var items = new ArrayList<Item>();
		var id = key + "_t";
		final var item = Item.Builder.getDefault(id)
				.name("name")
				.source("src_" + key)
				.sourceKey("src_key_" + key)
				.filters(filters)
				.build();
		items.add(item);
		apiWrapper.itemAdd(items);
		
		var events = new ArrayList<Event>();
		final var event = new Event
				.Builder(key + "_t")
				.node("name")
				.source("src_" + key)
				.sourceKey("src_key_" + key)
				.status(BaseStatus.WARNING)
				.fields(Collections.singletonMap("filter", key))
				.build();
		events.add(event);
		apiWrapper.eventAdd(events);
	
		List<Event> result = Collections.emptyList();
		for (int i = 0; i < 3; i++) {
			result = apiWrapper.eventSeach("source = \"src_" + key + "\" and status = " + BaseStatus.WARNING.toString());
			if(result.size() == 1) {
				break;
			}
			Thread.sleep(1000);
		}
		assertEquals(1, result.size());
		assertEquals(id, result.get(0).getId());
		result = apiWrapper.eventSeach("source = \"src_" + key + "\" and status != " + BaseStatus.WARNING.toString());
		assertEquals(10, result.size());
		assertEquals(0, result.stream().filter(i -> i.getStatus() == BaseStatus.WARNING).count());

	}
	
	@Test
	public void eventAndItemLink() throws InterruptedException {
		var key = "eventAndItemLink";
		addEvents(10, key);
		addItems(10, key);
		
		var itemRule = new ItemFilter(null, false, Collections.singletonMap("filter", key));
		Map<String, ItemFilter> filters = new HashMap<>();
		filters.put("filter_" + key, itemRule);
		
		var items = new ArrayList<Item>();
		var itemId = key + "_t";
		final var item = Item.Builder.getDefault(itemId)
				.name("name")
				.source("src_" + key)
				.sourceKey("src_key_" + key)
				.filters(filters)
				.build();
		items.add(item);
		apiWrapper.itemAdd(items);
		
		var eventId = key + "_t";
		var events = new ArrayList<Event>();
		final var event = new Event
				.Builder(eventId)
				.node("name")
				.source("src_" + key)
				.sourceKey("src_key_" + key)
				.status(BaseStatus.WARNING)
				.fields(Collections.singletonMap("filter", key))
				.build();
		events.add(event);
		apiWrapper.eventAdd(events);
	
		List<Event> result = Collections.emptyList();
		for (int i = 0; i < 3; i++) {
			result = apiWrapper.eventSeach("item.source = \"src_" + key + "\" and calculated = true");
			if(result.size() == 1) {
				break;
			}
			Thread.sleep(1000);
		}
		assertEquals(1, result.size());
		assertEquals(eventId, result.get(0).getId());
	}

	@Test
	public void eventFieldsEqual() {
		var key = "eventFieldsEqual";
		var id1 = key + "_t1";
		var id2 = key + "_t2";
		var events = new ArrayList<Event>();
		var event = new Event
				.Builder(id1)
				.node("name")
				.source("src_" + key)
				.sourceKey("src_key_" + key)
				.status(BaseStatus.WARNING)
				.fields(Collections.singletonMap(key + "_name", key + "_value"))
				.build();
		events.add(event);
		event = new Event
				.Builder(id2)
				.node("name")
				.source("src_" + key)
				.sourceKey("src_key_" + key)
				.status(BaseStatus.WARNING)
				.fields(Collections.singletonMap(key + "_name", key + "_value1"))
				.build();
		events.add(event);
		apiWrapper.eventAdd(events);
		var result = apiWrapper.eventSeach("fields.\"" + key + "_name\" = \"" + key + "_value\"");
		assertTrue(result.stream().filter(i -> id1.equals(i.getId())).findAny().isPresent());
		assertEquals(0, result.stream().filter(i -> id2.equals(i.getId())).count());
		result = apiWrapper.eventSeach("event.fields.\"" + key + "_name\" = \"" + key + "_value\"");
		assertTrue(result.stream().filter(i -> id1.equals(i.getId())).findAny().isPresent());
		assertEquals(0, result.stream().filter(i -> id2.equals(i.getId())).count());
	}

	@Test
	public void eventFromHistoryEqual() {
		var key = "eventFromHistoryEqual";
		var id = key + "_t";
		var events = new ArrayList<Event>();
		var fromHistory = new HashSet<String>();
		fromHistory.add(key + "_node1");
		fromHistory.add(key + "_node2");
		final var event = Event.Builder.getDefault(id)
				.node("node")
				.source("src_" + key)
				.sourceKey("src_key_" + key)
				.fromHistory(fromHistory)
				.build();
		events.add(event);
		apiWrapper.eventAdd(events);
		var result = apiWrapper.eventSeach("fromHistory = \"" + key +"_node1\""); 
		assertTrue(result.stream().filter(i -> id.equals(i.getId())).findAny().isPresent());
	}
	
	@Test
	public void eventFromHistoryNotContain() {
		var key = "eventFromHistoryNotContain";
		var id1 = key + "_t1";
		var id2 = key + "_t2";
		var events = new ArrayList<Event>();
		var fromHistory = new HashSet<String>();
		fromHistory.add(key + "_node2");
		fromHistory.add(key + "_node3");
		var event = Event.Builder.getDefault(id1)
				.node("node")
				.source("src_" + key)
				.sourceKey("src_key_" + key)
				.fromHistory(fromHistory)
				.build();
		events.add(event);
		new HashSet<String>();
		fromHistory = new HashSet<String>();
		fromHistory.add(key + "_node1");
		fromHistory.add(key + "_node3");
		event = Event.Builder.getDefault(id2)
				.node("node")
				.source("src_" + key)
				.sourceKey("src_key_" + key)
				.fromHistory(fromHistory)
				.build();
		events.add(event);
		apiWrapper.eventAdd(events);
		var result = apiWrapper.eventSeach("fromHistory HAS NOT \"" + key +"_node1\" AND source = \"src_" + key + "\"");
		assertEquals(1, result.size());
		assertEquals(1, result.stream().filter(i -> id1.equals(i.getId())).count());
		result = apiWrapper.eventSeach("fromHistory HAS NOT \"" + key +"_node2\" AND source = \"src_" + key + "\"");
		assertEquals(1, result.size());
		assertEquals(1, result.stream().filter(i -> id2.equals(i.getId())).count());
	}

	@Test
	public void eventDeletedOnIsNull() {
		var key = "eventDeletedOnIsNull";
		addEvents(10, key);
		var result = apiWrapper.eventSeach("deletedOn IS NULL" + " AND source = \"src_" + key + "\"");
		assertEquals(0, result.stream().filter(i -> i.getDeletedOn() != null).count());
		assertEquals(9, result.size());

	}

	@Test
	public void eventFieldsIsNull() {
		var key = "eventFieldsIsNull";
		addEvents(10, key);
		var name = key + "_field01";
		var result = apiWrapper.eventSeach("fields.\"" + name + "\" IS NULL"+ " AND source = \"src_" + key + "\"");
		assertEquals(0, result.stream().filter(i -> i.getFields().keySet().contains(name)).count());
		assertEquals(9, result.size());
	}

}
