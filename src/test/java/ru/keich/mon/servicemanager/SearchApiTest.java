package ru.keich.mon.servicemanager;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import ru.keich.mon.servicemanager.event.Event;
import ru.keich.mon.servicemanager.item.Item;
import ru.keich.mon.servicemanager.item.ItemFilter;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
public class SearchApiTest {

	@Autowired
	public ApiWrapper apiWrapper;

	@Test
	public void itemIdEqual() {
		var key = "itemIdEqual";
		apiWrapper.addItems(10, key);
		var id = key + "_3";
		var result = apiWrapper.itemSearch("id = \"" + id + "\"");
		assertEquals(1, result.size());
		assertEquals(id, result.get(0).getId());
		result = apiWrapper.itemSearch("item.id = \"" + id + "\"");
		assertEquals(1, result.size());
		assertEquals(id, result.get(0).getId());
	}

	@Test
	public void itemIdNotEqual() {
		var key = "itemIdNotEqual";
		apiWrapper.addItems(10, key);
		var id = key + "_3";
		var result = apiWrapper.itemSearch("id != \"" + id + "\" AND source = \"src_" + key + "\"");
		assertEquals(9, result.stream().filter(i -> !id.equals(i.getId())).count());
		assertEquals(0, result.stream().filter(i -> id.equals(i.getId())).count());
		result = apiWrapper.itemSearch("item.id != \"" + id + "\" AND item.source = \"src_" + key + "\"");
		assertEquals(9, result.stream().filter(i -> !id.equals(i.getId())).count());
		assertEquals(0, result.stream().filter(i -> id.equals(i.getId())).count());
	}

	@Test
	public void itemIdInEqual() {
		var key = "itemIdInEqual";
		apiWrapper.addItems(10, key);
		var id1 = key + "_3";
		var id2 = key + "_8";
		var result = apiWrapper.itemSearch("id IN (\"" + id1 + "\",\"" + id2 + "\")");
		assertEquals(2, result.size());
		assertEquals(2, result.stream().filter(i -> id1.equals(i.getId()) || id2.equals(i.getId())).count());
	}

	@Test
	public void itemNameEqual() {
		var key = "itemNameEqual";
		var id = key + "_t";
		var name = key + "_name";
		apiWrapper.addItems(10, key);
		var items = new ArrayList<Item>();
		final var item = Item.Builder.getDefault(id)
				.name(name)
				.source("src_" + key)
				.sourceKey("src_key_" + key)
				.build();
		items.add(item);
		apiWrapper.itemAdd(items);
		var result = apiWrapper.itemSearch("name = \"" + name + "\"");
		assertTrue(result.stream().filter(i -> id.equals(i.getId())).findAny().isPresent());
		result = apiWrapper.itemSearch("item.name = \"" + name + "\"");
		assertTrue(result.stream().filter(i -> id.equals(i.getId())).findAny().isPresent());
	}

	@Test
	public void itemNameNotEqual() {
		var key = "itemNameNotEqual";
		var id = key + "_t";
		var name = key + "_name";
		apiWrapper.addItems(10, key);
		var items = new ArrayList<Item>();
		final var item = Item.Builder.getDefault(id)
				.name(name)
				.source(ApiWrapper.PREFIX_SOURCE + key)
				.sourceKey(ApiWrapper.PREFIX_SOURCEKEY + key)
				.build();
		items.add(item);
		apiWrapper.itemAdd(items);
		var result = apiWrapper.itemSearch("name != \"" + name + "\" AND source = \"src_" + key + "\"");
		assertEquals(10, result.stream().filter(i -> !id.equals(i.getId())).count());
		assertEquals(0, result.stream().filter(i -> id.equals(i.getId())).count());
		result = apiWrapper.itemSearch("item.name != \"" + name + "\" AND item.source = \"src_" + key + "\"");
		assertEquals(10, result.stream().filter(i -> !id.equals(i.getId())).count());
		assertEquals(0, result.stream().filter(i -> id.equals(i.getId())).count());
	}

	@Test
	public void itemNameInEqual() {
		var key = "itemNameInEqual";
		apiWrapper.addItems(10, key);
		var name1 = "name_3";
		var name2 = "name_8";
		var result = apiWrapper.itemSearch("name IN (\"" + name1 + "\",\"" + name2 + "\") AND item.source = \"src_" + key + "\"");
		assertEquals(2, result.size());
		assertEquals(2, result.stream().filter(i -> name1.equals(i.getName()) || name2.equals(i.getName())).count());
	}

	@Test
	public void itemVersionEqual() {
		var key = "itemVersionEqual";
		var id = key + "_1";
		apiWrapper.addItems(10, key);
		var result = apiWrapper.itemSearch("id = \"" + id + "\"");
		assertEquals(1, result.size());
		var ver = result.get(0).getVersion();
		result = apiWrapper.itemSearch("version = " + ver);
		assertEquals(1, result.stream().filter(i -> id.equals(i.getId())).count());
	}

	@Test
	public void itemVersionNotEqual() {
		var key = "itemVersionNotEqual";
		var id = key + "_1";
		apiWrapper.addItems(10, key);
		var result = apiWrapper.itemSearch("id = \"" + id + "\"");
		assertEquals(1, result.size());
		var ver = result.get(0).getVersion();
		result = apiWrapper.itemSearch("version != " + ver + " AND source = \"src_" + key + "\"");
		assertEquals(9, result.stream().filter(i -> i.getVersion() != ver).count());
		assertEquals(0, result.stream().filter(i -> i.getVersion() == ver).count());
		result = apiWrapper.itemSearch("item.version != " + ver + " AND item.source = \"src_" + key + "\"");
		assertEquals(9, result.stream().filter(i -> i.getVersion() != ver).count());
		assertEquals(0, result.stream().filter(i -> i.getVersion() == ver).count());
	}

	@Test
	public void itemSourceEqual() {
		var key = "itemSourceEqual";
		apiWrapper.addItems(10, key);
		apiWrapper.addItems(10, key + "_");
		var source = "src_" + key;
		var result = apiWrapper.itemSearch("source = \"" + source + "\"");
		assertEquals(10, result.size());
		result = apiWrapper.itemSearch("item.source = \"" + source + "\"");
		assertEquals(10, result.size());
	}

	@Test
	public void itemSourceNotEqual() {
		var key = "itemSourceNotEqual";
		apiWrapper.addItems(10, key);
		apiWrapper.addItems(10, key + "_");
		var source = ApiWrapper.PREFIX_SOURCE + key;
		var result = apiWrapper.itemSearch("source != \"" + source + "\"");
		assertTrue(result.size() > 0);
		assertEquals(0, result.stream().filter(i -> i.getSource() == source).count());
		result = apiWrapper.itemSearch("item.source != \"" + source + "\"");
		assertTrue(result.size() > 0);
		assertEquals(0, result.stream().filter(i -> i.getSource() == source).count());
	}

	@Test
	public void itemSourceKeyEqual() {
		var key = "itemSourceKeyEqual";
		apiWrapper.addItems(10, key);
		apiWrapper.addItems(10, key + "_");
		var sourceKey = ApiWrapper.PREFIX_SOURCEKEY + key;
		var result = apiWrapper.itemSearch("sourceKey = \"" + sourceKey + "\"");
		assertEquals(10, result.size());
		result = apiWrapper.itemSearch("item.sourceKey = \"" + sourceKey + "\"");
		assertEquals(10, result.size());
	}

	@Test
	public void itemSourceKeyNotEqual() {
		var key = "itemSourceKeyNotEqual";
		apiWrapper.addItems(10, key);
		apiWrapper.addItems(10, key + "_");
		var sourceKey = ApiWrapper.PREFIX_SOURCEKEY + key;
		var result = apiWrapper.itemSearch("source != \"" + sourceKey + "\"");
		assertTrue(result.size() > 0);
		assertEquals(0, result.stream().filter(i -> i.getSourceKey() == sourceKey).count());
		result = apiWrapper.itemSearch("item.source != \"" + sourceKey + "\"");
		assertTrue(result.size() > 0);
		assertEquals(0, result.stream().filter(i -> i.getSourceKey() == sourceKey).count());
	}

	@Test
	public void itemSourceTypeEqual() {
		var key = "itemSourceTypeEqual";
		var id = key + "_t";
		apiWrapper.addItems(10, key);
		var items = new ArrayList<Item>();
		final var item  = Item.Builder.getDefault(id)
				.name("name")
				.source(ApiWrapper.PREFIX_SOURCE + key)
				.sourceKey(ApiWrapper.PREFIX_SOURCEKEY + key)
				.sourceType(SourceType.ZABBIX)
				.build();
		items.add(item);
		apiWrapper.itemAdd(items);
		var result = apiWrapper.itemSearch("sourceType = \"" + SourceType.ZABBIX.toString() + "\"");
		assertTrue(result.stream().filter(i -> id.equals(i.getId())).findAny().isPresent());
		result = apiWrapper.itemSearch("item.sourceType = \"" + SourceType.ZABBIX.toString() + "\"");
		assertTrue(result.stream().filter(i -> id.equals(i.getId())).findAny().isPresent());
	}

	@Test
	public void itemSourceTypeNotEqual() {
		var key = "itemSourceTypeNotEqual";
		var id = key + "_t";
		apiWrapper.addItems(10, key);
		var items = new ArrayList<Item>();
		final var item  = Item.Builder.getDefault(id)
				.name("name")
				.source(ApiWrapper.PREFIX_SOURCE + key)
				.sourceKey(ApiWrapper.PREFIX_SOURCEKEY + key)
				.sourceType(SourceType.ZABBIX)
				.build();
		items.add(item);
		apiWrapper.itemAdd(items);
		var result = apiWrapper.itemSearch("sourceType != \"" + SourceType.ZABBIX.toString() + "\"");
		assertTrue(result.size() > 0);
		assertEquals(0, result.stream().filter(i -> i.getSourceKey() == SourceType.ZABBIX.toString()).count());
		result = apiWrapper.itemSearch("item.sourceType != \"" + SourceType.ZABBIX.toString() + "\"");
		assertTrue(result.size() > 0);
		assertEquals(0, result.stream().filter(i -> i.getSourceKey() == SourceType.ZABBIX.toString()).count());
	}

	@Test
	public void itemStatus() throws InterruptedException {
		var key = "itemStatus";
		apiWrapper.addItems(10, key);
		apiWrapper.addEvents(10, key);
		
		var itemRule = new ItemFilter(null, false, Collections.singletonMap("filter", key));
		Map<String, ItemFilter> filters = new HashMap<>();
		filters.put("filter_" + key, itemRule);
		
		var items = new ArrayList<Item>();
		var id = key + "_t";
		final var item = Item.Builder.getDefault(id)
				.name("name")
				.source(ApiWrapper.PREFIX_SOURCE + key)
				.sourceKey(ApiWrapper.PREFIX_SOURCEKEY + key)
				.filters(filters)
				.build();
		items.add(item);
		apiWrapper.itemAdd(items);
		
		var events = new ArrayList<Event>();
		final var event = new Event
				.Builder(key + "_t")
				.node("name")
				.source(ApiWrapper.PREFIX_SOURCE + key)
				.sourceKey(ApiWrapper.PREFIX_SOURCEKEY + key)
				.status(BaseStatus.WARNING)
				.fields(Collections.singletonMap("filter", key))
				.build();
		events.add(event);
		apiWrapper.eventAdd(events);
	
		List<Item> result = Collections.emptyList();
		for (int i = 0; i < 3; i++) {
			result = apiWrapper.itemSearch("source = \"src_" + key + "\" and status = " + BaseStatus.WARNING.toString());
			if(result.size() == 1) {
				break;
			}
			Thread.sleep(1000);
		}
		assertEquals(1, result.size());
		assertEquals(id, result.get(0).getId());
		result = apiWrapper.itemSearch("source = \"src_" + key + "\" and status != " + BaseStatus.WARNING.toString());
		assertEquals(10, result.size());
		assertEquals(0, result.stream().filter(i -> i.getStatus() == BaseStatus.WARNING).count());
		result = apiWrapper.itemSearch("source = \"src_" + key + "\" and aggStatus = " + BaseStatus.WARNING.toString());
		assertEquals(1, result.size());
		assertEquals(id, result.get(0).getId());

	}

	@Test
	public void itemFieldsEqual() {
		var key = "itemFieldsEqual";
		apiWrapper.addItems(10, key);
		var id1 = key + "_t1";
		var id2 = key + "_t2";
		var items = new ArrayList<Item>();
		var item = Item.Builder.getDefault(id1)
				.name("name")
				.source(ApiWrapper.PREFIX_SOURCE + key)
				.sourceKey(ApiWrapper.PREFIX_SOURCEKEY + key)
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
		var result = apiWrapper.itemSearch("fields.\"" + key + "_name\" = \"" + key + "_value\" AND source = \"src_" + key + "\"");
		assertTrue(result.stream().filter(i -> id1.equals(i.getId())).findAny().isPresent());
		assertEquals(0, result.stream().filter(i -> id2.equals(i.getId())).count());
		result = apiWrapper.itemSearch("item.fields.\"" + key + "_name\" = \"" + key + "_value\" AND source = \"src_" + key + "\"");
		assertTrue(result.stream().filter(i -> id1.equals(i.getId())).findAny().isPresent());
		assertEquals(0, result.stream().filter(i -> id2.equals(i.getId())).count());
	}

	@Test
	public void itemFromHistoryEqual() {
		var key = "itemFromHistoryEqual";
		apiWrapper.addItems(10, key);
		var id = key + "_t";
		var items = new ArrayList<Item>();
		var fromHistory = new HashSet<String>();
		fromHistory.add(key + "_node1");
		fromHistory.add(key + "_node2");
		final var item = Item.Builder.getDefault(id)
				.name("name")
				.source(ApiWrapper.PREFIX_SOURCE + key)
				.sourceKey(ApiWrapper.PREFIX_SOURCEKEY + key)
				.fromHistory(fromHistory)
				.build();
		items.add(item);
		apiWrapper.itemAdd(items);
		var result = apiWrapper.itemSearch("fromHistory = \"" + key + "_node1\"");
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
				.source(ApiWrapper.PREFIX_SOURCE + key)
				.sourceKey(ApiWrapper.PREFIX_SOURCEKEY + key)
				.fromHistory(fromHistory)
				.build();
		items.add(event);
		new HashSet<String>();
		fromHistory = new HashSet<String>();
		fromHistory.add(key + "_node1");
		fromHistory.add(key + "_node3");
		event = Item.Builder.getDefault(id2)
				.name("name")
				.source(ApiWrapper.PREFIX_SOURCE + key)
				.sourceKey(ApiWrapper.PREFIX_SOURCEKEY + key)
				.fromHistory(fromHistory)
				.build();
		items.add(event);
		apiWrapper.itemAdd(items);
		var result = apiWrapper.itemSearch("fromHistory HAS NOT \"" + key +"_node1\" AND source = \"src_" + key + "\"");
		assertEquals(1, result.size());
		assertEquals(1, result.stream().filter(i -> id1.equals(i.getId())).count());
		result = apiWrapper.itemSearch("fromHistory HAS NOT \"" + key +"_node2\" AND source = \"src_" + key + "\"");
		assertEquals(1, result.size());
		assertEquals(1, result.stream().filter(i -> id2.equals(i.getId())).count());
	}

	@Test
	public void itemCreatedOnEqual() {
		var key = "itemCreatedOnEqual";
		apiWrapper.addItems(10, key);
		var id = key + "_1";
		var result = apiWrapper.itemSearch("id = \"" + id + "\"");
		assertEquals(1, result.size());
		var createdOn = result.get(0).getCreatedOn();
		var result1 = apiWrapper.itemSearch("createdOn = \"" + createdOn + "\"");
		assertTrue(result1.stream().filter(i -> id.equals(i.getId())).findAny().isPresent());
	}

	@Test
	public void itemUpdatedOnEqual() {
		var key = "itemUpdatedOnEqual";
		var id = key + "_1";
		apiWrapper.addItems(10, key);
		var result = apiWrapper.itemSearch("id = \"" + id + "\"");
		assertEquals(1, result.size());
		var updatedOn = result.get(0).getUpdatedOn();
		var result1 = apiWrapper.itemSearch("updatedOn = \"" + updatedOn + "\"");
		assertTrue(result1.stream().filter(i -> id.equals(i.getId())).findAny().isPresent());
	}

	@Test
	public void itemDeletedOnEqual() throws InterruptedException {
		var key = "itemDeletedOnEqual";
		var id = key + "_1";
		apiWrapper.addItems(10, key);
		apiWrapper.itemDel(Collections.singletonList(id));
		for (int i = 0; i < 3; i++) {
			var result = apiWrapper.itemSearch("id = \"" + id + "\"");
			assertEquals(1, result.size());
			var deletedOn = result.get(0).getDeletedOn();
			if (deletedOn != null) {
				var result1 = apiWrapper.itemSearch("deletedOn = \"" + deletedOn + "\"");
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
		apiWrapper.addEvents(10, key);
		apiWrapper.addItems(10, key);
		
		var itemRule = new ItemFilter(null, false, Collections.singletonMap("filter", key));
		Map<String, ItemFilter> filters = new HashMap<>();
		filters.put("filter_" + key, itemRule);
		
		var items = new ArrayList<Item>();
		var itemId = key + "_t";
		final var item = Item.Builder.getDefault(itemId)
				.name("name")
				.source(ApiWrapper.PREFIX_SOURCE + key)
				.sourceKey(ApiWrapper.PREFIX_SOURCEKEY + key)
				.filters(filters)
				.build();
		items.add(item);
		apiWrapper.itemAdd(items);
		
		var eventId = key + "_t";
		var events = new ArrayList<Event>();
		final var event = new Event
				.Builder(eventId)
				.node("name")
				.source(ApiWrapper.PREFIX_SOURCE + key)
				.sourceKey(ApiWrapper.PREFIX_SOURCEKEY + key)
				.status(BaseStatus.WARNING)
				.fields(Collections.singletonMap("filter", key))
				.build();
		events.add(event);
		apiWrapper.eventAdd(events);
	
		List<Item> result = Collections.emptyList();
		for (int i = 0; i < 3; i++) {
			result = apiWrapper.itemSearch("event.source = \"src_" + key + "\"");
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
		apiWrapper.addItems(10, key);
		var name = key + "_field01";
		var result = apiWrapper.itemSearch("fields.\"" + name + "\" IS NULL"+ " AND source = \"src_" + key + "\"");
		assertEquals(0, result.stream().filter(i -> i.getFields().keySet().contains(name)).count());
		assertEquals(9, result.size());
	}

	@Test
	public void itemDeletedOnIsNull() {
		var key = "itemDeletedOnIsNull";
		apiWrapper.addItems(10, key);
		var result = apiWrapper.itemSearch("deletedOn IS NULL" + " AND source = \"src_" + key + "\"");
		assertEquals(0, result.stream().filter(i -> i.getDeletedOn() != null).count());
		assertEquals(9, result.size());

	}

	@Test
	public void eventIdEqual() {
		var key = "eventIdEqual";
		apiWrapper.addEvents(10, key);
		var id = key + "_3";
		var result = apiWrapper.eventSearch("id = \"" + id + "\"");
		assertEquals(1, result.size());
		assertEquals(id, result.get(0).getId());
		result = apiWrapper.eventSearch("event.id = \"" + id + "\"");
		assertEquals(1, result.size());
		assertEquals(id, result.get(0).getId());
	}

	@Test
	public void eventIdNotEqual() {
		var key = "eventIdNotEqual";
		apiWrapper.addEvents(10, key);
		var id = key + "_3";
		var result = apiWrapper.eventSearch("id != \"" + id + "\" AND source = \"src_" + key + "\"");
		assertEquals(9, result.stream().filter(i -> !id.equals(i.getId())).count());
		assertEquals(0, result.stream().filter(i -> id.equals(i.getId())).count());
		result = apiWrapper.eventSearch("event.id != \"" + id + "\" AND event.source = \"src_" + key + "\"");
		assertEquals(9, result.stream().filter(i -> !id.equals(i.getId())).count());
		assertEquals(0, result.stream().filter(i -> id.equals(i.getId())).count());
	}

	@Test
	public void eventIdInEqual() {
		var key = "eventIdInEqual";
		apiWrapper.addEvents(10, key);
		var id1 = key + "_3";
		var id2 = key + "_8";
		var result = apiWrapper.eventSearch("id IN (\"" + id1 + "\",\"" + id2 + "\")");
		assertEquals(2, result.size());
		assertEquals(2, result.stream().filter(i -> id1.equals(i.getId()) || id2.equals(i.getId())).count());
	}

	@Test
	public void eventNodeEqual() {
		var key = "eventNodeEqual";
		var id = key + "_t";
		var node = key + "_nodeName";
		apiWrapper.addEvents(10, key);
		var events = new ArrayList<Event>();
		final var event = Event.Builder.getDefault(id)
				.node(node)
				.source(ApiWrapper.PREFIX_SOURCE + key)
				.sourceKey(ApiWrapper.PREFIX_SOURCEKEY + key)
				.build();
		events.add(event);
		apiWrapper.eventAdd(events);
		var result = apiWrapper.eventSearch("node = \"" + node + "\"");
		assertTrue(result.stream().filter(i -> id.equals(i.getId())).findAny().isPresent());
		result = apiWrapper.eventSearch("event.node = \"" + node + "\"");
		assertTrue(result.stream().filter(i -> id.equals(i.getId())).findAny().isPresent());
	}

	@Test
	public void eventNodeNotEqual() {
		var key = "eventNodeNotEqual";
		var id = key + "_t";
		var node = key + "_nodeName";
		apiWrapper.addEvents(10, key);
		var events = new ArrayList<Event>();
		final var event = Event.Builder.getDefault(id)
				.node(node)
				.source(ApiWrapper.PREFIX_SOURCE + key)
				.sourceKey(ApiWrapper.PREFIX_SOURCEKEY + key)
				.build();
		events.add(event);
		apiWrapper.eventAdd(events);
		var result = apiWrapper.eventSearch("node != \"" + node + "\" AND source = \"src_" + key + "\"");
		assertEquals(10, result.stream().filter(i -> !id.equals(i.getId())).count());
		assertEquals(0, result.stream().filter(i -> id.equals(i.getId())).count());
		result = apiWrapper.eventSearch("event.node != \"" + node + "\" AND event.source = \"src_" + key + "\"");
		assertEquals(10, result.stream().filter(i -> !id.equals(i.getId())).count());
		assertEquals(0, result.stream().filter(i -> id.equals(i.getId())).count());
	}

	@Test
	public void eventNodeInEqual() {
		var key = "eventNodeInEqual";
		apiWrapper.addEvents(10, key);
		var node1 = ApiWrapper.PREFIX_NODE + "3";
		var node2 = ApiWrapper.PREFIX_NODE + "8";
		var result = apiWrapper.eventSearch("node IN (\"" + node1 + "\",\"" + node2 + "\") AND source = \"src_" + key + "\"");
		assertEquals(2, result.size());
		assertEquals(2, result.stream().filter(i -> node1.equals(i.getNode()) || node2.equals(i.getNode())).count());
	}

	@Test
	public void eventVersionEqual() {
		var key = "eventVersionEqual";
		var id = key + "_1";
		apiWrapper.addEvents(10, key);
		var result = apiWrapper.eventSearch("id = \"" + id + "\"");
		assertEquals(1, result.size());
		var ver = result.get(0).getVersion();
		result = apiWrapper.eventSearch("version = " + ver);
		assertEquals(1, result.stream().filter(i -> id.equals(i.getId())).count());
	}

	@Test
	public void eventVersionNotEqual() {
		var key = "eventVersionNotEqual";
		var id = key + "_1";
		apiWrapper.addEvents(10, key);
		var result = apiWrapper.eventSearch("id = \"" + id + "\"");
		assertEquals(1, result.size());
		var ver = result.get(0).getVersion();
		result = apiWrapper.eventSearch("version != " + ver + " AND source = \"src_" + key + "\"");
		assertEquals(9, result.stream().filter(i -> i.getVersion() != ver).count());
		assertEquals(0, result.stream().filter(i -> i.getVersion() == ver).count());
		result = apiWrapper.eventSearch("event.version != " + ver + " AND event.source = \"src_" + key + "\"");
		assertEquals(9, result.stream().filter(i -> i.getVersion() != ver).count());
		assertEquals(0, result.stream().filter(i -> i.getVersion() == ver).count());
	}

	@Test
	public void eventSourceEqual() {
		var key = "eventSourceEqual";
		apiWrapper.addEvents(10, key);
		apiWrapper.addEvents(10, key + "_");
		var source = ApiWrapper.PREFIX_SOURCE + key;
		var result = apiWrapper.eventSearch("source = \"" + source + "\"");
		assertEquals(10, result.size());
		result = apiWrapper.eventSearch("event.source = \"" + source + "\"");
		assertEquals(10, result.size());
	}

	@Test
	public void eventSourceNotEqual() {
		var key = "eventSourceNotEqual";
		apiWrapper.addEvents(10, key);
		apiWrapper.addEvents(10, key + "_");
		var source = ApiWrapper.PREFIX_SOURCE + key;
		var result = apiWrapper.eventSearch("source != \"" + source + "\"");
		assertTrue(result.size() > 0);
		assertEquals(0, result.stream().filter(i -> i.getSource() == source).count());
		result = apiWrapper.eventSearch("event.source != \"" + source + "\"");
		assertTrue(result.size() > 0);
		assertEquals(0, result.stream().filter(i -> i.getSource() == source).count());
	}

	@Test
	public void eventSourceKeyEqual() {
		var key = "eventSourceKeyEqual";
		apiWrapper.addEvents(10, key);
		apiWrapper.addEvents(10, key + "_");
		var sourceKey = ApiWrapper.PREFIX_SOURCEKEY + key;
		var result = apiWrapper.eventSearch("sourceKey = \"" + sourceKey + "\"");
		assertEquals(10, result.size());
		result = apiWrapper.eventSearch("event.sourceKey = \"" + sourceKey + "\"");
		assertEquals(10, result.size());
	}

	@Test
	public void eventSourceKeyNotEqual() {
		var key = "eventSourceKeyNotEqual";
		apiWrapper.addEvents(10, key);
		apiWrapper.addEvents(10, key + "_");
		var sourceKey = ApiWrapper.PREFIX_SOURCEKEY + key;
		var result = apiWrapper.eventSearch("source != \"" + sourceKey + "\"");
		assertTrue(result.size() > 0);
		assertEquals(0, result.stream().filter(i -> i.getSourceKey() == sourceKey).count());
		result = apiWrapper.eventSearch("event.source != \"" + sourceKey + "\"");
		assertTrue(result.size() > 0);
		assertEquals(0, result.stream().filter(i -> i.getSourceKey() == sourceKey).count());
	}

	@Test
	public void eventSourceTypeEqual() {
		var key = "eventSourceTypeEqual";
		var id = key + "_t";
		apiWrapper.addEvents(10, key);
		var events = new ArrayList<Event>();
		final var event  = Event.Builder.getDefault(id)
				.node("name")
				.source(ApiWrapper.PREFIX_SOURCE + key)
				.sourceKey(ApiWrapper.PREFIX_SOURCEKEY + key)
				.sourceType(SourceType.ZABBIX)
				.build();
		events.add(event);
		apiWrapper.eventAdd(events);
		var result = apiWrapper.eventSearch("sourceType = \"" + SourceType.ZABBIX.toString() + "\"");
		assertTrue(result.stream().filter(i -> id.equals(i.getId())).findAny().isPresent());
		result = apiWrapper.eventSearch("event.sourceType = \"" + SourceType.ZABBIX.toString() + "\"");
		assertTrue(result.stream().filter(i -> id.equals(i.getId())).findAny().isPresent());
	}

	@Test
	public void eventSourceTypeNotEqual() {
		var key = "eventSourceTypeNotEqual";
		var id = key + "_t";
		apiWrapper.addEvents(10, key);
		var events = new ArrayList<Event>();
		final var event  = Event.Builder.getDefault(id)
				.node("name")
				.source(ApiWrapper.PREFIX_SOURCE + key)
				.sourceKey(ApiWrapper.PREFIX_SOURCEKEY + key)
				.sourceType(SourceType.ZABBIX)
				.build();
		events.add(event);
		apiWrapper.eventAdd(events);
		var result = apiWrapper.eventSearch("sourceType != \"" + SourceType.ZABBIX.toString() + "\"");
		assertTrue(result.size() > 0);
		assertEquals(0, result.stream().filter(i -> i.getSourceKey() == SourceType.ZABBIX.toString()).count());
		result = apiWrapper.eventSearch("event.sourceType != \"" + SourceType.ZABBIX.toString() + "\"");
		assertTrue(result.size() > 0);
		assertEquals(0, result.stream().filter(i -> i.getSourceKey() == SourceType.ZABBIX.toString()).count());
	}

	@Test
	public void eventStatus() throws InterruptedException {
		var key = "eventStatus";
		apiWrapper.addEvents(10, key);
		apiWrapper.addItems(10, key);
		
		var itemRule = new ItemFilter(null, false, Collections.singletonMap("filter", key));
		Map<String, ItemFilter> filters = new HashMap<>();
		filters.put("filter_" + key, itemRule);
		
		var items = new ArrayList<Item>();
		var id = key + "_t";
		final var item = Item.Builder.getDefault(id)
				.name("name")
				.source(ApiWrapper.PREFIX_SOURCE + key)
				.sourceKey(ApiWrapper.PREFIX_SOURCEKEY + key)
				.filters(filters)
				.build();
		items.add(item);
		apiWrapper.itemAdd(items);
		
		var events = new ArrayList<Event>();
		final var event = new Event
				.Builder(key + "_t")
				.node("name")
				.source(ApiWrapper.PREFIX_SOURCE + key)
				.sourceKey(ApiWrapper.PREFIX_SOURCEKEY + key)
				.status(BaseStatus.WARNING)
				.fields(Collections.singletonMap("filter", key))
				.build();
		events.add(event);
		apiWrapper.eventAdd(events);
	
		List<Event> result = Collections.emptyList();
		for (int i = 0; i < 3; i++) {
			result = apiWrapper.eventSearch("source = \"src_" + key + "\" and status = " + BaseStatus.WARNING.toString());
			if(result.size() == 1) {
				break;
			}
			Thread.sleep(1000);
		}
		assertEquals(1, result.size());
		assertEquals(id, result.get(0).getId());
		result = apiWrapper.eventSearch("source = \"src_" + key + "\" and status != " + BaseStatus.WARNING.toString());
		assertEquals(10, result.size());
		assertEquals(0, result.stream().filter(i -> i.getStatus() == BaseStatus.WARNING).count());

	}

	@Test
	public void eventAndItemLink() throws InterruptedException {
		var key = "eventAndItemLink";
		apiWrapper.addEvents(10, key);
		apiWrapper.addItems(10, key);
		
		var itemRule = new ItemFilter(null, false, Collections.singletonMap("filter", key));
		Map<String, ItemFilter> filters = new HashMap<>();
		filters.put("filter_" + key, itemRule);
		
		var items = new ArrayList<Item>();
		var itemId = key + "_t";
		final var item = Item.Builder.getDefault(itemId)
				.name("name")
				.source(ApiWrapper.PREFIX_SOURCE + key)
				.sourceKey(ApiWrapper.PREFIX_SOURCEKEY + key)
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
			result = apiWrapper.eventSearch("item.source = \"src_" + key + "\" and calculated = true");
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
				.source(ApiWrapper.PREFIX_SOURCE + key)
				.sourceKey(ApiWrapper.PREFIX_SOURCEKEY + key)
				.status(BaseStatus.WARNING)
				.fields(Collections.singletonMap(key + "_name", key + "_value"))
				.build();
		events.add(event);
		event = new Event
				.Builder(id2)
				.node("name")
				.source(ApiWrapper.PREFIX_SOURCE + key)
				.sourceKey(ApiWrapper.PREFIX_SOURCEKEY + key)
				.status(BaseStatus.WARNING)
				.fields(Collections.singletonMap(key + "_name", key + "_value1"))
				.build();
		events.add(event);
		apiWrapper.eventAdd(events);
		var result = apiWrapper.eventSearch("fields.\"" + key + "_name\" = \"" + key + "_value\"");
		assertTrue(result.stream().filter(i -> id1.equals(i.getId())).findAny().isPresent());
		assertEquals(0, result.stream().filter(i -> id2.equals(i.getId())).count());
		result = apiWrapper.eventSearch("event.fields.\"" + key + "_name\" = \"" + key + "_value\"");
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
				.source(ApiWrapper.PREFIX_SOURCE + key)
				.sourceKey(ApiWrapper.PREFIX_SOURCEKEY + key)
				.fromHistory(fromHistory)
				.build();
		events.add(event);
		apiWrapper.eventAdd(events);
		var result = apiWrapper.eventSearch("fromHistory = \"" + key +"_node1\""); 
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
				.source(ApiWrapper.PREFIX_SOURCE + key)
				.sourceKey(ApiWrapper.PREFIX_SOURCEKEY + key)
				.fromHistory(fromHistory)
				.build();
		events.add(event);
		new HashSet<String>();
		fromHistory = new HashSet<String>();
		fromHistory.add(key + "_node1");
		fromHistory.add(key + "_node3");
		event = Event.Builder.getDefault(id2)
				.node("node")
				.source(ApiWrapper.PREFIX_SOURCE + key)
				.sourceKey(ApiWrapper.PREFIX_SOURCEKEY + key)
				.fromHistory(fromHistory)
				.build();
		events.add(event);
		apiWrapper.eventAdd(events);
		var result = apiWrapper.eventSearch("fromHistory HAS NOT \"" + key +"_node1\" AND source = \"src_" + key + "\"");
		assertEquals(1, result.size());
		assertEquals(1, result.stream().filter(i -> id1.equals(i.getId())).count());
		result = apiWrapper.eventSearch("fromHistory HAS NOT \"" + key +"_node2\" AND source = \"src_" + key + "\"");
		assertEquals(1, result.size());
		assertEquals(1, result.stream().filter(i -> id2.equals(i.getId())).count());
	}

	@Test
	public void eventDeletedOnIsNull() {
		var key = "eventDeletedOnIsNull";
		apiWrapper.addEvents(10, key);
		var result = apiWrapper.eventSearch("deletedOn IS NULL" + " AND source = \"src_" + key + "\"");
		assertEquals(0, result.stream().filter(i -> i.getDeletedOn() != null).count());
		assertEquals(9, result.size());

	}

	@Test
	public void eventFieldsIsNull() {
		var key = "eventFieldsIsNull";
		apiWrapper.addEvents(10, key);
		var name = key + "_field01";
		var result = apiWrapper.eventSearch("fields.\"" + name + "\" IS NULL"+ " AND source = \"src_" + key + "\"");
		assertEquals(0, result.stream().filter(i -> i.getFields().keySet().contains(name)).count());
		assertEquals(9, result.size());
	}

}
