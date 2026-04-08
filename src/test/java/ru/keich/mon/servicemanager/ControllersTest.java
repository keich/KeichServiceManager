package ru.keich.mon.servicemanager;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.util.MultiValueMap;

import ru.keich.mon.servicemanager.entity.Entity;
import ru.keich.mon.servicemanager.event.Event;
import ru.keich.mon.servicemanager.item.Item;
import ru.keich.mon.servicemanager.item.ItemFilter;
import ru.keich.mon.servicemanager.item.ItemMaintenance;
import ru.keich.mon.servicemanager.item.ItemService;


@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
public class ControllersTest {
	@Autowired  
	ItemService itemService;

	@Autowired
	public ApiWrapper apiWrapper;

	@Test
	public void itemAddAndGet() {
		var key = "itemAddAndGet";
		apiWrapper.addItems(10, key);
		var id = key + "_4";
		var name = ApiWrapper.PREFIX_NAME + "4";
		var source = ApiWrapper.PREFIX_SOURCE + key;
		var idsourceKey = ApiWrapper.PREFIX_SOURCEKEY + key;
		var item = apiWrapper.itemGet(id);
		assertEquals(id, item.getId());
		assertEquals(name, item.getName());
		assertEquals(source, item.getSource());
		assertEquals(idsourceKey, item.getSourceKey());
	}

	@Test
	public void itemGetChildren() {
		var key = "itemGetChildren";
		
		var child = new Item.Builder(key + "_child")
				.source(ApiWrapper.PREFIX_SOURCE + key)
				.sourceKey(ApiWrapper.PREFIX_SOURCEKEY + key)
				.name("name1")
				.eventsStatus(Collections.emptyMap())
				.build();
		
		var parent = new Item.Builder(key + "_parent")
				.source(ApiWrapper.PREFIX_SOURCE + key)
				.sourceKey(ApiWrapper.PREFIX_SOURCEKEY + key)
				.name("name1")
				.childrenIds(Set.of(child.getId(), "testid"))
				.eventsStatus(Collections.emptyMap())
				.build();

		apiWrapper.itemAdd(List.of(child, parent));
		var result = apiWrapper.itemGetChildren(parent.getId());
		assertEquals(1, result.size());
		assertEquals(child.getId(), result.get(0).getId());
	}

	@Test
	public void itemVersionFilter() {
		var key = "itemVersionFilter";
		var source = ApiWrapper.PREFIX_SOURCE + key;
		apiWrapper.addItems(10, key);
		var reqParam = Map.of(Entity.FIELD_SOURCE, Collections.singletonList("EQ:" + source));
		var items = apiWrapper.itemSearch(MultiValueMap.fromMultiValue(reqParam));
		long maxVersion = 0L;
		for (Item item : items) {
			if(maxVersion < item.getVersion()) {
				maxVersion = item.getVersion();
			}
		}
		var reqParam1 = Map.of(Entity.FIELD_SOURCE, Collections.singletonList("EQ:" + source), Entity.FIELD_VERSION, Collections.singletonList("GE:" + maxVersion));
		var items1 = apiWrapper.itemSearch(MultiValueMap.fromMultiValue(reqParam1));
		assertEquals(1, items1.size());
	}

	@Test
	public void itemSourceFilter() {
		var key = "itemSourceFilter";
		var source = ApiWrapper.PREFIX_SOURCE + key;
		apiWrapper.addItems(10, key);
		apiWrapper.addItems(10, key + "_");
		var reqParam = Map.of(Entity.FIELD_SOURCE, Collections.singletonList("EQ:" + source));
		var items = apiWrapper.itemSearch(MultiValueMap.fromMultiValue(reqParam));
		assertEquals(10, items.size());
		assertEquals(0, items.stream().filter(i -> !source.equals(i.getSource())).count());
	}
	
	@Test
	public void itemSourceKeyFilter() {
		var key = "itemSourceKeyFilter";
		var sourceKey = ApiWrapper.PREFIX_SOURCEKEY + key;
		apiWrapper.addItems(10, key);
		apiWrapper.addItems(10, key + "_");
		var reqParam = Map.of(Entity.FIELD_SOURCEKEY, Collections.singletonList("EQ:" + sourceKey));
		var items = apiWrapper.itemSearch(MultiValueMap.fromMultiValue(reqParam));
		assertEquals(10, items.size());
		assertEquals(0, items.stream().filter(i -> !sourceKey.equals(i.getSourceKey())).count());
	}

	@Test
	public void itemFiltersEventMapping()  throws InterruptedException {
		var key = "itemFiltersEventMapping";
		var identity = key;
		var filter = new ItemFilter(BaseStatus.INDETERMINATE, false, Map.of("identity", identity));
		var child = new Item.Builder(key + "_child")
				.source(ApiWrapper.PREFIX_SOURCE + key)
				.sourceKey(ApiWrapper.PREFIX_SOURCEKEY + key)
				.name("name1")
				.eventsStatus(Collections.emptyMap())
				.filters(Map.of("by_identity",filter))
				.build();
		
		var parent = new Item.Builder(key + "_parent")
				.source(ApiWrapper.PREFIX_SOURCE + key)
				.sourceKey(ApiWrapper.PREFIX_SOURCEKEY + key)
				.name("name1")
				.childrenIds(Set.of(child.getId()))
				.eventsStatus(Collections.emptyMap())
				.build();

		apiWrapper.itemAdd(List.of(child, parent));
		
		var event1 = new Event.Builder(key + "_event1")
				.source(ApiWrapper.PREFIX_SOURCE + key)
				.sourceKey(ApiWrapper.PREFIX_SOURCEKEY + key)
				.node("node1")
				.fields(Map.of("identity", identity))
				.status(BaseStatus.WARNING)
				.build();
		apiWrapper.eventAdd(List.of(event1));
		
		Thread.sleep(1000);
		
		var events = apiWrapper.itemGetEvents(parent.getId());
		
		assertEquals(1, events.size());
		assertEquals(BaseStatus.WARNING, apiWrapper.itemGet(parent.getId()).getStatus());
		assertEquals(BaseStatus.WARNING, apiWrapper.itemGet(child.getId()).getStatus());
		
		
		var event2 = new Event.Builder(key + "_event2")
				.source(ApiWrapper.PREFIX_SOURCE + key)
				.sourceKey(ApiWrapper.PREFIX_SOURCEKEY + key)
				.node("node1")
				.fields(Map.of("identity", identity))
				.status(BaseStatus.CRITICAL)
				.build();
		apiWrapper.eventAdd(List.of(event2));
		
		Thread.sleep(1000);
		
		events = apiWrapper.itemGetEvents(parent.getId());
		
		assertEquals(2, events.size());
		assertEquals(BaseStatus.CRITICAL, apiWrapper.itemGet(parent.getId()).getStatus());
		assertEquals(BaseStatus.CRITICAL, apiWrapper.itemGet(child.getId()).getStatus());
		
		var event3 = new Event.Builder(key + "_event3")
				.source(ApiWrapper.PREFIX_SOURCE + key)
				.sourceKey(ApiWrapper.PREFIX_SOURCEKEY + key)
				.node("node1")
				.fields(Map.of("identity", identity))
				.status(BaseStatus.CLEAR)
				.build();
		apiWrapper.eventAdd(List.of(event3));
		
		Thread.sleep(1000);
		
		events = apiWrapper.itemGetEvents(parent.getId());
		
		assertEquals(3, events.size());
		assertEquals(BaseStatus.CRITICAL, apiWrapper.itemGet(parent.getId()).getStatus());
		assertEquals(BaseStatus.CRITICAL, apiWrapper.itemGet(child.getId()).getStatus());
		
		apiWrapper.eventDel(List.of(event1.getId(), event2.getId(), event3.getId()));
		
		Thread.sleep(1000);
		
		events = apiWrapper.itemGetEvents(parent.getId());
		
		assertEquals(0, events.size());
		assertEquals(BaseStatus.CLEAR, apiWrapper.itemGet(parent.getId()).getStatus());
		assertEquals(BaseStatus.CLEAR, apiWrapper.itemGet(child.getId()).getStatus());
	}		

	@Test
	public void itemEventClear()  throws InterruptedException {
		var key = "itemEventClear";
		var identity = key;
		var filter = new ItemFilter(BaseStatus.INDETERMINATE, false, Map.of("identity", identity));
		var item = new Item.Builder(key + "_id")
				.source(ApiWrapper.PREFIX_SOURCE + key)
				.sourceKey(ApiWrapper.PREFIX_SOURCEKEY + key)
				.name("name1")
				.eventsStatus(Collections.emptyMap())
				.filters(Map.of("by_identity", filter))
				.build();
		apiWrapper.itemAdd(List.of(item));
		var event1 = new Event.Builder(key + "_event1")
				.source(ApiWrapper.PREFIX_SOURCE + key)
				.sourceKey(ApiWrapper.PREFIX_SOURCEKEY + key)
				.node("node1")
				.fields(Map.of("identity", identity))
				.status(BaseStatus.WARNING)
				.build();
		apiWrapper.eventAdd(List.of(event1));
		
		Thread.sleep(1000);
		
		var events = apiWrapper.itemGetEvents(item.getId());
		
		assertEquals(1, events.size());
		assertEquals(BaseStatus.WARNING, apiWrapper.itemGet(item.getId()).getStatus());
		
		var event2 = new Event.Builder(event1)
				.status(BaseStatus.CLEAR)
				.build();
		apiWrapper.eventAdd(List.of(event2));
		
		Thread.sleep(1000);
		
		events = apiWrapper.itemGetEvents(item.getId());
		
		assertEquals(1, events.size());
		assertEquals(BaseStatus.CLEAR, apiWrapper.itemGet(item.getId()).getStatus());
	}

	@Test
	public void itemEventDeleted()  throws InterruptedException {
		var key = "itemEventDeleted";
		var identity = key;
		var filter = new ItemFilter(BaseStatus.INDETERMINATE, false, Map.of("identity", identity));
		var item = new Item.Builder(key + "_id")
				.source(ApiWrapper.PREFIX_SOURCE + key)
				.sourceKey(ApiWrapper.PREFIX_SOURCEKEY + key)
				.name("name1")
				.eventsStatus(Collections.emptyMap())
				.filters(Map.of("by_identity", filter))
				.build();
		apiWrapper.itemAdd(List.of(item));
		var event1 = new Event.Builder(key + "_event1")
				.source(ApiWrapper.PREFIX_SOURCE + key)
				.sourceKey(ApiWrapper.PREFIX_SOURCEKEY + key)
				.node("node1")
				.fields(Map.of("identity", identity))
				.status(BaseStatus.WARNING)
				.build();
		apiWrapper.eventAdd(List.of(event1));
		
		Thread.sleep(1000);
		
		var events = apiWrapper.itemGetEvents(item.getId());
		
		assertEquals(1, events.size());
		assertEquals(BaseStatus.WARNING, apiWrapper.itemGet(item.getId()).getStatus());
		
		apiWrapper.eventDel(List.of(event1.getId()));
		
		Thread.sleep(1000);
		
		events = apiWrapper.itemGetEvents(item.getId());
		
		assertEquals(0, events.size());
		assertEquals(BaseStatus.CLEAR, apiWrapper.itemGet(item.getId()).getStatus());
	}

	@Test
	public void eventAddAndGet() {
		var key = "eventAddAndGet";
		apiWrapper.addEvents(10, key);
		var id = key + "_4";
		var node = ApiWrapper.PREFIX_NODE + "4";
		var source = ApiWrapper.PREFIX_SOURCE + key;
		var idsourceKey = ApiWrapper.PREFIX_SOURCEKEY + key;
		var event = apiWrapper.eventGet(id);
		assertEquals(id, event.getId());
		assertEquals(node, event.getNode());
		assertEquals(source, event.getSource());
		assertEquals(idsourceKey, event.getSourceKey());
	}

	@Test
	public void eventVersionFilter() {
		var key = "eventVersionFilter";
		var source = ApiWrapper.PREFIX_SOURCE + key;
		apiWrapper.addEvents(10, key);
		var reqParam = Map.of(Entity.FIELD_SOURCE, Collections.singletonList("EQ:" + source));
		var events = apiWrapper.eventSearch(MultiValueMap.fromMultiValue(reqParam));
		long maxVersion = 0L;
		for (Event event : events) {
			if(maxVersion < event.getVersion()) {
				maxVersion = event.getVersion();
			}
		}
		var reqParam1 = Map.of(Entity.FIELD_SOURCE, Collections.singletonList("EQ:" + source), Entity.FIELD_VERSION, Collections.singletonList("GE:" + maxVersion));
		events = apiWrapper.eventSearch(MultiValueMap.fromMultiValue(reqParam1));
		assertEquals(1, events.size());
	}

	@Test
	public void eventSourceFilter() {
		var key = "eventSourceFilter";
		var source = ApiWrapper.PREFIX_SOURCE + key;
		apiWrapper.addEvents(10, key);
		apiWrapper.addEvents(10, key + "_");
		var reqParam = Map.of(Entity.FIELD_SOURCE, Collections.singletonList("EQ:" + source));
		var events = apiWrapper.eventSearch(MultiValueMap.fromMultiValue(reqParam));
		assertEquals(10, events.size());
		assertEquals(0, events.stream().filter(i -> !source.equals(i.getSource())).count());
	}

	@Test
	public void eventSourceKeyFilter() {
		var key = "eventSourceKeyFilter";
		var sourceKey = ApiWrapper.PREFIX_SOURCEKEY + key;
		apiWrapper.addEvents(10, key);
		apiWrapper.addEvents(10, key + "_");
		var reqParam = Map.of(Entity.FIELD_SOURCEKEY, Collections.singletonList("EQ:" + sourceKey));
		var events = apiWrapper.eventSearch(MultiValueMap.fromMultiValue(reqParam));
		assertEquals(10, events.size());
		assertEquals(0, events.stream().filter(i -> !sourceKey.equals(i.getSourceKey())).count());
	}

	@Test
	public void itemTree()  throws InterruptedException {
		var key = "itemTree";
		var child = new Item.Builder(key + "_child")
				.source(ApiWrapper.PREFIX_SOURCE + key)
				.sourceKey(ApiWrapper.PREFIX_SOURCEKEY + key)
				.name("name1")
				.eventsStatus(Collections.emptyMap())
				.build();
		
		var parent = new Item.Builder(key + "_parent")
				.source(ApiWrapper.PREFIX_SOURCE + key)
				.sourceKey(ApiWrapper.PREFIX_SOURCEKEY + key)
				.name("name1")
				.childrenIds(Set.of(child.getId(), "testid"))
				.eventsStatus(Collections.emptyMap())
				.build();

		apiWrapper.itemAdd(List.of(child, parent));
		var root = apiWrapper.itemGetTree(parent.getId());
		assertEquals(1, root.getChildren().size());
		assertEquals(child.getId(), root.getChildren().get(0).getId());
	}
	
	@Test
	public void updateItemAndKeepStatus()  throws InterruptedException {
		var key = "updateItemAndKeepStatus";
		var identity = key;
		var filter = new ItemFilter(BaseStatus.INDETERMINATE, false, Map.of("identity", identity));
		var item = new Item.Builder(key + "_id")
				.source(ApiWrapper.PREFIX_SOURCE + key)
				.sourceKey(ApiWrapper.PREFIX_SOURCEKEY + key)
				.name("name1")
				.eventsStatus(Collections.emptyMap())
				.filters(Map.of("by_identity", filter))
				.build();
		apiWrapper.itemAdd(List.of(item));
		var event1 = new Event.Builder(key + "_event1")
				.source(ApiWrapper.PREFIX_SOURCE + key)
				.sourceKey(ApiWrapper.PREFIX_SOURCEKEY + key)
				.node("node1")
				.fields(Map.of("identity", identity))
				.status(BaseStatus.WARNING)
				.build();
		apiWrapper.eventAdd(List.of(event1));
		
		Thread.sleep(1000);
		
		assertEquals(BaseStatus.WARNING, apiWrapper.itemGet(item.getId()).getStatus());
		apiWrapper.itemAdd(List.of(item));
		assertEquals(BaseStatus.WARNING, apiWrapper.itemGet(item.getId()).getStatus());
	}

	@Test
	public void itemSortAndLimit()  throws InterruptedException {
		var key = "itemSortAndLimit";
		var source = ApiWrapper.PREFIX_SOURCE + key;
		int size = 10;
		apiWrapper.addItems(size, key);
		var reqParam = new HashMap<String, List<String>>();
		reqParam.put(Entity.FIELD_SOURCE, Collections.singletonList("EQ:"+source));
		reqParam.put(Item.FIELD_NAME, Collections.singletonList("sort:1"));
		var items = apiWrapper.itemSearch(MultiValueMap.fromMultiValue(reqParam));
		assertEquals(10, items.size());
		for(int i = 0; i < size; i++) {
			assertEquals(ApiWrapper.PREFIX_NAME + i, items.get(i).getName());
		}
	}

	@Test
	public void eventDeleteLogic() {	
		var key = "eventDeleteLogic";
		var item = new Item.Builder(key + "_id")
				.source(ApiWrapper.PREFIX_SOURCE + key)
				.sourceKey(ApiWrapper.PREFIX_SOURCEKEY + key)
				.name("name1")
				.eventsStatus(Collections.emptyMap())
				.deletedOn(Instant.now())
				.build();
		apiWrapper.itemAdd(List.of(item));
		assertNotNull(apiWrapper.itemGet(item.getId()).getDeletedOn());
		var item1 = new Item.Builder(item)
				.deletedOn(null)
				.build();
		apiWrapper.itemAdd(List.of(item1));
		assertNull(apiWrapper.itemGet(item.getId()).getDeletedOn());
	}

	@Test
	public void itemMaintenanceAbsolute() throws InterruptedException {	
		var key = "itemMaintenanceAbsolute";
		var identity = key;
		var filter = new ItemFilter(BaseStatus.INDETERMINATE, false, Map.of("identity", identity));
		var startsOn = Instant.now().minus(1, ChronoUnit.HOURS);
		var endsOn = Instant.now().plus(1, ChronoUnit.HOURS);
		var absolute = new ItemMaintenance.AbsoluteMaintenance(startsOn, endsOn);
		var maintenance = new ItemMaintenance(absolute);
		var item = new Item.Builder(key + "_id")
				.source(ApiWrapper.PREFIX_SOURCE + key)
				.sourceKey(ApiWrapper.PREFIX_SOURCEKEY + key)
				.name("name1")
				.eventsStatus(Collections.emptyMap())
				.maintenance(maintenance)
				.filters(Map.of("by_identity", filter))
				.build();
		apiWrapper.itemAdd(List.of(item));
		
		var event = new Event.Builder(key + "_event1")
				.source(ApiWrapper.PREFIX_SOURCE + key)
				.sourceKey(ApiWrapper.PREFIX_SOURCEKEY + key)
				.node("node1")
				.fields(Map.of("identity", identity))
				.status(BaseStatus.WARNING)
				.build();
		apiWrapper.eventAdd(List.of(event));
		
		Thread.sleep(1000);
		var result = apiWrapper.itemGet(item.getId());
		assertEquals(BaseStatus.CLEAR, result.getStatus());
		assertTrue(result.isMaintenanceOn());
	}

	@Test
	public void eventCalculated() throws IOException, InterruptedException {	
		var key = "eventCalculated";
		var event = new Event.Builder(key + "_event1")
				.source(ApiWrapper.PREFIX_SOURCE + key)
				.sourceKey(ApiWrapper.PREFIX_SOURCEKEY + key)
				.node("node1")
				.status(BaseStatus.WARNING)
				.build();
		apiWrapper.eventAdd(List.of(event));
		
		Thread.sleep(1000);
		var result = apiWrapper.eventGet(event.getId());
		assertTrue(result.getCalculated());
	}

	// TODO test update not clear internal fields
	// TODO search test
	// TODO check tree

}
