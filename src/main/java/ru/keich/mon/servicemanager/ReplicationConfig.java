package ru.keich.mon.servicemanager;

import javax.net.ssl.SSLException;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import ru.keich.mon.servicemanager.entity.EntityReplication;
import ru.keich.mon.servicemanager.event.Event;
import ru.keich.mon.servicemanager.event.EventService;
import ru.keich.mon.servicemanager.item.Item;
import ru.keich.mon.servicemanager.item.ItemService;
import ru.keich.mon.servicemanager.replication.Replication;

@Configuration
public class ReplicationConfig {

	@Bean
	@ConditionalOnProperty(name = "replication.neighbor")
	Replication createReplication(EventService eventService, ItemService itemService,
			@Value("${replication.nodename}") String nodeName,
			@Value("${replication.neighbor}") String replicationNeighbor) throws SSLException {
		var eventReplication = new EntityReplication<String, Event>(eventService, nodeName, replicationNeighbor,
				"/api/v1/event", Event.class);
		var itemReplication = new EntityReplication<String, Item>(itemService, nodeName, replicationNeighbor,
				"/api/v1/item", Item.class);
		return new Replication(eventReplication, itemReplication);
	}

}
