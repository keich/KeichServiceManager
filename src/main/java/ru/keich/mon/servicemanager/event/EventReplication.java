package ru.keich.mon.servicemanager.event;

import java.util.function.Consumer;

import org.springframework.web.reactive.function.client.WebClient;

import lombok.extern.java.Log;
import ru.keich.mon.servicemanager.entity.EntityReplication;

@Log
public class EventReplication extends EntityReplication<Event> { 

	public EventReplication(WebClient webClient, String nodeName, Consumer<Event> consumer) throws IllegalArgumentException, IllegalAccessException, NoSuchFieldException {
		super(webClient, nodeName, "/api/v1/event", Event.class, consumer, log);
	}

}
