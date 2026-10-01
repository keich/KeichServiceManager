package ru.keich.mon.servicemanager.item;

import java.util.function.Consumer;

import org.springframework.web.reactive.function.client.WebClient;

import lombok.extern.java.Log;
import ru.keich.mon.servicemanager.entity.EntityReplication;

@Log
public class ItemReplication extends EntityReplication<Item> {

	public ItemReplication(WebClient webClient, String nodeName, Consumer<Item> consumer) throws IllegalArgumentException, IllegalAccessException, NoSuchFieldException {
		super(webClient, nodeName, "/api/v1/item", Item.class, consumer, log);
	}

}
