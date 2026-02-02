package ru.keich.mon.servicemanager;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;

import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import ru.keich.mon.servicemanager.event.Event;
import ru.keich.mon.servicemanager.item.Item;

public class ApiWrapper {

	private final TestRestTemplate restTemplate;
	private final ObjectMapper mapper;
	
	public ApiWrapper(TestRestTemplate restTemplate) {
		this.restTemplate =restTemplate;
		mapper = new ObjectMapper();
		mapper.registerModule(new JavaTimeModule());
	}

	private <T> void entityAdd(String path, List<T> entities) {
		var result = restTemplate.postForEntity("/api/v1" + path, entities, String.class);
		assertEquals(HttpStatus.OK, result.getStatusCode());
	}
	
	private <T> void entityDel(String path, List<String> ids) throws JsonProcessingException {
		var requestJson = mapper.writeValueAsString(ids);
		var headers = new HttpHeaders();
		headers.setContentType(MediaType.APPLICATION_JSON);
		var entity = new HttpEntity<String>(requestJson, headers);
		var result = restTemplate.exchange("/api/v1" + path, HttpMethod.DELETE, entity, String.class);
		assertEquals(HttpStatus.OK, result.getStatusCode());
	}
	
	private <T> List<T> entitySearch(String path, String search, ParameterizedTypeReference<List<T>> responseType) {
		var result = restTemplate.exchange("/api/v1/" + path + "?search=" + search,
				HttpMethod.GET, null, responseType);
		assertEquals(HttpStatus.OK, result.getStatusCode());
		return result.getBody();
	}
	
	public void itemAdd(List<Item> items) {
		entityAdd("/item", items);
	}
	
	public List<Item> itemSeach(String search) {
		return entitySearch("/item", search, new ParameterizedTypeReference<List<Item>>() {});
	}
	
	public void itemDel(List<String> ids) throws JsonProcessingException {
		entityDel("/item", ids);
	}
	
	public void eventAdd( List<Event> items) {
		entityAdd("/event", items);
	}
	
	public List<Event> eventSeach(String search) {
		return entitySearch("/event", search, new ParameterizedTypeReference<List<Event>>() {});
	}

}
