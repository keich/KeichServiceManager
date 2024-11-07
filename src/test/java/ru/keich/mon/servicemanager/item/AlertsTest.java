package ru.keich.mon.servicemanager.item;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;

import com.fasterxml.jackson.databind.ObjectMapper;

import ru.keich.mon.servicemanager.BaseStatus;
import ru.keich.mon.servicemanager.alert.Alert;
import ru.keich.mon.servicemanager.event.Event;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
public class AlertsTest {
	ObjectMapper mapper = new ObjectMapper();

	@Autowired
	private TestRestTemplate restTemplate;
	
	private void alertAdd(Alert alert) {
		var headers = new HttpHeaders();
		headers.setContentType(MediaType.APPLICATION_JSON);
		var list = new ArrayList<Alert>();
		list.add(alert);
		var result = restTemplate.postForEntity("/api/v2/alerts", list, String.class);
		assertEquals(HttpStatus.OK, result.getStatusCode());
	}
	
	private Event eventGet(String id) {
		var result = restTemplate.getForEntity("/api/v1/event/" + id , Event.class);
		assertEquals(HttpStatus.OK, result.getStatusCode());
		return result.getBody();
	}
	
	@Test
	public void alertPut() throws IOException {
		String json = """
				{"startsAt": "2024-11-06T10:31:00+03:00",
					 "generatorURL": "http://localhost:8888/vmalert/alert?group_id=11500128490285070060&alert_id=7380901136023483174",
					 "endsAt":"2024-11-07T10:46:37.417675131+03:00",
					 "labels":{
						"alertgroup":"test",
						"alertname":"probeHttpStatusCodeNe200",
						"job":"blackbox",
						"node":"srv1.example.com"},
					 "annotations":{ "description":"Target http://srv1.example.com:80/ping returns http status 0",
						"severity":"3",
						"node":"srv1.example.com",
						"alert_id": 7380901136023483174}
					}
				""";
		Alert alert = mapper.readValue(json, Alert.class);
		alert.setEndsAt(Instant.now().plusSeconds(60).toString());
		alertAdd(alert);
		var event = eventGet("7380901136023483174");
		assertEquals(BaseStatus.WARNING, event.getStatus());
		assertNotNull(event.getEndsOn());
	}

}
