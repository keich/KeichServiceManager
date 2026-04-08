package ru.keich.mon.servicemanager;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.io.IOException;
import java.time.Instant;
import java.util.Collections;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import ru.keich.mon.servicemanager.alert.Alert;
import tools.jackson.databind.json.JsonMapper;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
public class AlertsTest {

	@Autowired
	public ApiWrapper apiWrapper;

	@Autowired
	JsonMapper mapper;

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
		var startAt = Instant.parse("2024-11-06T10:31:00+03:00");
		Alert alert = mapper.readValue(json, Alert.class);
		alert.setEndsAt(Instant.now().plusSeconds(60).toString());
		apiWrapper.alertAdd(Collections.singletonList(alert));
		var event = apiWrapper.eventGet("7380901136023483174_" + startAt.getEpochSecond());
		assertEquals(BaseStatus.WARNING, event.getStatus());
		assertNotNull(event.getEndsOn());
	}

}
