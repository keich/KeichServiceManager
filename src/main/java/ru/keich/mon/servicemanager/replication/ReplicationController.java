package ru.keich.mon.servicemanager.replication;

import org.springframework.util.MultiValueMap;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import lombok.extern.java.Log;

@RestController
@RequestMapping("/api/v1")
@Log
public class ReplicationController {

	private final ReplicationService replicationService;

	public ReplicationController(ReplicationService replicationService) {
		this.replicationService = replicationService;
	}

	@GetMapping(value = "/replication/status", produces = "application/json")
	@CrossOrigin(origins = "*")
	public ReplicationStatus find(@RequestParam MultiValueMap<String, String> reqParam) {
		return replicationService.getStatus();
	}

}
