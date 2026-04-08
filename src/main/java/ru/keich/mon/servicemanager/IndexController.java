package ru.keich.mon.servicemanager;

import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;

@Controller
public class IndexController {

	@RequestMapping(value = "/{id}")
	@CrossOrigin(origins = "*")
	public String index(@PathVariable("id") String id) {
		return "forward:/static/index.html";
	}

}
