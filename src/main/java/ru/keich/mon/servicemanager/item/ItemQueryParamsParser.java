package ru.keich.mon.servicemanager.item;

import java.util.HashSet;
import java.util.Set;

import org.springframework.util.MultiValueMap;

import lombok.Getter;
import ru.keich.mon.servicemanager.entity.EntityQueryParamsParser;

@Getter
public class ItemQueryParamsParser extends EntityQueryParamsParser {
	private final boolean needEvents;
	
	public ItemQueryParamsParser(MultiValueMap<String, String> reqParam) {
		super(reqParam);
		if (this.getProperties().contains(Item.FIELD_EVENTS)) {
			this.needEvents = true;
		} else {
			this.needEvents = false;
		}
	}

	public Set<String> getPropertiesWith(String name) {
		if (!this.getProperties().isEmpty()) {
			var ret = new HashSet<String>(this.getProperties());
			ret.add(name);
			return ret;
		}
		return this.getProperties();
	}
}
