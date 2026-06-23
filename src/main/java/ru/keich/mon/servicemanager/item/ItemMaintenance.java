package ru.keich.mon.servicemanager.item;

import java.time.Instant;
import java.util.Collections;
import java.util.Set;
import java.util.function.Supplier;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import lombok.Getter;

@Getter
public class ItemMaintenance {

	public static final ItemMaintenance EMPTY = new ItemMaintenance(null);

	@Getter
	public static class AbsoluteMaintenance {
		private final Instant startsOn;
		private final Instant endsOn;
		
		@JsonCreator
		public AbsoluteMaintenance(
				@JsonProperty(value = "startsOn", required = true) Instant startsOn,
				@JsonProperty(value = "endsOn", required = true) Instant endsOn
				) {
			this.startsOn = startsOn;
			this.endsOn = endsOn;
		}

		@Override
		public String toString() {
			return "AbsoluteMaintenance [startsOn=" + startsOn + ", endsOn=" + endsOn + "]";
		}

		public boolean test() {
			var now = Instant.now();
			return now.isAfter(startsOn) && now.isBefore(endsOn);
		}

	}

	@JsonIgnore
	private final Supplier<Boolean> func;
	private final AbsoluteMaintenance absolute;	
	@JsonIgnore
	private final Set<Object> absoluteStartsOnForIndex;
	@JsonIgnore
	private final Set<Object> absoluteEndsOnForIndex;

	@JsonCreator
	public ItemMaintenance(
			@JsonProperty(value = "absolute", required = false) AbsoluteMaintenance absolute
			) {
		this.absolute = absolute;
		if(absolute != null) {
			func = () -> absolute.test();
			absoluteStartsOnForIndex = Collections.singleton(absolute.getStartsOn());
			absoluteEndsOnForIndex = Collections.singleton(absolute.getEndsOn());
			return;
		}
		func = () -> false;
		absoluteStartsOnForIndex = Collections.emptySet();
		absoluteEndsOnForIndex = Collections.emptySet();
	}

	@Override
	public String toString() {
		return "ItemMaintenance [absolute=" + absolute + "]";
	}
	
	public boolean test() {
		return func.get();
	}

	public static Set<Object> getAbsoluteStartsOnForIndex(ItemMaintenance maintenance) {
		return maintenance.getAbsoluteStartsOnForIndex();
	}

	public static Set<Object> getAbsoluteEndsOnForIndex(ItemMaintenance maintenance) {
		return maintenance.getAbsoluteEndsOnForIndex();
	}

}
