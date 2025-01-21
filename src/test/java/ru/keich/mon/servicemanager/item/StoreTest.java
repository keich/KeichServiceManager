package ru.keich.mon.servicemanager.item;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Instant;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Test;

import lombok.Getter;
import lombok.Setter;
import ru.keich.mon.servicemanager.SourceType;
import ru.keich.mon.servicemanager.entity.Entity;
import ru.keich.mon.servicemanager.query.predicates.Predicates;
import ru.keich.mon.servicemanager.query.predicates.QueryPredicate;
import ru.keich.mon.servicemanager.store.IndexedHashMap;
import ru.keich.mon.servicemanager.store.IndexedHashMap.IndexType;

public class StoreTest {
	public static final SourceType SOURCETYPE = SourceType.OTHER;
	
	@Getter
	@Setter
	static public class TestEntity extends Entity<String> {
		
		
		public static final String FIELD_NAME = "name";
		public static final String FIELD_SOMESET = "someSet";

		public TestEntity(String id, Long version, String source, String sourceKey, SourceType sourceType, Map<String, String> fields,
				Set<String> fromHistory, Instant createdOn, Instant updatedOn, Instant deletedOn) {
			super(id, version, source, sourceKey, sourceType, fields, fromHistory, createdOn, updatedOn, deletedOn);
		}

		String name;
		
		private Set<String> someSet;
		
		public static Set<Object> getNameUpperCaseForIndex(TestEntity e) {
			return  Optional.ofNullable(e.getName())
					.map(s -> Collections.singleton((Object)s))
					.orElse(Collections.emptySet());
		}
		
		public static Set<Object> getSomeSetForIndex(TestEntity e) {
			return e.getSomeSet().stream().collect(Collectors.toSet());
		}
		
		public static boolean testSomeSet(TestEntity e, QueryPredicate predicate) {
			return predicate.test(e.getSomeSet());
		}
		
		public static boolean testNameUpperCase(TestEntity e, QueryPredicate predicate) {
			return predicate.test(e.getName());
		}
		
		public static boolean testSource(TestEntity e, QueryPredicate predicate) {
			return predicate.test(e.getSource());
		}
		
		public static boolean testSourceKey(TestEntity e, QueryPredicate predicate) {
			return predicate.test(e.getSourceKey());
		}
		
		public static boolean testVersion(TestEntity e, QueryPredicate predicate) {
			return predicate.test(e.getVersion());
		}
		
		@Override
		public String toString() {
			return "TestEntity [name=" + name + ", someSet=" + someSet + "]";
		}
		
	}

	public void queryEqual(IndexedHashMap<String, TestEntity> store) {
		final long limit = -1;
		final var SOURCE_VALUE = "source1";
		var entity1 = new TestEntity("id1", 0L, SOURCE_VALUE, "sourceKey1", SOURCETYPE, Collections.emptyMap(),
				Collections.emptySet(), Instant.now(), Instant.now(), null);
		var entity2 = new TestEntity("id2", 0L, SOURCE_VALUE + "_", "sourceKey2", SOURCETYPE, Collections.emptyMap(),
				Collections.emptySet(), Instant.now(), Instant.now(), null);
		store.put(entity1);
		store.put(entity2);
		var repdicate = Predicates.equal(Entity.FIELD_SOURCE, SOURCE_VALUE);
		var retSet1 = store.keySet(repdicate, limit);
		assertEquals(1, retSet1.size());
		retSet1.stream().forEach(id -> {
			var retSource = store.get(id).map(e -> e.getSource()).orElse("");
			assertEquals(SOURCE_VALUE, retSource);
		});
	}

	@Test
	public void queryEqualByField() {
		var store = new IndexedHashMap<String, TestEntity>(null, this.getClass().getSimpleName());
		store.addQueryField(Entity.FIELD_SOURCE, TestEntity::testSource);
		queryEqual(store);
	}

	@Test
	public void queryEqualByIndex() {
		var store = new IndexedHashMap<String, TestEntity>(null, this.getClass().getSimpleName());
		store.addIndex(Entity.FIELD_SOURCE, IndexType.EQUAL, Entity::getSourceForIndex);
		queryEqual(store);
	}
	
	public void queryNotEqual(IndexedHashMap<String, TestEntity> store) {
		final long limit = -1;
		final var SOURCE_VALUE1 = "source1";
		final var SOURCE_VALUE2 = "source2";
		var entity1 = new TestEntity("id1", 0L, SOURCE_VALUE1, "sourceKey", SOURCETYPE, Collections.emptyMap(),
				Collections.emptySet(), Instant.now(), Instant.now(), null);
		var entity2 = new TestEntity("id2", 0L, SOURCE_VALUE2, "sourceKey", SOURCETYPE, Collections.emptyMap(),
				Collections.emptySet(), Instant.now(), Instant.now(), null);
		store.put(entity1);
		store.put(entity2);
		var repdicate = Predicates.notEqual(Entity.FIELD_SOURCE, SOURCE_VALUE1);
		var retSet1 = store.keySet(repdicate, limit);
		assertEquals(1, retSet1.size());
		retSet1.stream().forEach(id -> {
			var retSource = store.get(id).map(e -> e.getSource()).orElse("");
			assertEquals(SOURCE_VALUE2, retSource);
		});
	}

	@Test
	public void queryNotEqualByField() {
		var store = new IndexedHashMap<String, TestEntity>(null, this.getClass().getSimpleName());
		store.addQueryField(Entity.FIELD_SOURCE, TestEntity::testSource);
		queryNotEqual(store);
	}

	@Test
	public void queryNotEqualByIndex() {
		var store = new IndexedHashMap<String, TestEntity>(null, this.getClass().getSimpleName());
		store.addIndex(Entity.FIELD_SOURCE, IndexType.EQUAL, Entity::getSourceForIndex);
		queryNotEqual(store);
	}
	
	public void queryLessThan(IndexedHashMap<String, TestEntity> store) {
		final long limit = -1;
		final var SOURCE_VALUE = "source";
		final Long VERSION1 = 1L;
		final Long VERSION2 = 2L;
		var entity1 = new TestEntity("id1", VERSION1, SOURCE_VALUE, "sourceKey", SOURCETYPE, Collections.emptyMap(),
				Collections.emptySet(), Instant.now(), Instant.now(), null);
		var entity2 = new TestEntity("id2", VERSION2, SOURCE_VALUE, "sourceKey", SOURCETYPE, Collections.emptyMap(),
				Collections.emptySet(), Instant.now(), Instant.now(), null);
		store.put(entity1);
		store.put(entity2);
		var repdicate = Predicates.lessThan(Entity.FIELD_VERSION, VERSION2);
		var retSet1 = store.keySet(repdicate, limit);
		assertEquals(1, retSet1.size());
		retSet1.stream().forEach(id -> {
			var retVersion = store.get(id).map(e -> e.getVersion()).orElse(100L);
			assertTrue(retVersion < VERSION2);
		});
	}

	@Test
	public void queryLessThanByField() {
		var store = new IndexedHashMap<String, TestEntity>(null, this.getClass().getSimpleName());
		store.addQueryField(Entity.FIELD_VERSION, TestEntity::testVersion);
		queryLessThan(store);
	}

	@Test
	public void queryLessThanByIndex() {
		var store = new IndexedHashMap<String, TestEntity>(null, this.getClass().getSimpleName());
		store.addIndex(Entity.FIELD_VERSION, IndexType.UNIQ_SORTED, Entity::getVersionForIndex);
		queryLessThan(store);
	}
	
	public void queryGreaterEqual(IndexedHashMap<String, TestEntity> store) {
		final long limit = -1;
		final var SOURCE_VALUE = "source";
		final Long VERSION1 = 1L;
		final Long VERSION2 = 2L;
		final Long VERSION3 = 3L;
		var entity1 = new TestEntity("id1", VERSION1, SOURCE_VALUE, "sourceKey", SOURCETYPE, Collections.emptyMap(),
				Collections.emptySet(), Instant.now(), Instant.now(), null);
		var entity2 = new TestEntity("id2", VERSION2, SOURCE_VALUE, "sourceKey", SOURCETYPE, Collections.emptyMap(),
				Collections.emptySet(), Instant.now(), Instant.now(), null);
		var entity3 = new TestEntity("id3", VERSION3, SOURCE_VALUE, "sourceKey", SOURCETYPE, Collections.emptyMap(),
				Collections.emptySet(), Instant.now(), Instant.now(), null);
		store.put(entity1);
		store.put(entity2);
		store.put(entity3);
		var repdicate = Predicates.greaterEqual(Entity.FIELD_VERSION, VERSION2);
		var retSet1 = store.keySet(repdicate, limit);
		assertEquals(2, retSet1.size());
		retSet1.stream().forEach(id -> {
			var retVersion = store.get(id).map(e -> e.getVersion()).orElse(0L);
			assertTrue(retVersion >= VERSION2);
		});
	}

	@Test
	public void queryGreaterEqualByField() {
		var store = new IndexedHashMap<String, TestEntity>(null, this.getClass().getSimpleName());
		store.addQueryField(Entity.FIELD_VERSION, TestEntity::testVersion);
		queryGreaterEqual(store);
	}

	@Test
	public void queryGreaterEqualByIndex() {
		var store = new IndexedHashMap<String, TestEntity>(null, this.getClass().getSimpleName());
		store.addIndex(Entity.FIELD_VERSION, IndexType.UNIQ_SORTED, Entity::getVersionForIndex);
		queryGreaterEqual(store);
	}
	
	public void queryGreaterThan(IndexedHashMap<String, TestEntity> store) {
		final long limit = -1;
		final var SOURCE_VALUE = "source";
		final Long VERSION1 = 1L;
		final Long VERSION2 = 2L;
		final Long VERSION3 = 3L;
		var entity1 = new TestEntity("id1", VERSION1, SOURCE_VALUE, "sourceKey", SOURCETYPE, Collections.emptyMap(),
				Collections.emptySet(), Instant.now(), Instant.now(), null);
		var entity2 = new TestEntity("id2", VERSION2, SOURCE_VALUE, "sourceKey", SOURCETYPE, Collections.emptyMap(),
				Collections.emptySet(), Instant.now(), Instant.now(), null);
		var entity3 = new TestEntity("id3", VERSION3, SOURCE_VALUE, "sourceKey", SOURCETYPE, Collections.emptyMap(),
				Collections.emptySet(), Instant.now(), Instant.now(), null);
		store.put(entity1);
		store.put(entity2);
		store.put(entity3);
		var repdicate = Predicates.greaterThan(Entity.FIELD_VERSION, VERSION2);
		var retSet1 = store.keySet(repdicate, limit);
		assertEquals(1, retSet1.size());
		retSet1.stream().forEach(id -> {
			var retVersion = store.get(id).map(e -> e.getVersion()).orElse(0L);
			assertTrue(retVersion > VERSION2);
		});
	}

	@Test
	public void queryGreaterThanByField() {
		var store = new IndexedHashMap<String, TestEntity>(null, this.getClass().getSimpleName());
		store.addQueryField(Entity.FIELD_VERSION, TestEntity::testVersion);
		queryGreaterThan(store);
	}

	@Test
	public void queryGreaterThanByIndex() {
		var store = new IndexedHashMap<String, TestEntity>(null, this.getClass().getSimpleName());
		store.addIndex(Entity.FIELD_VERSION, IndexType.UNIQ_SORTED, Entity::getVersionForIndex);
		queryGreaterThan(store);
	}
	
	public void queryContainString(IndexedHashMap<String, TestEntity> store) {
		final long limit = -1;
		final var SOURCE_VALUE = "source";
		final var TEST_NAME = "SomeTestName";
		final var OTHER_NAME = "OtherName";
		final Long VERSION1 = 1L;
		final Long VERSION2 = 2L;
		var entity1 = new TestEntity("id1", VERSION1, SOURCE_VALUE, "sourceKey", SOURCETYPE, Collections.emptyMap(),
				Collections.emptySet(), Instant.now(), Instant.now(), null);
		entity1.setName(TEST_NAME);
		var entity2 = new TestEntity("id2", VERSION2, SOURCE_VALUE, "sourceKey", SOURCETYPE, Collections.emptyMap(),
				Collections.emptySet(), Instant.now(), Instant.now(), null);
		entity2.setName(OTHER_NAME);
		store.put(entity1);
		store.put(entity2);
		var p1 = Predicates.contain(TestEntity.FIELD_NAME, "Test");
		var retSet1 = store.keySet(p1, limit);
		assertEquals(1, retSet1.size());
		retSet1.stream().forEach(id -> {
			var retName= store.get(id).map(e -> e.getName()).orElse("");
			assertEquals(TEST_NAME, retName);
		});
	}

	@Test
	public void queryContainByField() {
		var store = new IndexedHashMap<String, TestEntity>(null, this.getClass().getSimpleName());
		store.addQueryField(TestEntity.FIELD_NAME, TestEntity::testNameUpperCase);
		queryContainString(store);
	}

	@Test
	public void queryContainByIndex() {
		var store = new IndexedHashMap<String, TestEntity>(null, this.getClass().getSimpleName());
		store.addIndex(TestEntity.FIELD_NAME, IndexType.EQUAL, TestEntity::getNameUpperCaseForIndex);
		queryContainString(store);
	}
	
	public void queryNotContainString(IndexedHashMap<String, TestEntity> store) {
		final long limit = -1;
		final var SOURCE_VALUE = "source";
		final var TEST_NAME = "SomeTestName";
		final var OTHER_NAME = "OtherName";
		final Long VERSION1 = 1L;
		final Long VERSION2 = 2L;
		var entity1 = new TestEntity("id1", VERSION1, SOURCE_VALUE, "sourceKey", SOURCETYPE, Collections.emptyMap(),
				Collections.emptySet(), Instant.now(), Instant.now(), null);
		entity1.setName(TEST_NAME);
		var entity2 = new TestEntity("id2", VERSION2, SOURCE_VALUE, "sourceKey", SOURCETYPE, Collections.emptyMap(),
				Collections.emptySet(), Instant.now(), Instant.now(), null);
		entity2.setName(OTHER_NAME);
		store.put(entity1);
		store.put(entity2);
		var p1 = Predicates.notContain(TestEntity.FIELD_NAME, "Test");
		var retSet1 = store.keySet(p1, limit);
		assertEquals(1, retSet1.size());
		retSet1.stream().forEach(id -> {
			var retName= store.get(id).map(e -> e.getName()).orElse("");
			assertEquals(OTHER_NAME, retName);
		});
	}

	@Test
	public void queryNotContainByField() {
		var store = new IndexedHashMap<String, TestEntity>(null, this.getClass().getSimpleName());
		store.addQueryField(TestEntity.FIELD_NAME, TestEntity::testNameUpperCase);
		queryNotContainString(store);
	}

	@Test
	public void queryNotContainByIndex() {
		var store = new IndexedHashMap<String, TestEntity>(null, this.getClass().getSimpleName());
		store.addIndex(TestEntity.FIELD_NAME, IndexType.EQUAL, TestEntity::getNameUpperCaseForIndex);
		queryNotContainString(store);
	}

	public void queryNotInclude(IndexedHashMap<String, TestEntity> store) {
		final long limit = -1;
		final var SOURCE_VALUE = "source";
		final var TEST_NAME = "SomeTestName";
		final var OTHER_NAME = "OtherName";
		final Long VERSION1 = 1L;
		final Long VERSION2 = 2L;
		var entity1 = new TestEntity("id1", VERSION1, SOURCE_VALUE, "sourceKey", SOURCETYPE, Collections.emptyMap(),
				Collections.emptySet(), Instant.now(), Instant.now(), null);
		var s1 = new HashSet<String>();
		s1.add("Test1");
		s1.add("Test2");
		s1.add("Test3");
		entity1.setName(TEST_NAME);
		entity1.setSomeSet(s1);

		var entity2 = new TestEntity("id2", VERSION2, SOURCE_VALUE, "sourceKey", SOURCETYPE, Collections.emptyMap(),
				Collections.emptySet(), Instant.now(), Instant.now(), null);
		var s2 = new HashSet<String>();
		s2.add("Test4");
		s2.add("Test5");
		s2.add("Test6");
		entity2.setSomeSet(s2);
		entity2.setName(OTHER_NAME);
		var entity3 = new TestEntity("id3", VERSION2, SOURCE_VALUE, "sourceKey", SOURCETYPE, Collections.emptyMap(),
				Collections.emptySet(), Instant.now(), Instant.now(), null);
		var s3 = new HashSet<String>();
		s3.add("Test7");
		s3.add("Test8");
		s3.add("Test9");
		entity3.setName(OTHER_NAME);
		entity3.setSomeSet(s3);
		store.put(entity1);
		store.put(entity2);
		store.put(entity3);
		var p1 = Predicates.notInclude(TestEntity.FIELD_SOMESET, "Test4");
		var retSet1 = store.keySet(p1, limit);

		assertEquals(2, retSet1.size());
		assertTrue(retSet1.contains("id1"));
		assertTrue(retSet1.contains("id3"));
	}

	@Test
	public void queryNotIncludeByField() {
		var store = new IndexedHashMap<String, TestEntity>(null, this.getClass().getSimpleName());
		store.addQueryField(TestEntity.FIELD_SOMESET, TestEntity::testSomeSet);
		queryNotInclude(store);
	}

	@Test
	public void queryNotIncludeByIndex() {
		var store = new IndexedHashMap<String, TestEntity>(null, this.getClass().getSimpleName());
		store.addIndex(TestEntity.FIELD_SOMESET, IndexType.EQUAL, TestEntity::getSomeSetForIndex);
		queryNotInclude(store);
	}
}
