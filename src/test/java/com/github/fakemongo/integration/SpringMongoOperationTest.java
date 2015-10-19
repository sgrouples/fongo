package com.github.fakemongo.integration;

import com.github.fakemongo.junit.FongoRule;
import com.google.common.collect.Sets;
import com.mongodb.DBCollection;
import com.mongodb.Mongo;
import com.mongodb.WriteResult;
import java.util.Collections;
import java.util.Date;
import java.util.Set;
import java.util.UUID;
import org.assertj.core.api.Assertions;
import static org.junit.Assert.assertEquals;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.springframework.data.mongodb.core.IndexOperations;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.SimpleMongoDbFactory;
import org.springframework.data.mongodb.core.index.IndexInfo;
import static org.springframework.data.mongodb.core.query.Criteria.where;
import static org.springframework.data.mongodb.core.query.Query.query;
import org.springframework.data.mongodb.core.query.Update;

/**
 * User: william
 * Date: 15/03/14
 */
public class SpringMongoOperationTest {

  @Rule
  public FongoRule fongoRule = new FongoRule(false);

  private MongoOperations mongoOperations;

  @Before
  public void before() throws Exception {
    Mongo mongo = fongoRule.getMongo();
    //Mongo mongo = new MongoClient();
    mongoOperations = new MongoTemplate(new SimpleMongoDbFactory(mongo, UUID.randomUUID().toString()));
  }

  @Test
  public void insertAndIndexesTest() {
    Item item = new Item(UUID.randomUUID(), "name", new Date());
    mongoOperations.insert(item);

    DBCollection collection = mongoOperations.getCollection(Item.COLLECTION_NAME);
    assertEquals(1, collection.count());

    IndexOperations indexOperations = mongoOperations.indexOps(Item.COLLECTION_NAME);
    System.out.println(indexOperations.getIndexInfo());
    boolean indexedId = false;
    boolean indexedName = false;
    for (IndexInfo indexInfo : indexOperations.getIndexInfo()) {
      if (indexInfo.isIndexForFields(Collections.singletonList("_id"))) {
        indexedId = true;
      }
      if (indexInfo.isIndexForFields(Collections.singletonList("name"))) {
        indexedName = true;
      }
    }
    Assertions.assertThat(indexedId).as("_id field is not indexedId").isTrue();
    Assertions.assertThat(indexedName).as("name field is not indexedId").isTrue();
  }


  static class A {
    public A(String id, Set<B> bs) {
      this.id = id;
      this.bs = bs;
    }

    String id;
    Set<B> bs;
  }

  static class B {
    public B(String reference, Set<String> ids) {
      this.reference = reference;
      this.ids = ids;
    }

    String reference;
    Set<String> ids;
  }

  @Test
  public void test_ids() {
    // Given
    B b = new B("ref", Sets.newHashSet("1", "2"));
    B b2 = new B("ref2", null);
    A a = new A("id", Sets.newHashSet(b, b2));
    mongoOperations.insert(a);
    String reference = "ref";
    String idToPull = "1";

    // When
    final WriteResult writeResult = mongoOperations.updateFirst(query(where("_id").is(a.id).and("bs").elemMatch(where("reference").is(reference))), new Update().pull("bs.$.ids", idToPull), A.class);

    // Then
    Assertions.assertThat(writeResult.getN()).isEqualTo(1);
  }

}
