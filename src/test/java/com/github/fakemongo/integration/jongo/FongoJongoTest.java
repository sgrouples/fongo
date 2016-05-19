package com.github.fakemongo.integration.jongo;

import com.github.fakemongo.junit.FongoRule;
import com.google.common.collect.Lists;
import com.mongodb.AggregationOptions;
import com.mongodb.DBCursor;
import com.mongodb.WriteConcern;
import java.util.Iterator;
import org.assertj.core.api.Assertions;
import static org.assertj.core.api.Assertions.assertThat;
import org.bson.types.ObjectId;
import org.jongo.Jongo;
import org.jongo.MongoCollection;
import org.jongo.MongoCursor;
import org.jongo.QueryModifier;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

public class FongoJongoTest {

  @Rule
  public FongoRule fongoRule = new FongoRule(false);

  private Jongo jongo;

  private MongoCollection collection;

  @Before
  public void setup() {
    this.jongo = new Jongo(fongoRule.getDB());
    this.collection = jongo.getCollection("test").withWriteConcern(WriteConcern.UNACKNOWLEDGED);
  }

  @Test
  public void should_insert_neested_class() {
    // Given
    JongoItem jongoItem = new JongoItem();
    jongoItem.setField("Hello World");
    jongoItem.setId(new JongoItem.JongoItemId("one", "two"));

    // When
    this.collection.insert(jongoItem);
    JongoItem result = this.collection.findOne().as(JongoItem.class);

    // Then
    Assertions.assertThat(result).isEqualTo(jongoItem);
  }

  @Test
  public void should_save_neested_class() {
    // Given
    JongoItem jongoItem = new JongoItem();
    jongoItem.setField("Hello World");
    jongoItem.setId(new JongoItem.JongoItemId("one", "two"));

    // When
    this.collection.save(jongoItem);
    JongoItem result = this.collection.findOne().as(JongoItem.class);

    // Then
    Assertions.assertThat(result).isEqualTo(jongoItem);
  }

  @Test
  public void should_retrieve_neested_class() {
    // Given
    JongoItem jongoItem = new JongoItem();
    jongoItem.setField("Hello World");
    jongoItem.setId(new JongoItem.JongoItemId("one", "two"));
    this.collection.insert(jongoItem);

    // When
    JongoItem result = this.collection.findOne("{_id:#}", jongoItem.getId()).as(JongoItem.class);

    // Then
    Assertions.assertThat(result).isEqualTo(jongoItem);
  }

  @Test
  public void should_findAll_works() {
    // Given
    JongoItem jongoItem = new JongoItem();
    jongoItem.setField("Hello World");
    jongoItem.setId(new JongoItem.JongoItemId("one", "two"));
    this.collection.insert(jongoItem);

    // When
    final MongoCursor<JongoItem> result = this.collection.find("{_id:#}", jongoItem.getId()).as(JongoItem.class);

    // Then
    Assertions.assertThat((Iterable<JongoItem>) result).containsExactly(jongoItem);
  }

  @Test
  public void should_findAndModify_works() {
    // Given
    JongoItem jongoItem = new JongoItem();
    jongoItem.setField("Hello World");
    jongoItem.setId(new JongoItem.JongoItemId("one", "two"));
    this.collection.insert(jongoItem);

    // When
    final JongoItem result = this.collection.findAndModify("{_id:#}", jongoItem.getId()).with("{$set:{field:#}}", "newField").returnNew().as(JongoItem.class);

    // Then
    jongoItem.setField("newField");
    Assertions.assertThat(result).isEqualTo(jongoItem);
  }

  @Test
  public void should_findAndModify_returnOld_works() {
    // Given
    JongoItem jongoItem = new JongoItem();
    jongoItem.setField("Hello World");
    jongoItem.setId(new JongoItem.JongoItemId("one", "two"));
    this.collection.insert(jongoItem);

    // When
    final JongoItem result = this.collection.findAndModify("{_id:#}", jongoItem.getId()).with("{$set:{field:#}}", "newField").as(JongoItem.class);

    // Then
    Assertions.assertThat(result).isEqualTo(jongoItem);
  }

  @Test
  public void should_update_upsert_works() {
    // Given
    JongoItem jongoItem = new JongoItem();
    jongoItem.setField("Hello World");
    jongoItem.setId(new JongoItem.JongoItemId("one", "two"));
    this.collection.insert(jongoItem);

    // When
    this.collection.update("{_id:#}", jongoItem.getId()).upsert().with(jongoItem);
    final JongoItem result = this.collection.findOne().as(JongoItem.class);

    // Then
    Assertions.assertThat(result).isEqualTo(jongoItem);
  }

  @Test
  public void should_canBindPrimitiveArrayParameter() {
    // Given
    collection.insert("{value:42, other:true}");

    // When

    // Then
    assertThat(collection.count("{value:{$in:#}}", new int[]{42, 34})).isEqualTo(1);
  }


  @Test
  public void canUseListWithANullElement() throws Exception {

    collection.insert("{name:null}");
    collection.insert("{name:'John'}");

    long nb = collection.count("{name:{$in:#}}", Lists.newArrayList(1, null));

    assertThat(nb).isEqualTo(1);
  }

  @Test
  public void canUseQueryModifier() throws Exception {
        /* given */
    collection.save(new Friend(new ObjectId(), "John"));
    collection.save(new Friend(new ObjectId(), "Robert"));

        /* when */
    Iterator<Friend> friends = collection.find()
        .with(new QueryModifier() {
          public void modify(DBCursor cursor) {
            cursor.addSpecial("$maxScan", 1);
          }
        })
        .as(Friend.class);

        /* then */
    assertThat(friends.hasNext()).isTrue();
    friends.next();
    assertThat(friends.hasNext()).isFalse();
  }

  @Test
  public void canAggregateWithDefaultOptions() throws Exception {
    collection.save(new Friend(new ObjectId(), "John"));
    collection.save(new Friend(new ObjectId(), "Robert"));

    AggregationOptions options = AggregationOptions.builder().build();
    Iterable<Friend> friends = collection.aggregate("{$match:{}}").options(options).as(Friend.class);

    assertThat(friends.iterator().hasNext()).isTrue();
    for (Friend friend : friends) {
      assertThat(friend.getName()).isIn("John", "Robert");
    }
  }

}
