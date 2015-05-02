package com.github.fakemongo.integration.jongo;

import com.github.fakemongo.junit.FongoRule;
import com.mongodb.WriteConcern;
import org.assertj.core.api.Assertions;
import org.jongo.Jongo;
import org.jongo.MongoCollection;
import org.jongo.MongoCursor;
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
}
