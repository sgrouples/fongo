/**
 * Copyright (C) 2015 Deveryware S.A. All Rights Reserved.
 */
package com.github.fakemongo;

import com.github.fakemongo.junit.FongoRule;
import com.mongodb.WriteConcern;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.DistinctIterable;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoIterable;
import com.mongodb.client.model.CountOptions;
import com.mongodb.client.model.DeleteOneModel;
import static com.mongodb.client.model.Filters.eq;
import com.mongodb.client.model.FindOneAndUpdateOptions;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.InsertOneModel;
import com.mongodb.client.model.Projections;
import static com.mongodb.client.model.Projections.excludeId;
import com.mongodb.client.model.ReplaceOneModel;
import com.mongodb.client.model.ReturnDocument;
import static com.mongodb.client.model.Sorts.ascending;
import static com.mongodb.client.model.Sorts.descending;
import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;
import com.mongodb.connection.ServerVersion;
import java.util.ArrayList;
import java.util.Arrays;
import static java.util.Arrays.asList;
import java.util.List;
import org.assertj.core.api.Assertions;
import static org.assertj.core.api.Assertions.assertThat;
import org.assertj.core.util.Lists;
import org.bson.BsonString;
import org.bson.Document;
import org.junit.Assume;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

/**
 *
 */
public abstract class AbstractFongoV3Test {
  public final FongoRule fongoRule = new FongoRule(false, serverVersion());

  public final ExpectedException exception = ExpectedException.none();

  @Rule
  public TestRule rules = RuleChain.outerRule(exception).around(fongoRule);

  abstract ServerVersion serverVersion();

  @Test
  public void getCollection_works() {
    // Given
    final MongoDatabase mongoDatabase = fongoRule.getMongoClient().getDatabase("test");

    // When
    final MongoCollection<Document> collection = mongoDatabase.getCollection("test");

    // Then
    assertThat(collection).isNotNull();
  }

  @Test
  public void getCollection_and_count_works_with_empty_collection() {
    // Given
    final MongoCollection<Document> collection = newCollection();

    // When/Then
    assertThat(collection.count()).isEqualTo(0);
  }

  @Test
  public void insertOne_and_count_works() {
    // Given
    final MongoCollection<Document> collection = newCollection();
    collection.insertOne(new Document("i", 1));
    collection.insertOne(new Document("i", 1));

    // When/Then
    assertThat(collection.count()).isEqualTo(2L);
  }

  @Test
  public void insertOne_and_count_with_skip_works() {
    // Given
    final MongoCollection<Document> collection = newCollection();
    collection.insertOne(new Document("i", 1));
    collection.insertOne(new Document("i", 1));
    collection.insertOne(new Document("i", 1));

    // When/Then
    assertThat(collection.count(null, new CountOptions().skip(1))).isEqualTo(2L);
  }

  @Test
  public void insertOne_and_count_with_limit_works() {
    // Given
    final MongoCollection<Document> collection = newCollection();
    collection.insertOne(new Document("i", 1));
    collection.insertOne(new Document("i", 1));
    collection.insertOne(new Document("i", 1));

    // When/Then
    assertThat(collection.count(null, new CountOptions().limit(1))).isEqualTo(1L);
  }

  @Test
  public void insertOne_and_count_with_criteria() {
    // Given
    final MongoCollection<Document> collection = newCollection();
    collection.insertOne(new Document("i", 1));
    collection.insertOne(new Document("i", 2));
    collection.insertOne(new Document("i", 2));

    // When/Then
    assertThat(collection.count(new Document("i", 2))).isEqualTo(2L);
  }

  @Test
  public void insertOne_can_be_retrieved() {
    // Given
    final MongoCollection<Document> collection = newCollection();

    // When
    collection.insertOne(docId(1));
    collection.insertOne(docId(2));
    final List<Document> documents = toList(collection.find());

    // Then
    assertThat(documents).containsExactly(docId(1), docId(2));
  }

  @Test
  public void insertMany_can_be_retrieved() {
    // Given
    final MongoCollection<Document> collection = newCollection();

    // When
    collection.insertMany(asList(docId(1), docId(2)));
    final List<Document> documents = toList(collection.find());

    // Then
    assertThat(documents).containsExactly(docId(1), docId(2));
  }

  @Test
  public void find_with_limit() {
    // Given
    final MongoCollection<Document> collection = newCollection();

    // When
    collection.insertMany(asList(docId(1), docId(2)));
    final List<Document> documents = toList(collection.find().limit(1));

    // Then
    assertThat(documents).containsExactly(docId(1));
  }

  @Test
  public void find_first() {
    // Given
    final MongoCollection<Document> collection = newCollection();

    // When
    collection.insertMany(asList(docId(1), docId(2)));
    final Document first = collection.find().first();

    // Then
    assertThat(first).isEqualTo(docId(1));
  }

  @Test
  public void find_filter() {
    // Given
    final MongoCollection<Document> collection = newCollection();

    // When
    collection.insertMany(asList(docId(1), docId(2)));
    final FindIterable<Document> documents = collection.find(eq("_id", 1));

    // Then
    Assertions.assertThat(toList(documents)).containsExactly(docId(1));
  }

  @Test
  public void find_sort() {
    // Given
    final MongoCollection<Document> collection = newCollection();

    // When
    collection.insertMany(asList(docId(1), docId(2)));
    final FindIterable<Document> documents = collection.find().sort(descending("_id"));

    // Then
    Assertions.assertThat(toList(documents)).containsExactly(docId(2), docId(1));
  }

  @Test
  public void find_with_skip() {
    // Given
    final MongoCollection<Document> collection = newCollection();

    // When
    collection.insertMany(asList(docId(1), docId(2)));
    final List<Document> documents = toList(collection.find().skip(1));

    // Then
    assertThat(documents).containsExactly(docId(2));
  }

  @Test
  public void find_with_skip_and_limit_empty() {
    // Given
    final MongoCollection<Document> collection = newCollection();

    // When
    collection.insertMany(asList(docId(1), docId(2)));
    final List<Document> documents = toList(collection.find().skip(2).limit(1));

    // Then
    assertThat(documents).isEmpty();
  }

  @Test
  public void find_with_skip_and_limit_only_one() {
    // Given
    final MongoCollection<Document> collection = newCollection();

    // When
    collection.insertMany(asList(docId(1), docId(2), docId(3)));
    final List<Document> documents = toList(collection.find().skip(1).limit(1));

    // Then
    assertThat(documents).containsExactly(docId(2));
  }

  @Test
  public void find_with_criteria() {
    // Given
    final MongoCollection<Document> collection = newCollection();
    collection.insertMany(asList(docId(1), docId(2)));

    // When
    final List<Document> documents = toList(collection.find(docId(1)));

    // Then
    assertThat(documents).containsExactly(docId(1));
  }

  @Test
  public void find_with_criteria_and_projection() {
    // Given
    final MongoCollection<Document> collection = newCollection();
    collection.insertMany(asList(docId(1).append("b", 2).append("c", 3), docId(2).append("b", 3)));

    // When
    final List<Document> documents = toList(collection.find(docId(1)).projection(new Document("b", 1)));

    // Then
    assertThat(documents).containsExactly(docId(1).append("b", 2));
  }

  @Test
  public void find_projection_excludeId() {
    // Given
    final MongoCollection<Document> collection = newCollection();
    collection.insertMany(asList(docId(1).append("b", 2).append("c", 3), docId(2).append("b", 3)));

    // When
    final List<Document> documents = toList(collection.find(docId(1)).projection(excludeId()));

    // Then
    assertThat(documents).containsExactly(new Document("b", 2).append("c", 3));
  }

  @Test
  public void updateOne_simple() {
    // Given
    MongoCollection collection = newCollection();
    collection.insertOne(docId(1));
    collection.insertOne(docId(2).append("b", 5));
    collection.insertOne(docId(3));
    collection.insertOne(docId(4));

    // When
    final UpdateResult updateResult = collection.updateOne(docId(2), new Document("$set", new Document("b", 8)));

    // Then
    Assertions.assertThat(toList(collection.find(docId(2)))).containsExactly(docId(2).append("b", 8));
    assertThat(updateResult.getMatchedCount()).isEqualTo(1);
    assertThat(updateResult.getUpsertedId()).isNull();
    if (!serverVersion().equals(Fongo.OLD_SERVER_VERSION)) {
      assertThat(updateResult.getModifiedCount()).isEqualTo(1);
    }
  }

  @Test
  public void updateOne_rejectNot$() {
    // Given
    MongoCollection collection = newCollection();
    collection.insertOne(docId(1));
    collection.insertOne(docId(2).append("b", 5));
    collection.insertOne(docId(3));
    collection.insertOne(docId(4));

    exception.expectMessage("Invalid BSON field name b");
    exception.expect(IllegalArgumentException.class);
    // When
    collection.updateOne(docId(2), new Document("b", 8));
  }

  @Test
  public void deleteOne_remove_one() {
    // Given
    MongoCollection collection = newCollection();
    collection.insertOne(docId(1));
    collection.insertOne(docId(2).append("b", 5));
    collection.insertOne(docId(3).append("b", 5));
    collection.insertOne(docId(4));
    collection.insertOne(docId(5).append("b", 6));

    // When
    final DeleteResult deleteResult = collection.deleteOne(new Document("b", 5));

    // Then
    Assertions.assertThat(toList(collection.find())).containsExactly(docId(1), docId(3).append("b", 5), docId(4), docId(5).append("b", 6));
    assertThat(deleteResult.getDeletedCount()).isEqualTo(1L);
  }

  @Test
  public void deleteOne_remove_must_throw_exception_with_unacknowledged() {
    // Given
    MongoCollection collection = newCollection().withWriteConcern(WriteConcern.UNACKNOWLEDGED);
    collection.insertOne(docId(1));
    collection.insertOne(docId(2).append("b", 5));
    collection.insertOne(docId(3).append("b", 5));
    collection.insertOne(docId(4));
    collection.insertOne(docId(5).append("b", 6));

    // When
    final DeleteResult deleteResult = collection.deleteOne(new Document("b", 5));

    // Then
    exception.expect(UnsupportedOperationException.class);
    deleteResult.getDeletedCount();
  }

  @Test
  public void deleteMany_remove_many() {
    // Given
    MongoCollection collection = newCollection();
    collection.insertOne(docId(1));
    collection.insertOne(docId(2).append("b", 5));
    collection.insertOne(docId(3).append("b", 5));
    collection.insertOne(docId(4));
    collection.insertOne(docId(5).append("b", 6));

    // When
    final DeleteResult deleteResult = collection.deleteMany(new Document("b", 5));

    // Then
    Assertions.assertThat(toList(collection.find())).containsExactly(docId(1), docId(4), docId(5).append("b", 6));
    assertThat(deleteResult.getDeletedCount()).isEqualTo(2L);
  }

  @Test
  public void findOneAndDelete_remove_one() {
    // Given
    MongoCollection<Document> collection = newCollection();
    collection.insertOne(docId(1));
    collection.insertOne(docId(2).append("b", 5));
    collection.insertOne(docId(3).append("b", 5));
    collection.insertOne(docId(4));
    collection.insertOne(docId(5).append("b", 6));

    // When
    final Document deleted = collection.findOneAndDelete(new Document("b", 5));

    // Then
    Assertions.assertThat(toList(collection.find())).containsExactly(docId(1), docId(3).append("b", 5), docId(4), docId(5).append("b", 6));
    assertThat(deleted).isEqualTo(docId(2).append("b", 5));
  }

  @Test
  public void findOneAndReplace_replace_the_first() {
    // Given
    MongoCollection<Document> collection = newCollection();
    collection.insertOne(docId(1));
    collection.insertOne(docId(2).append("b", 5));
    collection.insertOne(docId(3).append("b", 5));
    collection.insertOne(docId(4));
    collection.insertOne(docId(5).append("b", 6));

    // When
    final Document deleted = collection.findOneAndReplace(new Document("b", 5), docId(2).append("c", 8));

    // Then
    Assertions.assertThat(toList(collection.find())).containsOnly(docId(1), docId(2).append("c", 8),
        docId(3).append("b", 5), docId(4), docId(5).append("b", 6));
    assertThat(deleted).isEqualTo(docId(2).append("b", 5));
  }

  @Test
  public void findOneAndUpdate_$set_replace_the_first() {
    // Given
    MongoCollection<Document> collection = newCollection();
    collection.insertOne(docId(1));
    collection.insertOne(docId(2).append("b", 5));
    collection.insertOne(docId(3).append("b", 5));
    collection.insertOne(docId(4));
    collection.insertOne(docId(5).append("b", 6));

    // When
    final Document updated = collection.findOneAndUpdate(new Document("b", 5), new Document("$set", docId(2).append("c", 8)));

    // Then
    Assertions.assertThat(toList(collection.find())).containsOnly(docId(1), docId(2).append("b", 5).append("c", 8),
        docId(3).append("b", 5), docId(4), docId(5).append("b", 6));
    assertThat(updated).isEqualTo(docId(2).append("b", 5));
  }

  @Test
  public void findOneAndUpdate_replace_the_first() {
    // Given
    MongoCollection<Document> collection = newCollection();
    collection.insertOne(docId(1));
    collection.insertOne(docId(2).append("b", 5));
    collection.insertOne(docId(3).append("b", 5));
    collection.insertOne(docId(4));
    collection.insertOne(docId(5).append("b", 6));

    // When
    final Document updated = collection.findOneAndUpdate(new Document("b", 5), new Document("$set", new Document("c", 8)));

    // Then
    Assertions.assertThat(toList(collection.find())).containsOnly(docId(1), docId(2).append("b", 5).append("c", 8),
        docId(3).append("b", 5), docId(4), docId(5).append("b", 6));
    assertThat(updated).isEqualTo(docId(2).append("b", 5));
  }

  @Test
  public void findOneAndUpdate_sort_replace_the_last() {
    // Given
    MongoCollection<Document> collection = newCollection();
    collection.insertOne(docId(1));
    collection.insertOne(docId(2).append("b", 5));
    collection.insertOne(docId(3).append("b", 5));
    collection.insertOne(docId(4));
    collection.insertOne(docId(5).append("b", 6));

    // When
    final Document updated = collection.findOneAndUpdate(new Document("b", 5), new Document("$set", new Document("c", 8)),
        new FindOneAndUpdateOptions().sort(descending("_id")));

    // Then
    Assertions.assertThat(toList(collection.find())).containsOnly(docId(1), docId(2).append("b", 5),
        docId(3).append("b", 5).append("c", 8), docId(4), docId(5).append("b", 6));
    assertThat(updated).isEqualTo(docId(3).append("b", 5));
  }

  @Test
  public void findOneAndUpdate_projection() {
    // Given
    MongoCollection<Document> collection = newCollection();
    collection.insertOne(docId(1));
    collection.insertOne(docId(2).append("b", 5));
    collection.insertOne(docId(3).append("b", 5));
    collection.insertOne(docId(4));
    collection.insertOne(docId(5).append("b", 6));

    // When
    final Document updated = collection.findOneAndUpdate(new Document("b", 5), new Document("$set", new Document("c", 8)),
        new FindOneAndUpdateOptions().projection(Projections.include("c")).returnDocument(ReturnDocument.AFTER));

    // Then
    Assertions.assertThat(toList(collection.find())).containsOnly(docId(1), docId(2).append("b", 5).append("c", 8),
        docId(3).append("b", 5), docId(4), docId(5).append("b", 6));
    assertThat(updated).isEqualTo(docId(2).append("c", 8));
  }

  @Test
  public void findOneAndUpdate_retrieve_before() {
    // Given
    MongoCollection<Document> collection = newCollection();
    collection.insertOne(new Document("updateField", 1));

    // When
    Document document = collection.findOneAndUpdate(eq("updateField", 1), new Document("$set", new Document("updateField", 2)),
        new FindOneAndUpdateOptions().returnDocument(ReturnDocument.BEFORE));

    // Then
    assertThat(document.get("updateField")).isEqualTo(1);
    document = collection.find(eq("updateField", 2)).first();
    assertThat(document.get("updateField")).isEqualTo(2);
  }

  @Test
  public void findOneAndUpdate_retrieve_after() {
    // Given
    MongoCollection<Document> collection = newCollection();
    collection.insertOne(new Document("updateField", 1));

    // When
    Document document = collection.findOneAndUpdate(eq("updateField", 1), new Document("$set", new Document("updateField", 3)),
        new FindOneAndUpdateOptions().returnDocument(ReturnDocument.AFTER));

    // Then
    assertThat(document.get("updateField")).isEqualTo(3);
    document = collection.find(eq("updateField", 3)).first();
    assertThat(document.get("updateField")).isEqualTo(3);
  }

  @Test
  public void findOneAndUpdate_upsert() {
    // Given
    MongoCollection<Document> collection = newCollection();

    // When
    Document document = collection.findOneAndUpdate(eq("updateField", 1), new Document("$set", new Document("updateField", 3)),
        new FindOneAndUpdateOptions().returnDocument(ReturnDocument.AFTER).upsert(true));

    // Then
    assertThat(document.get("updateField")).isEqualTo(3);
    document = collection.find(eq("updateField", 3)).first();
    assertThat(document.get("updateField")).isEqualTo(3);
  }

  @Test
  public void findOneAndUpdate_throw_exception_if_not_$field() {
    // Given
    MongoCollection<Document> collection = newCollection();
    collection.insertOne(new Document("updateField", 1));

    exception.expect(IllegalArgumentException.class);
    // When
    collection.findOneAndUpdate(eq("updateField", 1), new Document("updateField", 3),
        new FindOneAndUpdateOptions().returnDocument(ReturnDocument.AFTER));
  }

  @Test
  public void distinct_must_retrieve_distinct_results() {
    // Given
    MongoCollection collection = newCollection();
    collection.insertOne(docId(1));
    collection.insertOne(docId(2).append("b", 5));
    collection.insertOne(docId(3).append("b", 5));
    collection.insertOne(docId(4));
    collection.insertOne(docId(5).append("b", 6));

    // When
    final DistinctIterable distinctIterable = collection.distinct("b", Integer.class);

    // Then
    Assertions.assertThat(toList(distinctIterable)).containsExactly(5, 6);
  }

  @Test
  public void distinct_first_must_retrieve_first_distinct_results() {
    // Given
    MongoCollection collection = newCollection();
    collection.insertOne(docId(1));
    collection.insertOne(docId(2).append("b", 5));
    collection.insertOne(docId(3).append("b", 5));
    collection.insertOne(docId(4));
    collection.insertOne(docId(5).append("b", 6));

    // When
    final DistinctIterable distinctIterable = collection.distinct("b", Integer.class);

    // Then
    assertThat(distinctIterable.first()).isEqualTo(5);
  }

  @Test
  public void distinct_filter_must_retrieve_distinct_results() {
    // Given
    MongoCollection collection = newCollection();
    collection.insertOne(docId(1));
    collection.insertOne(docId(2).append("b", 5));
    collection.insertOne(docId(3).append("b", 5));
    collection.insertOne(docId(4));
    collection.insertOne(docId(5).append("b", 6));

    // When
    final DistinctIterable distinctIterable = collection.distinct("b", Integer.class);

    // Then
    Assertions.assertThat(toList(distinctIterable.filter(docId(5)))).containsExactly(6);
  }

  @Test
  public void createIndex_create_an_index() {
    Assume.assumeTrue(fongoRule.isRealMongo());

    // Given
    MongoCollection collection = newCollection();
    collection.insertOne(docId(1));
    collection.insertOne(docId(2).append("b", 5));
    collection.insertOne(docId(3).append("b", 5));
    collection.insertOne(docId(4));
    collection.insertOne(docId(5).append("b", 6));

    // When
    collection.createIndex(new Document("b", 1), new IndexOptions().name("b").unique(false));

    // Then
    Assertions.assertThat(toList(collection.listIndexes())).containsExactly(new Document("v", 1).append("key", new Document("_id", 1)).append("name", "_id_").append("ns", collection.getNamespace().getDatabaseName() + "." + collection.getNamespace().getCollectionName()),
        new Document("v", 1).append("key", new Document("b", 1)).append("name", "b").append("ns", collection.getNamespace().getDatabaseName() + "." + collection.getNamespace().getCollectionName()));
  }

  // See http://mongodb.github.io/mongo-java-driver/3.0/driver/getting-started/quick-tour/
  @Test
  public void bulkWrite_ordered() {
    // 2. Ordered bulk operation - order is guaranteed
    // Given
    MongoCollection collection = newCollection();

    // When
    collection.bulkWrite(
        Arrays.asList(new InsertOneModel<Document>(new Document("_id", 1)),
            new InsertOneModel<Document>(new Document("_id", 2)),
            new InsertOneModel<Document>(new Document("_id", 3)),
            new InsertOneModel<Document>(new Document("_id", 4)),
            new InsertOneModel<Document>(new Document("_id", 5)),
            new InsertOneModel<Document>(new Document("_id", 6)),
            new UpdateOneModel<Document>(new Document("_id", 1),
                new Document("$set", new Document("x", 2))),
            new DeleteOneModel<Document>(new Document("_id", 2)),
            new ReplaceOneModel<Document>(new Document("_id", 3),
                new Document("_id", 3).append("x", 4))));


    // Then
    Assertions.assertThat(toList(collection.find().sort(ascending("_id")))).containsExactly(
        docId(1).append("x", 2), docId(3).append("x", 4), docId(4), docId(5), docId(6));
  }

  @Test
  public void should_utf8_works() {
    // Given
    final MongoCollection<Document> mongoCollection = newCollection();
    final String expected = "\u0010";

    // When
    mongoCollection.insertOne(new Document("_id", expected));

    // Then
    assertThat(mongoCollection.find().first()).isEqualTo(new Document("_id", expected));
  }

  @Test
  public void should_aggregate_works() {
    // Given
    final MongoCollection<Document> mongoCollection = newCollection();
    final String expected = "꼢𑡜ᳫ鉠鮻罖᧭䆔瘉";
    mongoCollection.insertOne(new Document("_id", expected));

    // When
    final AggregateIterable<Document> aggregate = mongoCollection.aggregate(Lists.newArrayList(new Document("$match", new Document("_id", expected))));

    // Then
    Assertions.assertThat(toList(aggregate)).containsOnly(docId(expected));
  }

  @Test
  public void should_get_right_type() {
    // Given
    final MongoCollection<Document> mongoCollection = newCollection();
    Document d = new Document("test", 1l);
    mongoCollection.insertOne(d);
    d = mongoCollection.find(new Document("test", 1l)).first();
    assertThat(d.get("test")).isEqualTo(1L);

    // When
    d = mongoCollection.findOneAndUpdate(new Document("test", 1l), new Document("$set", new Document("test", 1l)));

    // Then
    assertThat(d.get("test")).isInstanceOf(Long.class).isEqualTo(1L);
  }

  @Test
  public void should_modifiedCount_retrieve_the_right_value() {
    Assume.assumeFalse(serverVersion().equals(Fongo.OLD_SERVER_VERSION));
    // Given
    MongoCollection<Document> col = newCollection();
    col.insertOne(new Document("key", "value"));

    // When
    UpdateResult result = col.updateOne(eq("key", "value"), new Document("$set", new Document("key", "value2")));

    // Then
    assertThat(result.getModifiedCount()).isEqualTo(1);
  }


  @Test
  public void should_findOneAndReplace_not_found_do_nothing() {
    // Given
    MongoCollection<Document> col = newCollection();
    final Document document = new Document("_id", 1).append("key", "value");
    col.insertOne(document);

    // When
    final Document oneAndReplace = col.findOneAndReplace(eq("key", "other_value"), new Document("key", "value2"));

    // When
    assertThat(oneAndReplace).isNull();
    assertThat(toList(col.find())).containsOnly(document);
  }

  @Test
  public void should_replaceOne_replace_document() {
    // Given
    MongoCollection<Document> col = newCollection();
    final Document document = new Document("_id", 1).append("key", "value");
    col.insertOne(document);

    // When
    final UpdateResult updateResult = col.replaceOne(eq("_id", 1), new Document("key", "value2"));

    // When
    assertThat(toList(col.find())).containsOnly(new Document("_id", 1).append("key", "value2"));
    assertThat(updateResult.getMatchedCount()).isEqualTo(1L);
    assertThat(updateResult.getUpsertedId()).isNull();
    if (!serverVersion().equals(Fongo.OLD_SERVER_VERSION)) {
      assertThat(updateResult.getModifiedCount()).isEqualTo(1L);
    }
  }

  @Test
  public void should_upsert_when_update() {
    // Given
    MongoCollection<Document> collection = newCollection();

    //Database is empty. Not present document with (_id: '123')
    Document query = new Document("_id", "123");
    Document update = new Document("$set", new Document("name", "Emil"));

    // When
    final UpdateResult updateResult = collection.updateOne(query, update, new UpdateOptions().upsert(true));

    // Then
    assertThat(updateResult).isNotNull();
    assertThat(toList(collection.find())).containsOnly(new Document("_id", "123").append("name", "Emil"));
    assertThat(updateResult.getUpsertedId()).isEqualTo(new BsonString("123"));
    assertThat(updateResult.getMatchedCount()).isEqualTo(0);
    if (!serverVersion().equals(Fongo.OLD_SERVER_VERSION)) {
      assertThat(updateResult.getModifiedCount()).isEqualTo(0);
    }
  }

  private Document docId(final Object value) {
    return new Document("_id", value);
  }

  private <T> List<T> toList(final MongoIterable<T> iterable) {
    return iterable.into(new ArrayList<T>());
  }

  public MongoCollection<Document> newCollection() {
    return fongoRule.newMongoCollection("db");
  }
}
