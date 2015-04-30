package com.github.fakemongo;

import com.github.fakemongo.junit.FongoRule;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.CountOptions;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;
import java.util.ArrayList;
import static java.util.Arrays.asList;
import java.util.List;
import static org.assertj.core.api.Assertions.assertThat;
import org.bson.Document;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

public class FongoV3Test {

  public final FongoRule fongoRule = new FongoRule(false);

  public final ExpectedException exception = ExpectedException.none();

  @Rule
  public TestRule rules = RuleChain.outerRule(exception).around(fongoRule);

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
    collection.insertOne(new Document("_id", 1));
    collection.insertOne(new Document("_id", 2));
    final List<Document> documents = toList(collection.find());

    // Then
    assertThat(documents).containsExactly(new Document("_id", 1), new Document("_id", 2));
  }

  @Test
  public void insertMany_can_be_retrieved() {
    // Given
    final MongoCollection<Document> collection = newCollection();

    // When
    collection.insertMany(asList(new Document("_id", 1), new Document("_id", 2)));
    final List<Document> documents = toList(collection.find());

    // Then
    assertThat(documents).containsExactly(new Document("_id", 1), new Document("_id", 2));
  }

  @Test
  public void find_with_limit() {
    // Given
    final MongoCollection<Document> collection = newCollection();

    // When
    collection.insertMany(asList(new Document("_id", 1), new Document("_id", 2)));
    final List<Document> documents = toList(collection.find().limit(1));

    // Then
    assertThat(documents).containsExactly(new Document("_id", 1));
  }

  @Test
  public void find_with_skip() {
    // Given
    final MongoCollection<Document> collection = newCollection();

    // When
    collection.insertMany(asList(new Document("_id", 1), new Document("_id", 2)));
    final List<Document> documents = toList(collection.find().skip(1));

    // Then
    assertThat(documents).containsExactly(new Document("_id", 2));
  }

  @Test
  public void find_with_skip_and_limit_empty() {
    // Given
    final MongoCollection<Document> collection = newCollection();

    // When
    collection.insertMany(asList(new Document("_id", 1), new Document("_id", 2)));
    final List<Document> documents = toList(collection.find().skip(2).limit(1));

    // Then
    assertThat(documents).isEmpty();
  }

  @Test
  public void find_with_skip_and_limit_only_one() {
    // Given
    final MongoCollection<Document> collection = newCollection();

    // When
    collection.insertMany(asList(new Document("_id", 1), new Document("_id", 2), new Document("_id", 3)));
    final List<Document> documents = toList(collection.find().skip(1).limit(1));

    // Then
    assertThat(documents).containsExactly(new Document("_id", 2));
  }

  @Test
  public void find_with_criteria() {
    // Given
    final MongoCollection<Document> collection = newCollection();
    collection.insertMany(asList(new Document("_id", 1), new Document("_id", 2)));

    // When
    final List<Document> documents = toList(collection.find(new Document("_id", 1)));

    // Then
    assertThat(documents).containsExactly(new Document("_id", 1));
  }

  @Test
  public void updateOne_simple() {
    // Given
    MongoCollection collection = newCollection();
    collection.insertOne(new Document("_id", 1));
    collection.insertOne(new Document("_id", 2).append("b", 5));
    collection.insertOne(new Document("_id", 3));
    collection.insertOne(new Document("_id", 4));

    // When
    final UpdateResult updateResult = collection.updateOne(new Document("_id", 2), new Document("$set", new Document("b", 8)));

    // Then
    assertThat(toList(collection.find(new Document("_id", 2)))).containsExactly(new Document("_id", 2).append("b", 8));
    assertThat(updateResult.getMatchedCount()).isEqualTo(1);
//    assertThat(updateResult.getModifiedCount()).isEqualTo(1);
    assertThat(updateResult.getUpsertedId()).isNull();
  }

  @Test
  public void deleteOne_remove_one() {
    // Given
    MongoCollection collection = newCollection();
    collection.insertOne(new Document("_id", 1));
    collection.insertOne(new Document("_id", 2).append("b", 5));
    collection.insertOne(new Document("_id", 3).append("b", 5));
    collection.insertOne(new Document("_id", 4));
    collection.insertOne(new Document("_id", 5).append("b", 6));

    // When
    final DeleteResult deleteResult = collection.deleteOne(new Document("b", 5));

    // Then
    assertThat(toList(collection.find())).containsExactly(new Document("_id", 1), new Document("_id", 3).append("b", 5), new Document("_id", 4), new Document("_id", 5).append("b", 6));
    assertThat(deleteResult.getDeletedCount()).isEqualTo(1L);
  }

  @Test
  public void deleteMany_remove_many() {
    // Given
    MongoCollection collection = newCollection();
    collection.insertOne(new Document("_id", 1));
    collection.insertOne(new Document("_id", 2).append("b", 5));
    collection.insertOne(new Document("_id", 3).append("b", 5));
    collection.insertOne(new Document("_id", 4));
    collection.insertOne(new Document("_id", 5).append("b", 6));

    // When
    final DeleteResult deleteResult = collection.deleteMany(new Document("b", 5));

    // Then
    assertThat(toList(collection.find())).containsExactly(new Document("_id", 1), new Document("_id", 4), new Document("_id", 5).append("b", 6));
    assertThat(deleteResult.getDeletedCount()).isEqualTo(2L);
  }

  @Test
  public void findOneAndDelete_remove_one() {
    // Given
    MongoCollection collection = newCollection();
    collection.insertOne(new Document("_id", 1));
    collection.insertOne(new Document("_id", 2).append("b", 5));
    collection.insertOne(new Document("_id", 3).append("b", 5));
    collection.insertOne(new Document("_id", 4));
    collection.insertOne(new Document("_id", 5).append("b", 6));

    // When
    final Object deleted = collection.findOneAndDelete(new Document("b", 5));

    // Then
    assertThat(toList(collection.find())).containsExactly(new Document("_id", 1), new Document("_id", 3).append("b", 5), new Document("_id", 4), new Document("_id", 5).append("b", 6));
    assertThat(deleted).isEqualTo(new Document("_id", 2).append("b", 5));
  }

  @Test
  public void findOneAndReplace_replace_the_first() {
    // Given
    MongoCollection collection = newCollection();
    collection.insertOne(new Document("_id", 1));
    collection.insertOne(new Document("_id", 2).append("b", 5));
    collection.insertOne(new Document("_id", 3).append("b", 5));
    collection.insertOne(new Document("_id", 4));
    collection.insertOne(new Document("_id", 5).append("b", 6));

    // When
    final Object deleted = collection.findOneAndReplace(new Document("b", 5), new Document("_id", 2).append("b", 8));

    // Then
    assertThat(toList(collection.find())).containsExactly(new Document("_id", 1), new Document("_id", 2).append("b", 8),
        new Document("_id", 3).append("b", 5), new Document("_id", 4), new Document("_id", 5).append("b", 6));
    assertThat(deleted).isEqualTo(new Document("_id", 2).append("b", 5));
  }

  private List<Document> toList(final FindIterable<Document> collection) {
    final List<Document> documents = new ArrayList<Document>();
    for (Document document : collection) {
      documents.add(document);
    }
    return documents;
  }

  public MongoCollection newCollection() {
    return fongoRule.newMongoCollection("db");
  }
}
