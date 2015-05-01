package com.github.fakemongo;

import com.github.fakemongo.junit.FongoRule;
import com.mongodb.client.DistinctIterable;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoIterable;
import com.mongodb.client.model.CountOptions;
import com.mongodb.client.model.DeleteOneModel;
import static com.mongodb.client.model.Filters.eq;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.InsertOneModel;
import static com.mongodb.client.model.Projections.excludeId;
import com.mongodb.client.model.ReplaceOneModel;
import static com.mongodb.client.model.Sorts.ascending;
import static com.mongodb.client.model.Sorts.descending;
import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;
import java.util.ArrayList;
import java.util.Arrays;
import static java.util.Arrays.asList;
import java.util.List;
import static org.assertj.core.api.Assertions.assertThat;
import org.bson.Document;
import org.junit.Assume;
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
    assertThat(toList(documents)).containsExactly(docId(1));
  }

  @Test
  public void find_sort() {
    // Given
    final MongoCollection<Document> collection = newCollection();

    // When
    collection.insertMany(asList(docId(1), docId(2)));
    final FindIterable<Document> documents = collection.find().sort(descending("_id"));

    // Then
    assertThat(toList(documents)).containsExactly(docId(2), docId(1));
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
    assertThat(toList(collection.find(docId(2)))).containsExactly(docId(2).append("b", 8));
    assertThat(updateResult.getMatchedCount()).isEqualTo(1);
//    assertThat(updateResult.getModifiedCount()).isEqualTo(1);
    assertThat(updateResult.getUpsertedId()).isNull();
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
    assertThat(toList(collection.find())).containsExactly(docId(1), docId(3).append("b", 5), docId(4), docId(5).append("b", 6));
    assertThat(deleteResult.getDeletedCount()).isEqualTo(1L);
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
    assertThat(toList(collection.find())).containsExactly(docId(1), docId(4), docId(5).append("b", 6));
    assertThat(deleteResult.getDeletedCount()).isEqualTo(2L);
  }

  @Test
  public void findOneAndDelete_remove_one() {
    // Given
    MongoCollection collection = newCollection();
    collection.insertOne(docId(1));
    collection.insertOne(docId(2).append("b", 5));
    collection.insertOne(docId(3).append("b", 5));
    collection.insertOne(docId(4));
    collection.insertOne(docId(5).append("b", 6));

    // When
    final Object deleted = collection.findOneAndDelete(new Document("b", 5));

    // Then
    assertThat(toList(collection.find())).containsExactly(docId(1), docId(3).append("b", 5), docId(4), docId(5).append("b", 6));
    assertThat(deleted).isEqualTo(docId(2).append("b", 5));
  }

  @Test
  public void findOneAndReplace_replace_the_first() {
    // Given
    MongoCollection collection = newCollection();
    collection.insertOne(docId(1));
    collection.insertOne(docId(2).append("b", 5));
    collection.insertOne(docId(3).append("b", 5));
    collection.insertOne(docId(4));
    collection.insertOne(docId(5).append("b", 6));

    // When
    final Object deleted = collection.findOneAndReplace(new Document("b", 5), docId(2).append("b", 8));

    // Then
    assertThat(toList(collection.find())).containsOnly(docId(1), docId(2).append("b", 8),
        docId(3).append("b", 5), docId(4), docId(5).append("b", 6));
    assertThat(deleted).isEqualTo(docId(2).append("b", 5));
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
    assertThat(toList(distinctIterable)).containsExactly(5, 6);
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
    assertThat(toList(distinctIterable.filter(docId(5)))).containsExactly(6);
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
    assertThat(toList(collection.listIndexes())).containsExactly(new Document("v", 1).append("key", new Document("_id", 1)).append("name", "_id_").append("ns", collection.getNamespace().getDatabaseName() + "." + collection.getNamespace().getCollectionName()),
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
    assertThat(toList(collection.find().sort(ascending("_id")))).containsExactly(
        docId(1).append("x", 2), docId(3).append("x", 4), docId(4), docId(5), docId(6));
  }

  private Document docId(int value) {
    return new Document("_id", value);
  }

  private <T> List<T> toList(final MongoIterable<T> iterable) {
    final List<T> documents = new ArrayList<T>();
    for (T document : iterable) {
      documents.add(document);
    }
    return documents;
  }

  public MongoCollection<Document> newCollection() {
    return fongoRule.newMongoCollection("db");
  }
}
