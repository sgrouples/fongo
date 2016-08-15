package com.github.fakemongo;

import com.github.fakemongo.impl.Util;
import com.github.fakemongo.junit.FongoRule;
import com.google.common.collect.Iterables;
import com.mongodb.AggregationOptions;
import com.mongodb.AggregationOutput;
import com.mongodb.BasicDBObject;
import com.mongodb.Cursor;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MongoException;
import java.util.Arrays;
import java.util.Calendar;
import java.util.List;
import java.util.TimeZone;
import org.assertj.core.api.Assertions;
import org.assertj.core.util.Lists;
import org.junit.Assert;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

// TODO : sum of double value ($sum : 1.3)
// sum of "1" (String) must return 0.

// Handle $group { _id = 0}
public class FongoAggregateTest {

  public final FongoRule fongoRule = new FongoRule(false);

  public final ExpectedException exception = ExpectedException.none();

  @Rule
  public TestRule rules = RuleChain.outerRule(exception).around(fongoRule);

  @Test
  public void shouldHandleUnknownPipeline() {
    ExpectedMongoException.expect(exception, MongoException.class);
    ExpectedMongoException.expectCode(exception, 16436);
    DBObject badsort = new BasicDBObject("_id", 1);

    fongoRule.newCollection().aggregate(Arrays.asList(badsort));
    // Not found : com.mongodb.CommandFailureException: { "serverUsed" : "localhost/127.0.0.1:27017" , "errmsg" : "exception: Unrecognized pipeline stage name: '_id'" , "code" : 16436 , "ok" : 0.0}
  }

  @Test
  public void shouldGenerateErrorOnNonList() {
    //    ExpectedMongoException.expect(exception, MongoException.class);
    //    ExpectedMongoException.expectCode(exception, 15978);
    DBCollection collection = fongoRule.newCollection();
    collection.insert(new BasicDBObject("author", "william").append("tags", "value"));
    DBObject matching = new BasicDBObject("$match", new BasicDBObject("author", "william"));
    DBObject unwind = new BasicDBObject("$unwind", "$tags");

    AggregationOutput output = collection.aggregate(Arrays.asList(matching, unwind));
    DBObject resultAggregate = Iterables.getFirst(output.results(), null);
    assertNotNull(resultAggregate);
    assertEquals("value", resultAggregate.get("tags"));
  }


  @Test
  public void shouldHandleLast() {
    DBCollection collection = createTestCollection();
    DBObject match = new BasicDBObject("$match", new BasicDBObject("myId", new BasicDBObject("$in", Util.list("p1", "p2", "p3"))));
    DBObject sort = new BasicDBObject("$sort", new BasicDBObject("date", 1));
    DBObject group = new BasicDBObject("$group", new BasicDBObject("_id", "0").append("date", new BasicDBObject("$last", "$date")));

    // Aggregate
    AggregationOutput output = collection.aggregate(Arrays.asList(match, sort, group));
    int result = -1;
//    if (output.getCommandResult().ok() && output.getCommandResult().containsField("result")) {
    DBObject resultAggregate = Iterables.getFirst(output.results(), null);
    if (resultAggregate != null && resultAggregate.containsField("date")) {
      result = ((Number) resultAggregate.get("date")).intValue();
    }
//    }

    assertEquals(7, result);
  }

  @Test
  public void shoulHandleLastNullValue() {
    DBCollection collection = createTestCollection();
    DBObject match = new BasicDBObject("$match", new BasicDBObject("myId", new BasicDBObject("$in", Util.list("p4"))));
    DBObject group = new BasicDBObject("$group", new BasicDBObject("_id", "0").append("date", new BasicDBObject("$last", "$date")));

    // Aggregate
    AggregationOutput output = collection.aggregate(Arrays.asList(match, group));
    boolean result = false;

    DBObject resultAggregate = Iterables.getFirst(output.results(), null);
    if (resultAggregate != null && resultAggregate.containsField("date")) {
      assertNull(resultAggregate.get("date"));
      result = true;
    }
    assertTrue("Result not found", result);
  }

  @Test
  public void shouldHandleFirst() {
    DBCollection collection = createTestCollection();
    DBObject match = new BasicDBObject("$match", new BasicDBObject("myId", new BasicDBObject("$in", Util.list("p1", "p0"))));
//    DBObject sort = new BasicDBObject("$sort", new BasicDBObject("date", 1));
    DBObject group = new BasicDBObject("$group", new BasicDBObject("_id", "0").append("date", new BasicDBObject("$first", "$date")));
    AggregationOutput output = collection.aggregate(Arrays.asList(match, group));
    int result = -1;
    DBObject resultAggregate = Iterables.getFirst(output.results(), null);
    if (resultAggregate != null && resultAggregate.containsField("date")) {
      result = ((Number) resultAggregate.get("date")).intValue();
    }

    assertEquals(1, result);
  }

  @Test
  public void shoulHandleFirstNullValue() {
    DBCollection collection = createTestCollection();
    DBObject match = new BasicDBObject("$match", new BasicDBObject("myId", new BasicDBObject("$in", Util.list("p4"))));
    DBObject group = new BasicDBObject("$group", new BasicDBObject("_id", "0").append("date", new BasicDBObject("$first", "$date")));
    AggregationOutput output = collection.aggregate(Arrays.asList(match, group));
    boolean result = false;

    DBObject resultAggregate = Iterables.getFirst(output.results(), null);
    if (resultAggregate != null && resultAggregate.containsField("date")) {
      assertNull(resultAggregate.get("date"));
      result = true;
    }
    assertTrue("Result not found", result);
  }


  @Test
  public void shouldHandleMin() {
    DBCollection collection = createTestCollection();
    DBObject match = new BasicDBObject("$match", new BasicDBObject("myId", new BasicDBObject("$in", Util.list("p0", "p1"))));
    DBObject group = new BasicDBObject("$group", new BasicDBObject("_id", "0").append("date", new BasicDBObject("$min", "$date")));
    AggregationOutput output = collection.aggregate(Arrays.asList(match, group));
    int result = 0;
    DBObject resultAggregate = Iterables.getFirst(output.results(), null);
    if (resultAggregate != null && resultAggregate.containsField("date")) {
      result = ((Number) resultAggregate.get("date")).intValue();
    }

    assertEquals(1, result);
  }

  @Test
  public void shouldHandleMax() {
    DBCollection collection = createTestCollection();
    DBObject match = new BasicDBObject("$match", new BasicDBObject("myId", new BasicDBObject("$in", Util.list("p0", "p1"))));
    DBObject group = new BasicDBObject("$group", new BasicDBObject("_id", "0").append("date", new BasicDBObject("$max", "$date")));
    AggregationOutput output = collection.aggregate(Arrays.asList(match, group));
    int result = 0;
    DBObject resultAggregate = Iterables.getFirst(output.results(), null);
    if (resultAggregate != null && resultAggregate.containsField("date")) {
      result = ((Number) resultAggregate.get("date")).intValue();
    }

    assertEquals(6, result);
  }

  @Test
  public void shouldHandleMaxWithLimit() {
    DBCollection collection = createTestCollection();
    DBObject match = new BasicDBObject("$match", new BasicDBObject("myId", new BasicDBObject("$in", Util.list("p0", "p1"))));
    DBObject limit = new BasicDBObject("$limit", 3);
    DBObject group = new BasicDBObject("$group", new BasicDBObject("_id", "0").append("date", new BasicDBObject("$max", "$date")));
    AggregationOutput output = collection.aggregate(Arrays.asList(match, limit, group));
    int result = 0;
    DBObject resultAggregate = Iterables.getFirst(output.results(), null);
    if (resultAggregate != null && resultAggregate.containsField("date")) {
      result = ((Number) resultAggregate.get("date")).intValue();
    }

    assertEquals(3, result);
  }

  @Test
  public void shouldHandleMinWithSkip() {
    DBCollection collection = createTestCollection();
    DBObject match = new BasicDBObject("$match", new BasicDBObject("myId", new BasicDBObject("$in", Util.list("p0", "p1"))));
    DBObject skip = new BasicDBObject("$skip", 3);
    DBObject group = new BasicDBObject("$group", new BasicDBObject("_id", "0").append("date", new BasicDBObject("$min", "$date")));
    AggregationOutput output = collection.aggregate(Arrays.asList(match, skip, group));
    int result = 0;
    DBObject resultAggregate = Iterables.getFirst(output.results(), null);
    if (resultAggregate != null && resultAggregate.containsField("date")) {
      result = ((Number) resultAggregate.get("date")).intValue();
    }

    assertEquals(4, result);
  }

  @Test
  public void shouldHandleSort() {
    DBCollection collection = createTestCollection();
    DBObject match = new BasicDBObject("$match", new BasicDBObject("myId", new BasicDBObject("$in", Util.list("p1", "p2", "p3"))));
    DBObject sort = new BasicDBObject("$sort", new BasicDBObject("date", 1));
    AggregationOutput output = collection.aggregate(Arrays.asList(match, sort));
    int lastDate = -1;
    for (DBObject result : output.results()) {
      int date = ((Number) result.get("date")).intValue();
      assertTrue(lastDate < date);
      lastDate = date;
    }

    Assert.assertEquals(7, lastDate);
  }

  @Test
  public void shouldUnwindList() {
    DBCollection collection = fongoRule.newCollection();
    collection.insert(new BasicDBObject("_id", 1).append("author", "william").append("tags", Util.list("scala", "java", "mongo")));
    DBObject matching = new BasicDBObject("$match", new BasicDBObject("author", "william"));
    DBObject unwind = new BasicDBObject("$unwind", "$tags");

    AggregationOutput output = collection.aggregate(Arrays.asList(matching, unwind));

    // Assert
    List<DBObject> result = Lists.newArrayList(output.results());
    assertEquals(3, result.size());

    // TODO : remove comment when _id can NOT be unique anymore.
    Assert.assertEquals(fongoRule.parseList("[ { \"_id\" : 1 , \"author\" : \"william\" , \"tags\" : \"scala\"} ," +
        " { \"_id\" : 1 , \"author\" : \"william\" , \"tags\" : \"java\"} ," +
        " { \"_id\" : 1 , \"author\" : \"william\" , \"tags\" : \"mongo\"}]"), result);
    assertEquals("william", Util.extractField(result.get(0), "author"));
    assertEquals("scala", Util.extractField(result.get(0), "tags"));
    assertEquals("william", Util.extractField(result.get(1), "author"));
    assertEquals("java", Util.extractField(result.get(1), "tags"));
    assertEquals("william", Util.extractField(result.get(2), "author"));
    assertEquals("mongo", Util.extractField(result.get(2), "tags"));
  }

  @Test
  public void shouldUnwindEmptyList() {
    DBCollection collection = fongoRule.newCollection();
    collection.insert(new BasicDBObject("author", "william").append("tags", Util.list()));
    DBObject matching = new BasicDBObject("$match", new BasicDBObject("author", "william"));
    DBObject project = new BasicDBObject("$project", new BasicDBObject("author", 1).append("tags", 1));
    DBObject unwind = new BasicDBObject("$unwind", "$tags");

    // Aggregate
    AggregationOutput output = collection.aggregate(Arrays.asList(matching, project, unwind));

    // Assert
    Assertions.assertThat(output.results()).hasSize(0);
  }

  @Test
  public void shouldHandleAvgOfField() {
    DBCollection collection = createTestCollection();
    DBObject match = new BasicDBObject("$match", new BasicDBObject("myId", new BasicDBObject("$in", Util.list("p0", "p1"))));
    DBObject group = new BasicDBObject("$group", new BasicDBObject("_id", null).append("date", new BasicDBObject("$avg", "$date")));

    // Aggregate
    AggregationOutput output = collection.aggregate(Arrays.asList(match, group));
    Number result = -1;
    DBObject resultAggregate = Iterables.getFirst(output.results(), null);
    if (resultAggregate != null && resultAggregate.containsField("date")) {
      result = ((Number) resultAggregate.get("date"));
    }

    assertTrue("must be a Double but was a " + result.getClass(), result instanceof Double);
    Assert.assertEquals(3.5D, result.doubleValue(), 0.00001);
  }

  @Test
  public void shouldHandleAvgOfDoubleField() {
    DBCollection collection = fongoRule.newCollection();
    collection.insert(new BasicDBObject("myId", "p4").append("date", 1D));
    collection.insert(new BasicDBObject("myId", "p4").append("date", 2D));
    collection.insert(new BasicDBObject("myId", "p4").append("date", 3D));
    collection.insert(new BasicDBObject("myId", "p4").append("date", 4D));
    collection.insert(new BasicDBObject("myId", "p4").append("date", 5D));
    collection.insert(new BasicDBObject("myId", "p4").append("date", 6D));
    collection.insert(new BasicDBObject("myId", "p4").append("date", 7D));
    collection.insert(new BasicDBObject("myId", "p4").append("date", 10D));

    DBObject match = new BasicDBObject("$match", new BasicDBObject("myId", "p4"));
    DBObject group = new BasicDBObject("$group", new BasicDBObject("_id", null).append("date", new BasicDBObject("$avg", "$date")));

    // Aggregate
    AggregationOutput output = collection.aggregate(Arrays.asList(match, group));
    Number result = -1;
    DBObject resultAggregate = Iterables.getFirst(output.results(), null);
    if (resultAggregate != null && resultAggregate.containsField("date")) {
      result = ((Number) resultAggregate.get("date"));
    }

    assertTrue("must be a Double but was a " + result.getClass(), result instanceof Double);
    Assert.assertEquals(4.75D, result.doubleValue(), 0.000001);
  }


  @Test
  public void shouldHandleSumOfField() {
    DBCollection collection = createTestCollection();
    DBObject match = new BasicDBObject("$match", new BasicDBObject("myId", new BasicDBObject("$in", Util.list("p0", "p1"))));
    DBObject groupFields = new BasicDBObject("_id", null);
    groupFields.put("date", new BasicDBObject("$sum", "$date"));
    DBObject group = new BasicDBObject("$group", groupFields);

    // Aggregate
    AggregationOutput output = collection.aggregate(Arrays.asList(match, group));
    Number result = -1;
    DBObject resultAggregate = Iterables.getFirst(output.results(), null);
    if (resultAggregate != null && resultAggregate.containsField("date")) {
      result = ((Number) resultAggregate.get("date"));
    }

    assertEquals(21, result);
  }

  // Group with "simple _id"
  @Test
  public void shouldHandleSumOfNumber() {
    DBCollection collection = createTestCollection();
    DBObject match = new BasicDBObject("$match", new BasicDBObject("myId", new BasicDBObject("$in", Util.list("p0", "p1"))));
    DBObject group = new BasicDBObject("$group", new BasicDBObject("_id", null).append("sum", new BasicDBObject("$sum", 2)));

    // Aggregate
    AggregationOutput output = collection.aggregate(Arrays.asList(match, group));

    // Assert
    Number result = -1;
    DBObject resultAggregate = Iterables.getFirst(output.results(), null);
    if (resultAggregate != null && resultAggregate.containsField("sum")) {
      result = ((Number) resultAggregate.get("sum"));
    }

    Assertions.assertThat(result).isEqualTo(14);
  }


  @Test
  public void shouldHandleSumOfNumberOfDouble() {
    DBCollection collection = createTestCollection();
    DBObject match = new BasicDBObject("$match", new BasicDBObject("myId", new BasicDBObject("$in", Util.list("p0", "p1"))));
    DBObject group = new BasicDBObject("$group", new BasicDBObject("_id", null).append("sum", new BasicDBObject("$sum", 2.0)));

    // Aggregate
    AggregationOutput output = collection.aggregate(Arrays.asList(match, group));

    // Assert
    Number result = -1;
    DBObject resultAggregate = Iterables.getFirst(output.results(), null);
    if (resultAggregate != null && resultAggregate.containsField("sum")) {
      result = ((Number) resultAggregate.get("sum"));
    }
    Assertions.assertThat(result).isEqualTo(14.0);
  }


  @Test
  public void shouldHandleSumOfNumberOfLong() {
    DBCollection collection = createTestCollection();
    DBObject match = new BasicDBObject("$match", new BasicDBObject("myId", new BasicDBObject("$in", Util.list("p0", "p1"))));
    DBObject group = new BasicDBObject("$group", new BasicDBObject("_id", null).append("sum", new BasicDBObject("$sum", Integer.MAX_VALUE)));

    // Aggregate
    AggregationOutput output = collection.aggregate(Arrays.asList(match, group));

    // Assert
    Number result = -1;
    DBObject resultAggregate = Iterables.getFirst(output.results(), null);
    if (resultAggregate != null && resultAggregate.containsField("sum")) {
      result = ((Number) resultAggregate.get("sum"));
    }
    Assertions.assertThat(result).isEqualTo(Integer.MAX_VALUE * 7l);
  }


  // Group with "simple _id"
  @Test
  public void shouldHandleSumOfFieldGroupedByMyId() {
    DBCollection collection = createTestCollection();
    DBObject match = new BasicDBObject("$match", new BasicDBObject("myId", new BasicDBObject("$in", Util.list("p0", "p1"))));
    DBObject sort = new BasicDBObject("$sort", new BasicDBObject("_id", 1));
    DBObject group = new BasicDBObject("$group", new BasicDBObject("_id", "$myId").append("count", new BasicDBObject("$sum", "$date")));

    // Aggregate
    AggregationOutput output = collection.aggregate(Arrays.asList(match, group, sort));

    // Assert
    List<DBObject> resultAggregate = Lists.newArrayList(output.results());
    assertEquals(2, resultAggregate.size());
    Assert.assertEquals(Arrays.asList(
        new BasicDBObject("_id", "p0").append("count", 15),
        new BasicDBObject("_id", "p1").append("count", 6)), resultAggregate);
  }

  @Test
  public void shouldHandleSumOfValueGroupedByMyId() {
    DBCollection collection = createTestCollection();
    DBObject match = new BasicDBObject("$match", new BasicDBObject("myId", new BasicDBObject("$in", Util.list("p0", "p1"))));
    DBObject sort = new BasicDBObject("$sort", new BasicDBObject("_id", 1));
    DBObject group = new BasicDBObject("$group", new BasicDBObject("_id", "$myId").append("count", new BasicDBObject("$sum", 2)));

    // Aggregate
    AggregationOutput output = collection.aggregate(Arrays.asList(match, group, sort));

    // Assert
    List<DBObject> resultAggregate = Lists.newArrayList(output.results());
    Assert.assertEquals(Arrays.asList(
        new BasicDBObject("_id", "p0").append("count", 12),
        new BasicDBObject("_id", "p1").append("count", 2)), resultAggregate);
  }

  // see http://stackoverflow.com/questions/8161444/mongodb-getting-list-of-values-by-using-group
  @Test
  public void mustHandlePushInGroup() {
    DBCollection collection = fongoRule.insertJSON(fongoRule.newCollection(),
        "[{\n a_id: 1,\n \"name\": \"n1\"\n},\n" +
            "{\n a_id: 2,\n \"name\": \"n2\"\n},\n" +
            "{\n a_id: 1,\n \"name\": \"n3\"\n},\n" +
            "{\n a_id: 1,\n \"name\": \"n4\"\n},\n" +
            "{\n a_id: 2,\n \"name\": \"n5\"\n}]"
    );

    DBObject group = fongoRule.parseDBObject("{$group: { '_id': '$a_id', 'name': { $push: '$name'}}}");

    // Aggregate
    AggregationOutput output = collection.aggregate(Arrays.asList(group, new BasicDBObject("$sort", new BasicDBObject("_id", 1))));

    // Assert
    List<DBObject> resultAggregate = Lists.newArrayList(output.results());
    assertEquals(fongoRule.parseList("[ " +
        "{ \"_id\" : 1 , \"name\" : [ \"n1\" , \"n3\" , \"n4\"]} , " +
        "{ \"_id\" : 2 , \"name\" : [ \"n2\" , \"n5\"]}]"), resultAggregate);
  }

  @Test
  public void mustHandlePushInGroupWithSameValue() {
    DBCollection collection = fongoRule.insertJSON(fongoRule.newCollection(),
        "[{\n a_id: 1,\n \"name\": \"n1\"\n},\n" +
            "{\n a_id: 2,\n \"name\": \"n5\"\n},\n" +
            "{\n a_id: 1,\n \"name\": \"n1\"\n},\n" +
            "{\n a_id: 1,\n \"name\": \"n2\"\n},\n" +
            "{\n a_id: 2,\n \"name\": \"n5\"\n}]\n"
    );

    DBObject group = fongoRule.parseDBObject("{$group: { '_id': '$a_id', 'name': { $push: '$name'}}}");

    // Aggregate
    AggregationOutput output = collection.aggregate(Arrays.asList(group, new BasicDBObject("$sort", new BasicDBObject("_id", 1))));

    // Assert
    List<DBObject> resultAggregate = Lists.newArrayList(output.results());
    assertEquals(fongoRule.parseList("[ " +
        "{ \"_id\" : 1 , \"name\" : [ \"n1\" , \"n1\" , \"n2\"]} , " +
        "{ \"_id\" : 2 , \"name\" : [ \"n5\" , \"n5\"]}]"), resultAggregate);
  }

  @Test
  public void mustHandleAddToSetInGroup() {
    DBCollection collection = fongoRule.insertJSON(fongoRule.newCollection(),
        "[{\n a_id: 1,\n \"name\": \"n1\"\n},\n" +
            "{\n a_id: 2,\n \"name\": \"n2\"\n},\n" +
            "{\n a_id: 1,\n \"name\": \"n3\"\n},\n" +
            "{\n a_id: 1,\n \"name\": \"n4\"\n},\n" +
            "{\n a_id: 2,\n \"name\": \"n5\"\n}]"
    );

    DBObject group = fongoRule.parseDBObject("{$group: { '_id': '$a_id', 'name': { $push: '$name'}}}");

    // Aggregate
    AggregationOutput output = collection.aggregate(Arrays.asList(group, new BasicDBObject("$sort", new BasicDBObject("_id", 1))));

    // Assert
    List<DBObject> resultAggregate = Lists.newArrayList(output.results());
    assertEquals(fongoRule.parseList("[ " +
        "{ \"_id\" : 1 , \"name\" : [ \"n1\" , \"n3\" , \"n4\"]} , " +
        "{ \"_id\" : 2 , \"name\" : [ \"n2\" , \"n5\"]}]"), resultAggregate);
  }

  @Test
  public void mustHandleAddToSetInGroupWithSameValue() {
    DBCollection collection = fongoRule.insertJSON(fongoRule.newCollection(),
        "[{\n a_id: 1,\n \"name\": \"n1\"\n},\n" +
            "{\n a_id: 2,\n \"name\": \"n5\"\n},\n" +
            "{\n a_id: 1,\n \"name\": \"n1\"\n},\n" +
            "{\n a_id: 1,\n \"name\": \"n2\"\n},\n" +
            "{\n a_id: 2,\n \"name\": \"n5\"\n}]"
    );

    DBObject group = fongoRule.parseDBObject("{$group: { '_id': '$a_id', 'name': { $addToSet: '$name'}}}");

    // Aggregate
    AggregationOutput output = collection.aggregate(Arrays.asList(group, new BasicDBObject("$sort", new BasicDBObject("_id", 1))));

    // Assert
    List<DBObject> resultAggregate = Lists.newArrayList(output.results());

    // Take care, order is not same on MongoDB (n2,n1) // TODO
    Assertions.assertThat(resultAggregate).isEqualTo(fongoRule.parseList("[ " +
        "{ \"_id\" : 1 , \"name\" : [ \"n1\" , \"n2\"]} , " +
        "{ \"_id\" : 2 , \"name\" : [ \"n5\"]}]"));
  }

  @Test
  public void should_addToSet_extractField_with_index() {
    DBCollection collection = fongoRule.insertJSON(fongoRule.newCollection(),
        "[{uuid:\"2\", version:1, _class:\"A\"},{uuid:\"2\", version:2, _class:\"B\"}]"
    );
    collection.createIndex(new BasicDBObject("uuid", 1).append("version", 1), null, true);

    DBObject group = fongoRule.parseDBObject("{$group: { '_id': '$uuid', 'event': { $addToSet: '$_class'}}}");

    // Aggregate
    AggregationOutput output = collection.aggregate(Arrays.asList(group, new BasicDBObject("$sort", new BasicDBObject("_id", 1))));

    // Assert
    List<DBObject> resultAggregate = Lists.newArrayList(output.results());

    // Take care, order is not same on MongoDB (B,A)
    Assertions.assertThat(resultAggregate).isEqualTo(fongoRule.parseList("[ { \"_id\" : \"2\" , \"event\" : [ \"A\" , \"B\"]}]"));
  }

  @Test
  public void should_addToSet_extractField_with_index2() {
    DBCollection collection = fongoRule.insertJSON(fongoRule.newCollection("versioning"),
        "[{ \"_id\" : { \"$oid\" : \"5323764d210663e62bdc69b9\"} , \"_class\" : \"LogAlertCreated\" ," +
            " \"uuidAlert\" : \"2\" , \"version\" : 1 , \"createdAt\" : 1394832973819 , " +
            "\"alert\" : { \"uuid\" : { \"$uuid\" : \"951a2f50-586d-4bef-83c5-22f68354f4d0\"} ," +
            " \"senderMessage\" : \"user\" , \"title\" : \"title\" , \"category\" : 12 , \"severity\" : 6 ," +
            " \"message\" : \"message\" , \"start\" : 1394832973799 , \"area\" : { \"_class\" : \"zone.CircleZone\" ," +
            " \"center\" : { \"_class\" : \"com.deveryware.mpa.model.v1.Coordinate\" , \"latitude\" : 42.2 , " +
            "\"longitude\" : 38.2} , \"radius\" : 12.0}}}, { \"_id\" : { \"$oid\" : \"5323764d210663e62bdc69ba\"} ," +
            " \"_class\" : \"LogAlertCreated\" , \"uuidAlert\" : \"1\" , \"version\" : 1 ," +
            " \"createdAt\" : 1394832973907 , \"alert\" : { \"uuid\" : { \"$uuid\" : \"406a76c0-cdc3-4de0-a8aa-aae6211dc01e\"} ," +
            " \"senderMessage\" : \"user\" , \"title\" : \"title\" , \"category\" : 12 , \"severity\" : 6 ," +
            " \"message\" : \"message\" , \"start\" : 1394832973907 , \"area\" : { \"_class\" : \"zone.CircleZone\" ," +
            " \"center\" : { \"_class\" : \"com.deveryware.mpa.model.v1.Coordinate\" , \"latitude\" : 42.2 ," +
            " \"longitude\" : 38.2} , \"radius\" : 12.0}}}, { \"_id\" : { \"$oid\" : \"5323764d210663e62bdc69bb\"} ," +
            " \"_class\" : \"LogAlertMessageModified\" , \"uuidAlert\" : \"1\" , \"version\" : 3 , " +
            "\"createdAt\" : 1394832973917 , \"message\" : \"notification3\"}, " +
            "{ \"_id\" : { \"$oid\" : \"5323764d210663e62bdc69bc\"} , \"_class\" : \"LogAlertMessageModified\" , " +
            "\"uuidAlert\" : \"1\" , \"version\" : 2 , \"createdAt\" : 1394832973933 , \"message\" : \"notification2\"}, " +
            "{ \"_id\" : { \"$oid\" : \"5323764d210663e62bdc69bd\"} , \"_class\" : \"LogAlertEnded\" , \"uuidAlert\" : \"1\" ," +
            " \"version\" : 4 , \"createdAt\" : 1394832973946}, { \"_id\" : { \"$oid\" : \"5323764d210663e62bdc69be\"} ," +
            " \"_class\" : \"LogAlertMessageModified\" , \"uuidAlert\" : \"2\" , \"version\" : 3 , \"createdAt\" : 1394832973959 ," +
            " \"message\" : \"modif3\"}, { \"_id\" : { \"$oid\" : \"5323764d210663e62bdc69bf\"} ," +
            " \"_class\" : \"LogAlertMessageModified\" , \"uuidAlert\" : \"2\" , \"version\" : 2 , " +
            "\"createdAt\" : 1394832973969 , \"message\" : \"modif2\"}]"
    );
    collection.createIndex(new BasicDBObject("uuidAlert", 1).append("version", 1), new BasicDBObject("unique", Boolean.TRUE));

    DBObject group = fongoRule.parseDBObject("{$group: { '_id': '$uuidAlert', 'events': { $addToSet: '$_class'}}}");

    // Aggregate
    AggregationOutput output = collection.aggregate(Arrays.asList(group, new BasicDBObject("$sort", new BasicDBObject("_id", 1))));

    // Assert
    List<DBObject> resultAggregate = Lists.newArrayList(output.results());

    // Take care, order is not same on MongoDB (n2,n1)
    Assertions.assertThat(resultAggregate).isEqualTo(fongoRule.parseList("[ { \"_id\" : \"1\" , \"events\" : [ \"LogAlertCreated\" , \"LogAlertMessageModified\" , \"LogAlertEnded\"]} , { \"_id\" : \"2\" , \"events\" : [ \"LogAlertCreated\" , \"LogAlertMessageModified\"]}]"));
  }

  // See https://github.com/fakemongo/fongo/issues/45
  @Test
  public void should_$sum_return_long() {
    DBCollection collection = createTestCollection();

    DBObject groupFields = new BasicDBObject("_id", null).append("count", new BasicDBObject("$sum", 1L));
    DBObject group = new BasicDBObject("$group", groupFields);
    AggregationOutput output = collection.aggregate(Lists.newArrayList(group));

    Assertions.assertThat(output.results()).hasSize(1);
    Assertions.assertThat(output.results().iterator().next().get("count")).isEqualTo(10L);
  }

  //  @Ignore("soon")
  @Test
  public void should_$group_contains__id() {
    DBCollection collection = createTestCollection();

    DBObject groupFields = new BasicDBObject("count", new BasicDBObject("$sum", 1L));
    DBObject group = new BasicDBObject("$group", groupFields);

    ExpectedMongoException.expect(exception, MongoException.class);
    ExpectedMongoException.expectCode(exception, 15955);
    exception.expectMessage("a group specification must include an _id");
    collection.aggregate(Lists.newArrayList(group));
  }

  // See https://github.com/fakemongo/fongo/issues/152
  @Ignore
  @Test
  public void should_$cond_return_value() {
    // Given (https://docs.mongodb.org/manual/reference/operator/aggregation/cond/)
    final DBCollection collection = fongoRule.newCollection();
    fongoRule.insertJSON(collection, "[{ \"_id\" : 1, \"item\" : \"abc1\", qty: 300 }," +
        "{ \"_id\" : 2, \"item\" : \"abc2\", qty: 200 }," +
        "{ \"_id\" : 3, \"item\" : \"xyz1\", qty: 250 }]");

    // When
    final AggregationOutput aggregate = collection.aggregate(fongoRule.parseList("   [\n" +
        "      {\n" +
        "         $project:\n" +
        "           {\n" +
        "             item: 1,\n" +
        "             discount:\n" +
        "               {\n" +
        "                 $cond: { if: { $gte: [ \"$qty\", 250 ] }, then: 30, else: 20 }\n" +
        "               }\n" +
        "           }\n" +
        "      }\n" +
        "   ]\n"));

    // Then
    Assertions.assertThat(aggregate.results()).containsExactlyElementsOf(fongoRule.parseList("[{ \"_id\" : 1, \"item\" : \"abc1\", \"discount\" : 30 }," +
        "{ \"_id\" : 2, \"item\" : \"abc2\", \"discount\" : 20 }," +
        "{ \"_id\" : 3, \"item\" : \"xyz1\", \"discount\" : 30 }]"));
  }

  // See https://github.com/fakemongo/fongo/issues/152
  @Ignore
  @Test
  public void should_$cond_in_simple_form_return_value() {
    // Given (https://docs.mongodb.org/manual/reference/operator/aggregation/cond/)
    final DBCollection collection = fongoRule.newCollection();
    fongoRule.insertJSON(collection, "[{ \"_id\" : 1, \"item\" : \"abc1\", qty: 300 }," +
        "{ \"_id\" : 2, \"item\" : \"abc2\", qty: 200 }," +
        "{ \"_id\" : 3, \"item\" : \"xyz1\", qty: 250 }]");

    // When
    final AggregationOutput aggregate = collection.aggregate(fongoRule.parseList("[\n" +
        "      {\n" +
        "         $project:\n" +
        "           {\n" +
        "             item: 1,\n" +
        "             discount:\n" +
        "               {\n" +
        "                 $cond: [ { $gte: [ \"$qty\", 250 ] }, 30, 20 ]\n" +
        "               }\n" +
        "           }\n" +
        "      }\n" +
        "   ]"));

    // Then
    Assertions.assertThat(aggregate.results()).containsExactlyElementsOf(fongoRule.parseList("[{ \"_id\" : 1, \"item\" : \"abc1\", \"discount\" : 30 }," +
        "{ \"_id\" : 2, \"item\" : \"abc2\", \"discount\" : 20 }," +
        "{ \"_id\" : 3, \"item\" : \"xyz1\", \"discount\" : 30 }]"));
  }

  @Test
  public void should_$ifnull_return_value() {
    // Given (https://docs.mongodb.org/manual/reference/operator/aggregation/ifNull/#exp._S_ifNull)
    final DBCollection collection = fongoRule.newCollection();
    fongoRule.insertJSON(collection, "[{ \"_id\" : 1, \"item\" : \"abc1\", description: \"product 1\", qty: 300 }," +
        "{ \"_id\" : 2, \"item\" : \"abc2\", description: null, qty: 200 }," +
        "{ \"_id\" : 3, \"item\" : \"xyz1\", qty: 250 }]");

    // When
    final AggregationOutput aggregate = collection.aggregate(fongoRule.parseList("[\n" +
        "      {\n" +
        "         $project: {\n" +
        "            item: 1,\n" +
        "            description: { $ifNull: [ \"$description\", \"Unspecified\" ] }\n" +
        "         }\n" +
        "      }\n" +
        "   ]"));

    // Then
    Assertions.assertThat(aggregate.results()).containsExactlyElementsOf(fongoRule.parseList("[{ \"_id\" : 1, \"item\" : \"abc1\", \"description\" : \"product 1\" }," +
        "{ \"_id\" : 2, \"item\" : \"abc2\", \"description\" : \"Unspecified\" }," +
        "{ \"_id\" : 3, \"item\" : \"xyz1\", \"description\" : \"Unspecified\" }\n]"));
  }

  // https://github.com/fakemongo/fongo/issues/163
  @Test
  public void should_date_return_value() {
    // Given
    final DBCollection collection = fongoRule.newCollection();
    collection.insert(new BasicDBObject("t", new java.util.Date()));

    // When
    BasicDBObject obj = new BasicDBObject("$group",
        new BasicDBObject("_id", new BasicDBObject("day", new BasicDBObject("$dayOfMonth", "$t"))));
    AggregationOutput ao = collection.aggregate(Arrays.asList(obj));

    // Then
    Assertions.assertThat(ao.results()).contains(new BasicDBObject("_id", new BasicDBObject("day", Calendar.getInstance(TimeZone.getTimeZone("GMT")).get(Calendar.DAY_OF_MONTH))));
  }

  // https://github.com/fakemongo/fongo/issues/163
  @Test
  public void should_multiple_date_return_value() {
    // Given
    final DBCollection collection = fongoRule.newCollection();
    collection.insert(new BasicDBObject("t", new java.util.Date()));

    // When
    BasicDBObject obj = new BasicDBObject("$group",
        new BasicDBObject("_id", new BasicDBObject("day", new BasicDBObject("$dayOfMonth", "$t")).append("month", new BasicDBObject("$month", "$t"))));
    AggregationOutput ao = collection.aggregate(Arrays.asList(obj));

    // Then
    Assertions.assertThat(ao.results()).contains(new BasicDBObject("_id", new BasicDBObject("day", Calendar.getInstance(TimeZone.getTimeZone("GMT")).get(Calendar.DAY_OF_MONTH)).append("month", Calendar.getInstance().get(Calendar.MONTH) + 1)));
  }

  // https://github.com/fakemongo/fongo/issues/163
  @Test
  public void should_invalid_keyword_send_error() {
    // Given
    final DBCollection collection = fongoRule.newCollection();
    collection.insert(new BasicDBObject("t", new java.util.Date()));

    // When
    BasicDBObject obj = new BasicDBObject("$group",
        new BasicDBObject("_id", new BasicDBObject("day", new BasicDBObject("$NOTEXIST", "$t"))));
    ExpectedMongoException.expect(exception, MongoException.class);
    ExpectedMongoException.expectCode(exception, 15999);
    exception.expectMessage("invalid operator '$NOTEXIST'");
    collection.aggregate(Arrays.asList(obj));
  }

  @Test
  public void should_specify_attribute_multiple_time() {
    // Given
    DBCollection collection = fongoRule.insertJSON(fongoRule.newCollection(),
        "[{" +
            "    _id: \"552f80885b663d29f1026376\",\n" +
            "    startDate: \"2015-03-30T00:00:00Z\",\n" +
            "    endDate: \"2015-03-30T00:05:00Z\",\n" +
            "    message: \"hi there\"" +
            "}]");

    DBObject pipeline = fongoRule.parseDBObject(
        "{ \"$project\": { " +
            "a: \"$startDate\", " +
            "b: \"$startDate\", " +
            "c: \"$startDate\", " +
            "d: \"$endDate\" " +
            "}}");

    // Aggregate
    AggregationOutput output = collection.aggregate(Arrays.asList(pipeline));
    // Assert

    List<DBObject> resultAggregate = Lists.newArrayList(output.results());
    assertEquals(fongoRule.parseList("[{ \"_id\" : \"552f80885b663d29f1026376\" , " +
        "\"a\" : \"2015-03-30T00:00:00Z\" , " +
        "\"b\" : \"2015-03-30T00:00:00Z\" , " +
        "\"c\" : \"2015-03-30T00:00:00Z\" , " +
        "\"d\" : \"2015-03-30T00:05:00Z\"}]"), resultAggregate);
  }

  // https://github.com/fakemongo/fongo/issues/202
  @Test
  public void should_cursor_next_works() {
    // Given
    final DBCollection collection = fongoRule.newCollection();
    AggregationOptions options = AggregationOptions.builder()
        .outputMode(AggregationOptions.OutputMode.CURSOR).batchSize(100)
        .allowDiskUse(true).build();
    collection.insert(new BasicDBObject("t", new java.util.Date()));

    // When
    BasicDBObject obj = new BasicDBObject("$group",
        new BasicDBObject("_id", new BasicDBObject("day", new BasicDBObject("$dayOfMonth", "$t"))));
    Cursor cursor = collection.aggregate(Arrays.asList(obj), options);

    // Then
    Assertions.assertThat(cursor.hasNext()).isTrue();
    Assertions.assertThat(cursor.next()).isEqualTo(new BasicDBObject("_id", new BasicDBObject("day", Calendar.getInstance(TimeZone.getTimeZone("GMT")).get(Calendar.DAY_OF_MONTH))));
  }

  // https://github.com/fakemongo/fongo/issues/220
  // https://docs.mongodb.com/manual/reference/operator/aggregation/sample/
  @Test
  public void should_$sample_return_random_documents() {
    // Given
    final DBCollection collection = fongoRule.newCollection();
    AggregationOptions options = AggregationOptions.builder()
        .outputMode(AggregationOptions.OutputMode.CURSOR).batchSize(100)
        .allowDiskUse(true).build();
    final List<DBObject> dbObjects = fongoRule.parseList("[{ \"_id\" : 1, \"name\" : \"dave123\", \"q1\" : true, \"q2\" : true },\n" +
        "{ \"_id\" : 2, \"name\" : \"dave2\", \"q1\" : false, \"q2\" : false  },\n" +
        "{ \"_id\" : 3, \"name\" : \"ahn\", \"q1\" : true, \"q2\" : true  },\n" +
        "{ \"_id\" : 4, \"name\" : \"li\", \"q1\" : true, \"q2\" : false  },\n" +
        "{ \"_id\" : 5, \"name\" : \"annT\", \"q1\" : false, \"q2\" : true  },\n" +
        "{ \"_id\" : 6, \"name\" : \"li\", \"q1\" : true, \"q2\" : true  },\n" +
        "{ \"_id\" : 7, \"name\" : \"ty\", \"q1\" : false, \"q2\" : true  }]");
    collection.insert(dbObjects);

    // When
    BasicDBObject obj = new BasicDBObject("$sample",
        new BasicDBObject("size", 3));
    Cursor cursor = collection.aggregate(Arrays.asList(obj), options);

    // Then
    List<DBObject> resultAggregate = Lists.newArrayList(cursor);
    Assertions.assertThat(resultAggregate).hasSize(3).doesNotContainNull();
    for (DBObject dbObject : resultAggregate) {
      Assertions.assertThat(dbObjects).contains(dbObject);
    }
  }

  private DBCollection createTestCollection() {
    DBCollection collection = fongoRule.newCollection();
    collection.insert(new BasicDBObject("myId", "p0").append("date", 1));
    collection.insert(new BasicDBObject("myId", "p0").append("date", 2));
    collection.insert(new BasicDBObject("myId", "p0").append("date", 3));
    collection.insert(new BasicDBObject("myId", "p0").append("date", 4));
    collection.insert(new BasicDBObject("myId", "p0").append("date", 5));
    collection.insert(new BasicDBObject("myId", "p1").append("date", 6));
    collection.insert(new BasicDBObject("myId", "p2").append("date", 7));
    collection.insert(new BasicDBObject("myId", "p3").append("date", 0));
    collection.insert(new BasicDBObject("myId", "p0"));
    collection.insert(new BasicDBObject("myId", "p4"));
    return collection;
  }
}
