package com.github.fakemongo;

import com.github.fakemongo.impl.Util;
import com.github.fakemongo.junit.FongoRule;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MapReduceCommand;
import com.mongodb.MapReduceOutput;
import com.mongodb.util.FongoJSON;
import java.io.IOException;
import static java.util.Arrays.asList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.assertj.core.api.Assertions;
import static org.junit.Assert.assertEquals;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FongoMapReduceTest {
  private static final Logger LOG = LoggerFactory.getLogger(FongoMapReduceTest.class);

  public final FongoRule fongoRule = new FongoRule(false);

  public final ExpectedException exception = ExpectedException.none();

  @Rule
  public TestRule rules = RuleChain.outerRule(exception).around(fongoRule);


  // see http://no-fucking-idea.com/blog/2012/04/01/using-map-reduce-with-mongodb/
  @Test
  public void testMapReduceSimple() {
    DBCollection coll = newCollectionWithUrls();


    String map = "function(){    emit(this.url, 1);  };";
    String reduce = "function(key, values){    var res = 0.0;    values.forEach(function(v){ res += 1.0});    return {count: res};  };";
    final MapReduceOutput result = coll.mapReduce(map, reduce, "result", new BasicDBObject());
    Assertions.assertThat(result.getDuration()).isGreaterThanOrEqualTo(0);
    Assertions.assertThat(result.getEmitCount()).isEqualTo(5);
    Assertions.assertThat(result.getOutputCount()).isEqualTo(2);
    Assertions.assertThat(result.getInputCount()).isEqualTo(5);


    List<DBObject> results = fongoRule.newCollection("result").find().toArray();
    assertEquals(fongoRule.parse("[{ \"_id\" : \"www.google.com\" , \"value\" : { \"count\" : 2.0}}, { \"_id\" : \"www.no-fucking-idea.com\" , \"value\" : { \"count\" : 3.0}}]"), results);
  }

  @Test
  public void testMapReduceEmitObject() {
    DBCollection coll = newCollectionWithUrls();


    String map = "function(){    emit({url: this.url}, 1);  };";
    String reduce = "function(key, values){    var res = 0;    values.forEach(function(v){ res += 1});    return {count: res};  };";
    coll.mapReduce(map, reduce, "result", new BasicDBObject());

    List<DBObject> results = fongoRule.newCollection("result").find().toArray();
    assertEquals(fongoRule.parse("[{\"_id\" : {url: \"www.google.com\"} , \"value\" : { \"count\" : 2.0}}, { \"_id\" : {url: \"www.no-fucking-idea.com\"} , \"value\" : { \"count\" : 3.0}}]"), results);
  }

  @Test
  public void testMapReduceJoinOneToMany() {
    DBCollection trash = fongoRule.newCollection();
    DBCollection dates = fongoRule.newCollection();

    fongoRule.insertJSON(trash, "[" +
        " {date: \"1\", trash_data: \"13\" },\n" +
        " {date: \"1\", trash_data: \"1\" },\n" +
        " {date: \"2\", trash_data: \"100\" },\n" +
        " {date: \"2\", trash_data: \"256\" }]");
    fongoRule.insertJSON(dates, "[{date: \"1\", dateval: \"10\"}, {date:\"2\", dateval: \"20\"}]");

    String mapTrash = "function() {    emit({date: this.date}, {value : [{dateval: null, trash_data:this.trash_data, date:this.date}]});};";
    String mapDates = "function() {  emit({date:this.date}, {value : [{dateval:this.dateval}]});};";
    String reduce = "function(key, values){" +
        "    var dateval = null;\n" +
        "    for (var j in values) {\n" +
        "        var valArr = values[j].value;\n" +
        "        for (var jj in valArr) {\n" +
        "            var value = valArr[jj];\n" +
        "            if (value.dateval !== null) {\n" +
        "                dateval = value.dateval;\n" +
        "            }\n" +
        "        }\n" +
        "    }" +
        "    var outValues = [];\n" +
        "    for (var j in values) {\n" +
        "        var valArr = values[j].value;\n" +
        "        for (var jj in valArr) {\n" +
        "            var orig = valArr[jj];\n" +
        "            var copy = {};\n" +
        "            for (var jjj in orig) {\n" +
        "             copy[jjj] = orig[jjj];\n" +
        "            }\n" +
        "            if (dateval !== null) {\n" +
        "                copy[\"dateval\"] = dateval;\n" +
        "            }\n" +
        "            outValues.push(copy);\n" +
        "        }\n" +
        "    }\n" +
        "    return {value:outValues};" +
        "};";
    trash.mapReduce(mapTrash, reduce, "result", MapReduceCommand.OutputType.REDUCE, new BasicDBObject());
    dates.mapReduce(mapDates, reduce, "result", MapReduceCommand.OutputType.REDUCE, new BasicDBObject());
    List<DBObject> results = fongoRule.newCollection("result").find().toArray();
    Map<String, DBObject> byId = new HashMap<String, DBObject>();
    for (DBObject res : results) {
      byId.put(FongoJSON.serialize(res.get("_id")), res);
    }
    List<DBObject> expected = fongoRule.parse("[" +
        "{\"_id\":{\"date\":\"1\"}, \"value\":" +
        "{\"value\":[{\"dateval\":\"10\"}, {\"trash_data\":\"13\", \"date\":\"1\", \"dateval\":\"10\"}, " +
        "{\"trash_data\":\"1\", \"date\":\"1\", \"dateval\":\"10\"}]}}, " +
        "{\"_id\":{\"date\":\"2\"}, \"value\":" +
        "{\"value\":[{\"dateval\":\"20\"}, {\"trash_data\":\"100\", \"date\":\"2\", \"dateval\":\"20\"}, " +
        "{\"trash_data\":\"256\", \"date\":\"2\", \"dateval\":\"20\"}]}}]");
    for (DBObject e : expected) {
      List<DBObject> values = (List<DBObject>) (((DBObject) e.get("value")).get("value"));
      DBObject id = (DBObject) e.get("_id");
      DBObject actual = byId.get(FongoJSON.serialize(id));
      List<DBObject> actualValues = (List<DBObject>) (((DBObject) actual.get("value")).get("value"));
      Assertions.assertThat(actualValues).containsAll(values);
      Assertions.assertThat(actualValues.size()).isEqualTo(values.size());
    }
    Assertions.assertThat(expected.size()).isEqualTo(results.size());
  }

  // see http://no-fucking-idea.com/blog/2012/04/01/using-map-reduce-with-mongodb/
  @Test
  public void testMapReduceWithArray() {
    DBCollection coll = fongoRule.newCollection();
    fongoRule.insertJSON(coll, "[{url: \"www.google.com\", date: 1, trash_data: 5, arr: ['a',2] },\n" +
        " {url: \"www.no-fucking-idea.com\", date: 1, trash_data: 13, arr: ['b',4] },\n" +
        " {url: \"www.google.com\", date: 1, trash_data: 1, arr: ['c',6] },\n" +
        " {url: \"www.no-fucking-idea.com\", date: 2, trash_data: 69, arr: ['d',8] },\n" +
        " {url: \"www.no-fucking-idea.com\", date: 2, trash_data: 256, arr: ['e',10] }]");


    String map = "function(){    emit(this.url, this.arr);  };";
    String reduce = "function(key, values){    var res = [];    values.forEach(function(v){ res = res.concat(v); });    return {mergedArray: res};  };";
    coll.mapReduce(map, reduce, "result", new BasicDBObject());

    List<DBObject> results = fongoRule.newCollection("result").find().toArray();
    assertEquals(fongoRule.parse("[{ \"_id\" : \"www.google.com\" , \"value\" : { \"mergedArray\" : [\"a\",2.0,\"c\",6.0]}}, " +
        "{ \"_id\" : \"www.no-fucking-idea.com\" , \"value\" : { \"mergedArray\" : [\"b\",4.0,\"d\",8.0,\"e\",10.0]}}]"), results);
  }

  @Test
  public void should_use_outputdb() {
    DBCollection coll = newCollectionWithUrls();


    String map = "function(){    emit(this.url, 1);  };";
    String reduce = "function(key, values){    var res = 0;    values.forEach(function(v){ res += 1});    return {count: res};  };";
    MapReduceCommand mapReduceCommand = new MapReduceCommand(coll, map, reduce, "result", MapReduceCommand.OutputType.MERGE, new BasicDBObject());
    mapReduceCommand.setOutputDB("otherColl");
    coll.mapReduce(mapReduceCommand);

    List<DBObject> results = fongoRule.getDb("otherColl").getCollection("result").find().toArray();
    assertEquals(fongoRule.parse("[{ \"_id\" : \"www.google.com\" , \"value\" : { \"count\" : 2.0}}, { \"_id\" : \"www.no-fucking-idea.com\" , \"value\" : { \"count\" : 3.0}}]"), results);
  }


  @Test
  public void testZipMapReduce() throws IOException {
    DBCollection coll = fongoRule.newCollection();
    fongoRule.insertFile(coll, "/zips.json");

    String map = "function () {\n" +
        "    var pitt = [-80.064879, 40.612044];\n" +
        "    var phil = [-74.978052, 40.089738];\n" +
        "\n" +
        "    function distance(a, b) {\n" +
        "        var dx = a[0] - b[0];\n" +
        "        var dy = a[1] - b[1];\n" +
        "        return Math.sqrt(dx * dx + dy * dy);\n" +
        "    }\n" +
        "\n" +
        "    if (distance(this.loc, pitt) < distance(this.loc, phil)) {\n" +
        "        emit(\"pitt\",1);\n" +
        "    } else {\n" +
        "        emit(\"phil\",1);\n" +
        "    }\n" +
        "}\n";
    String reduce = "function(name, values) { return Array.sum(values); };";

    MapReduceOutput output = coll.mapReduce(map, reduce, "resultF", new BasicDBObject("state", "MA"));

    List<DBObject> results = output.getOutputCollection().find().toArray();

    assertEquals(fongoRule.parse("[{ \"_id\" : \"phil\" , \"value\" : 474.0}]"), results);
  }

  /**
   * Inline output = in memory.
   */
  @Test
  public void testMapReduceInline() {
    // Given
    DBCollection coll = newCollectionWithUrls();

    // When
    String map = "function(){    emit(this.url, 1);  };";
    String reduce = "function(key, values){    var res = 0;    values.forEach(function(v){ res += 1});    return {count: res};  };";
    MapReduceOutput output = coll.mapReduce(map, reduce, null, MapReduceCommand.OutputType.INLINE, new BasicDBObject());

    // Then
    assertEquals(fongoRule.parse("[{ \"_id\" : \"www.google.com\" , \"value\" : { \"count\" : 2.0}}, { \"_id\" : \"www.no-fucking-idea.com\" , \"value\" : { \"count\" : 3.0}}]"), output.results());
  }

  @Test
  public void testMapReduceMapInError() {
    ExpectedMongoException.expectMongoCommandException(exception, 16722);
    DBCollection coll = newCollectionWithUrls();


    String map = "function(){    ;";
    String reduce = "function(key, values){    var res = 0;    values.forEach(function(v){ res += 1});    return {count: res};  };";
    coll.mapReduce(map, reduce, "result", new BasicDBObject());
  }

  @Test
  public void testMapReduceReduceInError() {
    ExpectedMongoException.expectMongoCommandException(exception, 16722);
    DBCollection coll = newCollectionWithUrls();


    String map = "function(){    emit(this.url, 1);  };";
    String reduce = "function(key, values){    values.forEach(function(v){ res += 1});    return {count: res};  };";
    coll.mapReduce(map, reduce, "result", new BasicDBObject());
  }

  /**
   * http://exceptionallyexceptionalexceptions.blogspot.fr/2012/03/mongodb-mapreduce-scope-variables.html
   */
  @Test
  public void should_scope_permit_to_initialize() {
    DBCollection coll = fongoRule.newCollection();

    fongoRule.insertJSON(coll, "[{\n" +
        "\t\"_id\": \"4f0c56f1b8eea0b686189c90\",\n" +
        "\t\"meh\": \"meh\",\n" +
        "\t\"feh\": \"feh\",\n" +
        "\t\"arrayOfStuff\": [\n" +
        "\t\t{\n" +
        "\t\t\t\"name\": \"Elgin City\",\n" +
        "\t\t\t\"date\": \"100\"\n" +
        "\t\t},\n" +
        "\t\t{\n" +
        "\t\t\t\"name\": \"Rangers\",\n" +
        "\t\t\t\"date\": \"200\"\n" +
        "\t\t},\n" +
        "\t\t{\n" +
        "\t\t\t\"name\": \"Arsenal\",\n" +
        "\t\t\t\"date\": \"300\"\n" +
        "\t\t}\n" +
        "\t]\n" +
        "},\n" +
        "{\n" +
        "\t\"_id\": \"4f0c56f1b8eea0b686189c99\",\n" +
        "\t\"meh\": \"meh meh meh meh\",\n" +
        "\t\"feh\": \"feh feh feh feh feh feh\",\n" +
        "\t\"arrayOfStuff\": [\n" +
        "\t\t{\n" +
        "\t\t\t\"name\": \"Satriani\",\n" +
        "\t\t\t\"date\": \"100\"\n" +
        "\t\t\t},\n" +
        "\t\t{\n" +
        "\t\t\t\"name\": \"Vai\",\n" +
        "\t\t\t\"date\": \"200\"\n" +
        "\t\t},\n" +
        "\t\t{\n" +
        "\t\t\t\"name\": \"Johnson\",\n" +
        "\t\t\t\"date\": \"300\"\n" +
        "\t\t}\n" +
        "\t]\n" +
        "}]");

    String map = "function() {" +
        "if(this.arrayOfStuff) {" +
        "this.arrayOfStuff.forEach(function(stuff) {" +
        "if(stuff.date > from && stuff.date < to) {" +
        "emit({day: stuff.date}, {count:1});" +
        "}" +
        "});" +
        "}" +
        "};";

    String reduce = "function(key , values) {" +
        "var total = 0;" +
        "values.forEach(function(v) {" +
        "total += v.count;" +
        "});" +
        "return {count : total};" +
        "};";

    DBObject query = new BasicDBObject();
    query.put("meh", "meh");

    MapReduceCommand cmd = new MapReduceCommand(coll, map, reduce, null, MapReduceCommand.OutputType.INLINE, query);
    Map scope = new HashMap();
    scope.put("from", 100);
    scope.put("to", 301);
    cmd.setScope(scope);
    MapReduceOutput out = coll.mapReduce(cmd);

    Assertions.assertThat(out.results()).isEqualTo(fongoRule.parseList("[{\"_id\":{\"day\":\"200\"}, \"value\":{\"count\":1.0}}, " +
        "{\"_id\":{\"day\":\"300\"}, \"value\":{\"count\":1.0}}]"));
  }

  @Test
  public void should_scope_permit_to_initialize_NumberLong() {
    DBCollection coll = fongoRule.newCollection();

    fongoRule.insertJSON(coll, "[{\n" +
        "\t\"_id\": \"4f0c56f1b8eea0b686189c90\",\n" +
        "\t\"meh\": \"meh\",\n" +
        "\t\"feh\": \"feh\",\n" +
        "\t\"arrayOfStuff\": [\n" +
        "\t\t{\n" +
        "\t\t\t\"name\": \"Elgin City\",\n" +
        "\t\t\t\"date\": \"100\"\n" +
        "\t\t},\n" +
        "\t\t{\n" +
        "\t\t\t\"name\": \"Rangers\",\n" +
        "\t\t\t\"date\": \"200\"\n" +
        "\t\t},\n" +
        "\t\t{\n" +
        "\t\t\t\"name\": \"Arsenal\",\n" +
        "\t\t\t\"date\": \"300\"\n" +
        "\t\t}\n" +
        "\t]\n" +
        "},\n" +
        "{\n" +
        "\t\"_id\": \"4f0c56f1b8eea0b686189c99\",\n" +
        "\t\"meh\": \"meh meh meh meh\",\n" +
        "\t\"feh\": \"feh feh feh feh feh feh\",\n" +
        "\t\"arrayOfStuff\": [\n" +
        "\t\t{\n" +
        "\t\t\t\"name\": \"Satriani\",\n" +
        "\t\t\t\"date\": \"100\"\n" +
        "\t\t\t},\n" +
        "\t\t{\n" +
        "\t\t\t\"name\": \"Vai\",\n" +
        "\t\t\t\"date\": \"200\"\n" +
        "\t\t},\n" +
        "\t\t{\n" +
        "\t\t\t\"name\": \"Johnson\",\n" +
        "\t\t\t\"date\": \"300\"\n" +
        "\t\t}\n" +
        "\t]\n" +
        "}]");

    String map = "function() {" +
        "if(this.arrayOfStuff) {" +
        "this.arrayOfStuff.forEach(function(stuff) {" +
        "if(stuff.date > o.from.toNumber() && stuff.date < o.to.toNumber()) {" +
        "emit({day: stuff.date}, {count:1});" +
        "}" +
        "});" +
        "}" +
        "};";

    String reduce = "function(key , values) {" +
        "var total = 0;" +
        "values.forEach(function(v) {" +
        "total += v.count;" +
        "});" +
        "return {count : total};" +
        "};";

    DBObject query = new BasicDBObject();
    query.put("meh", "meh");

    MapReduceCommand cmd = new MapReduceCommand(coll, map, reduce, null, MapReduceCommand.OutputType.INLINE, query);
    Map scope = new HashMap();
    scope.put("o", new BasicDBObject("to", 301L).append("from", 100L));
    cmd.setScope(scope);
    MapReduceOutput out = coll.mapReduce(cmd);

    Assertions.assertThat(out.results()).isEqualTo(fongoRule.parseList("[{\"_id\":{\"day\":\"200\"}, \"value\":{\"count\":1.0}}, " +
        "{\"_id\":{\"day\":\"300\"}, \"value\":{\"count\":1.0}}]"));
  }

  @Test
  public void should_printjson_work() {
    // Given
    DBCollection coll = newCollectionWithUrls();


    String map = "function(){    emit({url: this.url}, 1);  };";
    String reduce = "function(key, values){    var res = 0.0;    values.forEach(function(v){ res += 1.0});    printjson(res); return {\"count\": res};  };";

    // When
    final MapReduceOutput result = coll.mapReduce(map, reduce, "result", new BasicDBObject());

    // Then
    List<DBObject> results = fongoRule.newCollection("result").find().toArray();
    assertEquals(fongoRule.parse("[{\"_id\" : {url: \"www.google.com\"} , \"value\" : { \"count\" : 2.0}}, { \"_id\" : {url: \"www.no-fucking-idea.com\"} , \"value\" : { \"count\" : 3.0}}]"), results);
  }

  @Test
  public void should_print_work() {
    // Given
    DBCollection coll = newCollectionWithUrls();


    String map = "function(){    emit({url: this.url}, 1);  };";
    String reduce = "function(key, values){    var res = 0.0;    values.forEach(function(v){ res += 1.0});    print(res); return {\"count\": res};  };";

    // When
    final MapReduceOutput result = coll.mapReduce(map, reduce, "result", new BasicDBObject());

    // Then
    List<DBObject> results = fongoRule.newCollection("result").find().toArray();
    assertEquals(fongoRule.parse("[{\"_id\" : {url: \"www.google.com\"} , \"value\" : { \"count\" : 2.0}}, { \"_id\" : {url: \"www.no-fucking-idea.com\"} , \"value\" : { \"count\" : 3.0}}]"), results);
  }

  @Test
  public void should_printjsononeline_work() {
    // Given
    DBCollection coll = newCollectionWithUrls();


    String map = "function(){    emit({url: this.url}, 1);  };";
    String reduce = "function(key, values){    var res = 0.0;    values.forEach(function(v){ res += 1.0});    printjsononeline(res); return {\"count\": res};  };";

    // When
    final MapReduceOutput result = coll.mapReduce(map, reduce, "result", new BasicDBObject());

    // Then
    List<DBObject> results = fongoRule.newCollection("result").find().toArray();
    assertEquals(fongoRule.parse("[{\"_id\" : {url: \"www.google.com\"} , \"value\" : { \"count\" : 2.0}}, { \"_id\" : {url: \"www.no-fucking-idea.com\"} , \"value\" : { \"count\" : 3.0}}]"), results);
  }

  @Test
  public void should_assert_work() {
    // Given
    DBCollection coll = newCollectionWithUrls();


    String map = "function(){    emit({url: this.url}, 1);  };";
    String reduce = "function(key, values){    var res = 0.0;    values.forEach(function(v){ res += 1.0});    assert(true); return {\"count\": res};  };";

    // When
    final MapReduceOutput result = coll.mapReduce(map, reduce, "result", new BasicDBObject());

    // Then
    List<DBObject> results = fongoRule.newCollection("result").find().toArray();
    assertEquals(fongoRule.parse("[{\"_id\" : {url: \"www.google.com\"} , \"value\" : { \"count\" : 2.0}}, { \"_id\" : {url: \"www.no-fucking-idea.com\"} , \"value\" : { \"count\" : 3.0}}]"), results);
  }

  @Test
  public void should_assert_return_an_error() {
    ExpectedMongoException.expectMongoCommandException(exception, 16722);
    exception.expectMessage("Error: assert failed");
    // Given
    DBCollection coll = newCollectionWithUrls();


    String map = "function(){    emit({url: this.url}, 1);  };";
    String reduce = "function(key, values){    var res = 0.0;    values.forEach(function(v){ res += 1.0});    assert(res == 2); return {\"count\": res};  };";

    // When
    coll.mapReduce(map, reduce, "result", new BasicDBObject());
  }

  @Test
  public void should_isNumber_work() {
    // Given
    DBCollection coll = newCollectionWithUrls();


    String map = "function(){    emit({url: this.url}, 1);  };";
    String reduce = "function(key, values){    var res = 0.0;    values.forEach(function(v){ res += 1.0}); return {\"count\": isNumber(res) ? 1 : 2};  };";

    // When
    final MapReduceOutput result = coll.mapReduce(map, reduce, "result", new BasicDBObject());

    // Then
    List<DBObject> results = fongoRule.newCollection("result").find().toArray();
    assertEquals(fongoRule.parse("[{\"_id\" : {url: \"www.google.com\"} , \"value\" : { \"count\" : 1.0}}, { \"_id\" : {url: \"www.no-fucking-idea.com\"} , \"value\" : { \"count\" : 1.0}}]"), results);
  }

  @Test
  public void should_isString_work() {
    // Given
    DBCollection coll = newCollectionWithUrls();


    String map = "function(){    emit({url: this.url}, 1);  };";
    String reduce = "function(key, values){    var res = 0.0;    values.forEach(function(v){ res += 1.0}); return {\"count\": isString(\"\" + res) ? 1 : 2};  };";

    // When
    final MapReduceOutput result = coll.mapReduce(map, reduce, "result", new BasicDBObject());

    // Then
    List<DBObject> results = fongoRule.newCollection("result").find().toArray();
    assertEquals(fongoRule.parse("[{\"_id\" : {url: \"www.google.com\"} , \"value\" : { \"count\" : 1.0}}, { \"_id\" : {url: \"www.no-fucking-idea.com\"} , \"value\" : { \"count\" : 1.0}}]"), results);
  }

  @Test
  public void should_isString_return_false() {
    // Given
    DBCollection coll = newCollectionWithUrls();


    String map = "function(){    emit({url: this.url}, 1);  };";
    String reduce = "function(key, values){    var res = 0.0;    values.forEach(function(v){ res += 1.0}); return {\"count\": isString(res) ? 1 : 2};  };";

    // When
    final MapReduceOutput result = coll.mapReduce(map, reduce, "result", new BasicDBObject());

    // Then
    List<DBObject> results = fongoRule.newCollection("result").find().toArray();
    assertEquals(fongoRule.parse("[{\"_id\" : {url: \"www.google.com\"} , \"value\" : { \"count\" : 2.0}}, { \"_id\" : {url: \"www.no-fucking-idea.com\"} , \"value\" : { \"count\" : 2.0}}]"), results);
  }

  @Test
  public void should_isObject_work() {
    // Given
    DBCollection coll = newCollectionWithUrls();


    String map = "function(){    emit({url: this.url}, 1);  };";
    String reduce = "function(key, values){    var res = 0.0;    values.forEach(function(v){ res += 1.0}); return {\"count\": isObject({res:res}) ? 1 : 2};  };";

    // When
    final MapReduceOutput result = coll.mapReduce(map, reduce, "result", new BasicDBObject());

    // Then
    List<DBObject> results = fongoRule.newCollection("result").find().toArray();
    assertEquals(fongoRule.parse("[{\"_id\" : {url: \"www.google.com\"} , \"value\" : { \"count\" : 1.0}}, { \"_id\" : {url: \"www.no-fucking-idea.com\"} , \"value\" : { \"count\" : 1.0}}]"), results);
  }

  @Test
  public void should_isObject_return_false_for_string() {
    // Given
    DBCollection coll = newCollectionWithUrls();

    String map = "function(){    emit({url: this.url}, 1);  };";
    String reduce = "function(key, values){    var res = 0.0;    values.forEach(function(v){ res += 1.0}); return {\"count\": isObject(\"\" + res) ? 1 : 2};  };";

    // When
    final MapReduceOutput result = coll.mapReduce(map, reduce, "result", new BasicDBObject());

    // Then
    List<DBObject> results = fongoRule.newCollection("result").find().toArray();
    assertEquals(fongoRule.parse("[{\"_id\" : {url: \"www.google.com\"} , \"value\" : { \"count\" : 2.0}}, { \"_id\" : {url: \"www.no-fucking-idea.com\"} , \"value\" : { \"count\" : 2.0}}]"), results);
  }

  @Test
  public void should_isObject_return_false_for_double() {
    // Given
    DBCollection coll = newCollectionWithUrls();

    String map = "function(){    emit({url: this.url}, 1);  };";
    String reduce = "function(key, values){    var res = 0.0;    values.forEach(function(v){ res += 1.0}); return {\"count\": isObject(res) ? 1 : 2}  };";

    // When
    final MapReduceOutput result = coll.mapReduce(map, reduce, "result", new BasicDBObject());

    // Then
    List<DBObject> results = fongoRule.newCollection("result").find().toArray();
    assertEquals(fongoRule.parse("[{\"_id\" : {url: \"www.google.com\"} , \"value\" : { \"count\" : 2.0}}, { \"_id\" : {url: \"www.no-fucking-idea.com\"} , \"value\" : { \"count\" : 2.0}}]"), results);
  }

  @Test
  public void should_tojson_works() {
    // Given
    DBCollection coll = newCollectionWithUrls();

    String map = "function(){    emit({url: this.url}, 1);  };";
    String reduce = "function(key, values){    var res = 0.0;    values.forEach(function(v){ res += 1.0});    ; return tojson({\"count\": res});  };";

    // When
    final MapReduceOutput result = coll.mapReduce(map, reduce, "result", new BasicDBObject());

    // Then
    List<DBObject> results = fongoRule.newCollection("result").find().toArray();
    // Some difference with real mongo on space
    assertEquals(fongoRule.parse("[{ \"_id\" : { \"url\" : \"www.google.com\"} , \"value\" : \"{\\\"count\\\":2}\"}, { \"_id\" : { \"url\" : \"www.no-fucking-idea.com\"} , \"value\" : \"{\\\"count\\\":3}\"}]"), results);
  }

  @Test
  public void should_tojsononeline_works() {
    // Given
    DBCollection coll = newCollectionWithUrls();

    String map = "function(){    emit({url: this.url}, 1);  };";
    String reduce = "function(key, values){    var res = 0.0;    values.forEach(function(v){ res += 1.0});    ; return tojsononeline({\"count\": res});  };";

    // When
    final MapReduceOutput result = coll.mapReduce(map, reduce, "result", new BasicDBObject());

    // Then
    List<DBObject> results = fongoRule.newCollection("result").find().toArray();
    // Some difference with real mongo on space
    assertEquals(fongoRule.parse("[{ \"_id\" : { \"url\" : \"www.google.com\"} , \"value\" : \"{\\\"count\\\":2}\"}, { \"_id\" : { \"url\" : \"www.no-fucking-idea.com\"} , \"value\" : \"{\\\"count\\\":3}\"}]"), results);
  }

  @Test
  public void should_NumberInt_works() {
    // Given
    DBCollection coll = newCollectionWithUrls();


    String map = "function(){    emit({url: this.url}, 1);  };";
    String reduce = "function(key, values){    var res = 0.0;    values.forEach(function(v){ res += 1.0}); return {\"count\": NumberInt(res)};  };";

    // When
    final MapReduceOutput result = coll.mapReduce(map, reduce, "result", new BasicDBObject());

    // Then
    List<DBObject> results = fongoRule.newCollection("result").find().toArray();
    assertEquals(asList(new BasicDBObject("_id", new BasicDBObject("url", "www.google.com")).append("value", new BasicDBObject("count", 2D)),
        new BasicDBObject("_id", new BasicDBObject("url", "www.no-fucking-idea.com")).append("value", new BasicDBObject("count", 3D))),
        results);
//    assertEquals(asList(new BasicDBObject("_id", new BasicDBObject("url", "www.google.com")).append("value", new BasicDBObject("count", 2L)),
//        new BasicDBObject("_id", new BasicDBObject("url", "www.no-fucking-idea.com")).append("value", new BasicDBObject("count", 3L))),
//        results);
  }

  @Test
  public void should_NumberInttoNumber_works() {
    // Given
    DBCollection coll = newCollectionWithUrls();


    String map = "function(){    emit({url: this.url}, 1);  };";
    String reduce = "function(key, values){    var res = 0.0;    values.forEach(function(v){ res += 1.0});    return {\"count\": NumberInt(res).toNumber()};  };";

    // When
    final MapReduceOutput result = coll.mapReduce(map, reduce, "result", new BasicDBObject());

    // Then
    List<DBObject> results = fongoRule.newCollection("result").find().toArray();
    assertEquals(asList(new BasicDBObject("_id", new BasicDBObject("url", "www.google.com")).append("value", new BasicDBObject("count", 2D)),
        new BasicDBObject("_id", new BasicDBObject("url", "www.no-fucking-idea.com")).append("value", new BasicDBObject("count", 3D))),
        results);
  }

  @Test
  public void should_NumberIntvalueOf_works() {
    // Given
    DBCollection coll = newCollectionWithUrls();


    String map = "function(){    emit({url: this.url}, 1);  };";
    String reduce = "function(key, values){    var res = 0.0;    values.forEach(function(v){ res += 1.0});   return {\"count\": NumberInt(res).valueOf()};  };";

    // When
    final MapReduceOutput result = coll.mapReduce(map, reduce, "result", new BasicDBObject());

    // Then
    List<DBObject> results = fongoRule.newCollection("result").find().toArray();
    assertEquals(asList(new BasicDBObject("_id", new BasicDBObject("url", "www.google.com")).append("value", new BasicDBObject("count", 2D)),
        new BasicDBObject("_id", new BasicDBObject("url", "www.no-fucking-idea.com")).append("value", new BasicDBObject("count", 3D))),
        results);
  }

  @Test
  public void should_NumberInttoString_works() {
    // Given
    DBCollection coll = newCollectionWithUrls();


    String map = "function(){    emit({url: this.url}, 1);  };";
    String reduce = "function(key, values){    var res = 0.0;    values.forEach(function(v){ res += 1.0});    printjson(res); return {\"count\": NumberInt(res).toString()};  };";

    // When
    final MapReduceOutput result = coll.mapReduce(map, reduce, "result", new BasicDBObject());

    // Then
    List<DBObject> results = fongoRule.newCollection("result").find().toArray();
    assertEquals(asList(new BasicDBObject("_id", new BasicDBObject("url", "www.google.com")).append("value", new BasicDBObject("count", "NumberInt(2)")),
        new BasicDBObject("_id", new BasicDBObject("url", "www.no-fucking-idea.com")).append("value", new BasicDBObject("count", "NumberInt(3)"))),
        results);
  }

  @Test
  public void should_NumberLong_works() {
    // Given
    DBCollection coll = newCollectionWithUrls();


    String map = "function(){    emit({url: this.url}, 1);  };";
    String reduce = "function(key, values){    var res = 0.0;    values.forEach(function(v){ res += 1.0});  return {\"count\": NumberLong(res)};  };";

    // When
    final MapReduceOutput result = coll.mapReduce(map, reduce, "result", new BasicDBObject());

    // Then
    List<DBObject> results = fongoRule.newCollection("result").find().toArray();
    assertEquals(asList(new BasicDBObject("_id", new BasicDBObject("url", "www.google.com")).append("value", new BasicDBObject("count", 2D)),
        new BasicDBObject("_id", new BasicDBObject("url", "www.no-fucking-idea.com")).append("value", new BasicDBObject("count", 3D))),
        results);
//    assertEquals(asList(new BasicDBObject("_id", new BasicDBObject("url", "www.google.com")).append("value", new BasicDBObject("count", 2L)),
//        new BasicDBObject("_id", new BasicDBObject("url", "www.no-fucking-idea.com")).append("value", new BasicDBObject("count", 3L))),
//        results);
  }

  @Test
  public void should_NumberLongtoNumber_works() {
    // Given
    DBCollection coll = newCollectionWithUrls();


    String map = "function(){    emit({url: this.url}, 1);  };";
    String reduce = "function(key, values){    var res = 0.0;    values.forEach(function(v){ res += 1.0});    return {\"count\": NumberLong(res).toNumber()};  };";

    // When
    final MapReduceOutput result = coll.mapReduce(map, reduce, "result", new BasicDBObject());

    // Then
    List<DBObject> results = fongoRule.newCollection("result").find().toArray();
    assertEquals(asList(new BasicDBObject("_id", new BasicDBObject("url", "www.google.com")).append("value", new BasicDBObject("count", 2D)),
        new BasicDBObject("_id", new BasicDBObject("url", "www.no-fucking-idea.com")).append("value", new BasicDBObject("count", 3D))),
        results);
  }

  @Test
  public void should_NumberLongvalueOf_works() {
    // Given
    DBCollection coll = newCollectionWithUrls();


    String map = "function(){    emit({url: this.url}, 1);  };";
    String reduce = "function(key, values){    var res = 0.0;    values.forEach(function(v){ res += 1.0});    return {\"count\": NumberLong(res).valueOf()};  };";

    // When
    final MapReduceOutput result = coll.mapReduce(map, reduce, "result", new BasicDBObject());

    // Then
    List<DBObject> results = fongoRule.newCollection("result").find().toArray();
    assertEquals(asList(new BasicDBObject("_id", new BasicDBObject("url", "www.google.com")).append("value", new BasicDBObject("count", 2D)),
        new BasicDBObject("_id", new BasicDBObject("url", "www.no-fucking-idea.com")).append("value", new BasicDBObject("count", 3D))),
        results);
  }

  @Test
  public void should_NumberLongtoString_works() {
    // Given
    DBCollection coll = newCollectionWithUrls();


    String map = "function(){    emit({url: this.url}, 1);  };";
    String reduce = "function(key, values){    var res = 0.0;    values.forEach(function(v){ res += 1.0});    return {\"count\": NumberLong(res).toString()};  };";

    // When
    final MapReduceOutput result = coll.mapReduce(map, reduce, "result", new BasicDBObject());

    // Then
    List<DBObject> results = fongoRule.newCollection("result").find().toArray();
    assertEquals(asList(new BasicDBObject("_id", new BasicDBObject("url", "www.google.com")).append("value", new BasicDBObject("count", "NumberLong(2)")),
        new BasicDBObject("_id", new BasicDBObject("url", "www.no-fucking-idea.com")).append("value", new BasicDBObject("count", "NumberLong(3)"))),
        results);
  }

  @Test
  public void should_NumberLongvalueOf_with_array_of_long_works() {
    // Given
    final DBCollection coll = fongoRule.newCollection();
    coll.insert(new BasicDBObject("url", "www.google.com").append("date", Util.wrap(asList(1L, 2L, 3L, 4L))),
        new BasicDBObject("url", "www.google.com").append("date", Util.wrap(asList(2L, 3L, 4L))),
        new BasicDBObject("url", "www.no-fucking-idea.com").append("date", Util.wrap(asList(2L, 3L, 4L))),
        new BasicDBObject("url", "www.no-fucking-idea.com").append("date", Util.wrap(asList(12L, 3L, 4L))));

    String map = "function(){  emit({url: this.url}, this.date);  };";
    String reduce = "function(key, values){    var res = 0;   values.forEach(function(v){ v.forEach(function(l) { res += l.toNumber()})});    return {\"count\": res};  };";

    // When
    final MapReduceOutput result = coll.mapReduce(map, reduce, "result", new BasicDBObject());

    // Then
    List<DBObject> results = fongoRule.newCollection("result").find().toArray();
    assertEquals(asList(new BasicDBObject("_id", new BasicDBObject("url", "www.google.com")).append("value", new BasicDBObject("count", 19D)),
        new BasicDBObject("_id", new BasicDBObject("url", "www.no-fucking-idea.com")).append("value", new BasicDBObject("count", 28D))),
        results);
  }


  private DBCollection newCollectionWithUrls() {
    return fongoRule.insertJSON(fongoRule.newCollection(), "[{url: \"www.google.com\", date: 1, trash_data: 5 },\n" +
        " {url: \"www.no-fucking-idea.com\", date: 1, trash_data: 13 },\n" +
        " {url: \"www.google.com\", date: 1, trash_data: 1 },\n" +
        " {url: \"www.no-fucking-idea.com\", date: 2, trash_data: 69 },\n" +
        " {url: \"www.no-fucking-idea.com\", date: 2, trash_data: 256 }]");
  }
}