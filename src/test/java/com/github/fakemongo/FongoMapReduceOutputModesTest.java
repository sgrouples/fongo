package com.github.fakemongo;

import com.github.fakemongo.junit.FongoRule;
import com.google.common.collect.ImmutableList;
import static com.google.common.collect.ImmutableList.copyOf;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MapReduceCommand;
import com.mongodb.MapReduceOutput;
import java.util.List;
import static org.assertj.core.api.Assertions.assertThat;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

/**
 * @author Vladimir Shakhov <bogdad@gmail.com>
 */
public class FongoMapReduceOutputModesTest {

  @Rule
  public FongoRule fongoRule = new FongoRule(false);

  private DBCollection users;
  private DBCollection typeHeights;
  private DBCollection userLogins;
  private DBCollection joinUsersLogins;

  @Before
  public void setUp() {
    DB db = fongoRule.getDB();
    users = db.getCollection("users");
    userLogins = db.getCollection("userLogins");
    typeHeights = db.getCollection("typeHeights");
    joinUsersLogins = db.getCollection("joinUsersLogins");
  }

  @Test
  public void inline() {
    BasicDBObject user1 = new BasicDBObject("_id", "idUser1")
        .append("type", "neutral").append("height", "100");
    BasicDBObject user2 = new BasicDBObject("_id", "idUser2")
        .append("type", "neutral").append("height", "150");
    BasicDBObject user3 = new BasicDBObject("_id", "idUser3")
        .append("type", "human").append("height", "200");
    BasicDBObject user4 = new BasicDBObject("_id", "idUser4")
        .append("type", "human").append("height", "400");

    users.insert(user1, user2, user3, user4);

    String map = "function () {" +
        "emit(this.type, this);" +
        "};";
    String reduce = "function (key, values) {" +
        "  var sum = '';" +
        "  for (var i in values) {" +
        "    sum += values[i].height;" +
        "  }" +
        "  return {sum : sum};" +
        "}";

    MapReduceOutput result = users.mapReduce(map, reduce, typeHeights.getName(),
        MapReduceCommand.OutputType.INLINE, new BasicDBObject());

    ImmutableList<DBObject> actual = copyOf(result.results());
    assertThat(actual).containsOnly(new BasicDBObject()
            .append("_id", "neutral")
            .append("value", new BasicDBObject("sum", "100150")),
        new BasicDBObject()
            .append("_id", "human")
            .append("value", new BasicDBObject("sum", "200400"))
    );
  }

  @Test
  public void replace() {
    BasicDBObject existingCat = new BasicDBObject()
        .append("_id", "cat")
        .append("value", new BasicDBObject().append("sum", "YY"));
    BasicDBObject existingNeutral = new BasicDBObject()
        .append("_id", "neutral")
        .append("value", new BasicDBObject().append("sum", "XX"));

    typeHeights.insert(existingNeutral, existingCat);

    BasicDBObject user1 = new BasicDBObject().append("_id", "idUser1")
        .append("type", "neutral").append("height", "100");
    BasicDBObject user2 = new BasicDBObject().append("_id", "idUser2")
        .append("type", "neutral").append("height", "150");
    BasicDBObject user3 = new BasicDBObject().append("_id", "idUser3")
        .append("type", "human").append("height", "200");
    BasicDBObject user4 = new BasicDBObject("_id", "idUser4")
        .append("type", "human").append("height", "400");

    users.insert(user1, user2, user3, user4);

    String map = "function () {" +
        "emit(this.type, this);" +
        "};";
    String reduce = "function (key, values) {" +
        "  var sum = '';" +
        "  for (var i in values) {" +
        "    sum += values[i].height;" +
        "  }" +
        "  return {sum : sum};" +
        "}";

    final MapReduceOutput output = users.mapReduce(map, reduce, typeHeights.getName(),
        MapReduceCommand.OutputType.REPLACE, new BasicDBObject());

    List<DBObject> actual = typeHeights.find().toArray();
    assertThat(actual).containsOnly(new BasicDBObject()
            .append("_id", "neutral")
            .append("value", new BasicDBObject().append("sum", "100150")),
        new BasicDBObject()
            .append("_id", "human")
            .append("value", new BasicDBObject().append("sum", "200400"))
    );
    assertThat(actual).doesNotContain(existingCat);
    assertThat(actual).doesNotContain(existingNeutral);
    assertThat(output.results()).containsOnly(new BasicDBObject()
            .append("_id", "neutral")
            .append("value", new BasicDBObject().append("sum", "100150")),
        new BasicDBObject()
            .append("_id", "human")
            .append("value", new BasicDBObject().append("sum", "200400"))
    );
  }

  @Test
  public void merge() {
    BasicDBObject existingCat = new BasicDBObject()
        .append("_id", "cat")
        .append("value", new BasicDBObject().append("sum", "YY"));
    BasicDBObject existingNeutral = new BasicDBObject()
        .append("_id", "neutral")
        .append("value", new BasicDBObject().append("sum", "XX"));

    typeHeights.insert(existingNeutral, existingCat);

    BasicDBObject user1 = new BasicDBObject().append("_id", "idUser1")
        .append("type", "neutral").append("height", "100");
    BasicDBObject user2 = new BasicDBObject().append("_id", "idUser2")
        .append("type", "neutral").append("height", "150");
    BasicDBObject user3 = new BasicDBObject().append("_id", "idUser3")
        .append("type", "human").append("height", "200");
    BasicDBObject user4 = new BasicDBObject("_id", "idUser4")
        .append("type", "human").append("height", "400");

    users.insert(user1, user2, user3, user4);

    String map = "function () {" +
        "emit(this.type, this);" +
        "};";
    String reduce = "function (key, values) {" +
        "  var sum = '';" +
        "  for (var i in values) {" +
        "    sum += values[i].height;" +
        "  }" +
        "  return {sum : sum};" +
        "}";

    final MapReduceOutput output = users.mapReduce(map, reduce, typeHeights.getName(),
        MapReduceCommand.OutputType.MERGE, new BasicDBObject());

    Iterable<DBObject> actual = typeHeights.find().toArray();
    assertThat(actual).containsOnly(new BasicDBObject()
            .append("_id", "neutral")
            .append("value", new BasicDBObject().append("sum", "100150")),
        new BasicDBObject()
            .append("_id", "human")
            .append("value", new BasicDBObject().append("sum", "200400")),
        existingCat
    );
    assertThat(actual).doesNotContain(existingNeutral);
    assertThat(output.results()).containsOnly(new BasicDBObject()
            .append("_id", "neutral")
            .append("value", new BasicDBObject().append("sum", "100150")),
        new BasicDBObject()
            .append("_id", "human")
            .append("value", new BasicDBObject().append("sum", "200400")),
        existingCat
    );
  }

  @Test
  public void reduceForJoinDataAllreadyThere() {

    joinUsersLogins.insert(new BasicDBObject()
        .append("_id", "idUser1")
        .append("somekey", "somevalue"));

    BasicDBObject user1Login = new BasicDBObject()
        .append("_id", "idUser1")
        .append("login", "bloble");
    BasicDBObject user2Login = new BasicDBObject()
        .append("_id", "idUser2")
        .append("login", "wwww");
    BasicDBObject user3Login = new BasicDBObject()
        .append("_id", "idUser3")
        .append("login", "wordpress");

    userLogins.insert(user1Login, user2Login, user3Login);

    BasicDBObject user1 = new BasicDBObject().append("_id", "idUser1")
        .append("type", "neutral").append("height", "100");
    BasicDBObject user2 = new BasicDBObject().append("_id", "idUser2")
        .append("type", "neutral").append("height", "150");
    BasicDBObject user3 = new BasicDBObject().append("_id", "idUser3")
        .append("type", "human").append("height", "200");

    users.insert(user1, user2, user3);

    String mapUsers = "function () {" +
        "emit(this._id, this);" +
        "};";
    String mapUserLogins = "function () {" +
        "emit(this._id, this);" +
        "};";
    String reduce = "function (id, values) {" +
        "function ifnull(r, v, key) {\n" +
        "  if (v[key] != undefined) r[key] = v[key];\n" +
        "  return r;\n" +
        "  }\n" +
        "  function ifnulls(r, v, keys) {\n" +
        "    for(var i in keys) r = ifnull(r, v, keys[i]);\n" +
        "    return r;\n" +
        "  }\n" +
        "  res = {};\n" +
        "  for (var i in values) {\n" +
        "    res = ifnulls(res, values[i], ['_id', 'login', 'type', 'height']);\n" +
        "  }\n" +
        "  return res;\n" +
        "}";

    users.mapReduce(mapUsers, reduce, joinUsersLogins.getName(),
        MapReduceCommand.OutputType.REDUCE, new BasicDBObject());
    final MapReduceOutput output = userLogins.mapReduce(mapUserLogins, reduce, joinUsersLogins.getName(),
        MapReduceCommand.OutputType.REDUCE, new BasicDBObject());

    Iterable<DBObject> actual = joinUsersLogins.find();

    assertThat(actual).containsOnly(new BasicDBObject()
            .append("_id", user1.get("_id"))
            .append("value", user1.append("login", user1Login.get("login"))),
        new BasicDBObject()
            .append("_id", user2.get("_id"))
            .append("value", user2.append("login", user2Login.get("login"))),
        new BasicDBObject()
            .append("_id", user3.get("_id"))
            .append("value", user3.append("login", user3Login.get("login"))));
    assertThat(output.results()).containsOnly(new BasicDBObject()
            .append("_id", user1.get("_id"))
            .append("value", user1.append("login", user1Login.get("login"))),
        new BasicDBObject()
            .append("_id", user2.get("_id"))
            .append("value", user2.append("login", user2Login.get("login"))),
        new BasicDBObject()
            .append("_id", user3.get("_id"))
            .append("value", user3.append("login", user3Login.get("login"))));
  }

  @Test
  public void reduceForJoin() {

    BasicDBObject user1Login = new BasicDBObject()
        .append("_id", "idUser1")
        .append("login", "bloble");
    BasicDBObject user2Login = new BasicDBObject()
        .append("_id", "idUser2")
        .append("login", "wwww");
    BasicDBObject user3Login = new BasicDBObject()
        .append("_id", "idUser3")
        .append("login", "wordpress");

    userLogins.insert(user1Login, user2Login, user3Login);

    BasicDBObject user1 = new BasicDBObject().append("_id", "idUser1")
        .append("type", "neutral").append("height", "100");
    BasicDBObject user2 = new BasicDBObject().append("_id", "idUser2")
        .append("type", "neutral").append("height", "150");
    BasicDBObject user3 = new BasicDBObject().append("_id", "idUser3")
        .append("type", "human").append("height", "200");

    users.insert(user1, user2, user3);

    String mapUsers = "function () {" +
        "emit(this._id, this);" +
        "};";
    String mapUserLogins = "function () {" +
        "emit(this._id, this);" +
        "};";
    String reduce = "function (id, values) {" +
        "function ifnull(r, v, key) {\n" +
        "  if (key in v && v[key] !=null) r[key] = v[key];\n" +
        "  return r;\n" +
        "  }\n" +
        "  function ifnulls(r, v, keys) {\n" +
        "    for(var i in keys) r = ifnull(r, v, keys[i]);\n" +
        "    return r;\n" +
        "  }\n" +
        "  res = {};\n" +
        "  for (var i in values) {\n" +
        "    res = ifnulls(res, values[i], ['_id', 'login', 'type', 'height']);\n" +
        "  }\n" +
        "  return res;\n" +
        "}";

    users.mapReduce(mapUsers, reduce, joinUsersLogins.getName(),
        MapReduceCommand.OutputType.REDUCE, new BasicDBObject());
    userLogins.mapReduce(mapUserLogins, reduce, joinUsersLogins.getName(),
        MapReduceCommand.OutputType.REDUCE, new BasicDBObject());

    Iterable<DBObject> actual = joinUsersLogins.find();

    assertThat(actual).containsOnly(new BasicDBObject()
            .append("_id", user1.get("_id"))
            .append("value", user1.append("login", user1Login.get("login"))),
        new BasicDBObject()
            .append("_id", user2.get("_id"))
            .append("value", user2.append("login", user2Login.get("login"))),
        new BasicDBObject()
            .append("_id", user3.get("_id"))
            .append("value", user3.append("login", user3Login.get("login"))));
  }
}
