package com.mongodb;

import com.github.fakemongo.Fongo;
import com.github.fakemongo.impl.Aggregator;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonDouble;
import org.bson.BsonInt32;
import org.bson.BsonString;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * fongo override of com.mongodb.DB
 * you shouldn't need to use this class directly
 *
 * @author jon
 */
public class FongoDB extends DB {
  private final static Logger LOG = LoggerFactory.getLogger(FongoDB.class);
  public static final String SYSTEM_NAMESPACES = "system.namespaces";

  private final Map<String, FongoDBCollection> collMap = new ConcurrentHashMap<String, FongoDBCollection>();
  private final Set<String> namespaceDeclarated = Collections.synchronizedSet(new LinkedHashSet<String>());
  final Fongo fongo;

  public FongoDB(Fongo fongo, String name) {
    super(fongo.getMongo(), name);
    this.fongo = fongo;
    doGetCollection("system.users");
    doGetCollection("system.indexes");
    doGetCollection(SYSTEM_NAMESPACES);
  }

  @Override
  public synchronized DBCollection createCollection(final String collectionName, final DBObject options) {
    // See getCreateCollectionOperation()
    if (options.get("size") != null && !(options.get("size") instanceof Number)) {
      throw new IllegalArgumentException("'size' should be Number");
    }
    if (options.get("max") != null && !(options.get("max") instanceof Number)) {
      throw new IllegalArgumentException("'max' should be Number");
    }
    if (options.get("capped") != null && !(options.get("capped") instanceof Boolean)) {
      throw new IllegalArgumentException("'capped' should be Boolean");
    }
    if (options.get("autoIndexId") != null && !(options.get("capped") instanceof Boolean)) {
      throw new IllegalArgumentException("'capped' should be Boolean");
    }
    if (options.get("storageEngine") != null && !(options.get("storageEngine") instanceof DBObject)) {
      throw new IllegalArgumentException("storageEngine' should be DBObject");
    }

    if (this.collMap.containsKey(collectionName)) {
      this.notOkErrorResult("collection already exists").throwOnError();
    }

    final DBCollection collection = getCollection(collectionName);
    this.addCollection((FongoDBCollection) collection);
    return collection;
  }

  @Override
  public FongoDBCollection getCollection(final String name) {
    return doGetCollection(name);
  }

  @Override
  protected synchronized FongoDBCollection doGetCollection(String name) {
    FongoDBCollection coll = collMap.get(name);
    if (coll == null) {
      coll = new FongoDBCollection(this, name);
      collMap.put(name, coll);
    }
    return coll;
  }

  private DBObject findAndModify(String collection, DBObject query, DBObject sort, boolean remove, DBObject update, boolean returnNew, DBObject fields, boolean upsert) {
    FongoDBCollection coll = doGetCollection(collection);

    return coll.findAndModify(query, fields, sort, remove, update, returnNew, upsert);
  }

  private List<DBObject> doAggregateCollection(String collection, List<DBObject> pipeline) {
    FongoDBCollection coll = doGetCollection(collection);
    Aggregator aggregator = new Aggregator(this, coll, pipeline);

    return aggregator.computeResult();
  }

  private MapReduceOutput doMapReduce(String collection, String map, String reduce, String finalize, Map<String, Object> scope, DBObject out, DBObject query, DBObject sort, Number limit) {
    FongoDBCollection coll = doGetCollection(collection);
    MapReduceCommand mapReduceCommand = new MapReduceCommand(coll, map, reduce, null, null, query);
    mapReduceCommand.setSort(sort);
    if (limit != null) {
      mapReduceCommand.setLimit(limit.intValue());
    }
    mapReduceCommand.setFinalize(finalize);
    mapReduceCommand.setOutputDB((String) out.get("db"));
    mapReduceCommand.setScope(scope);
    return coll.mapReduce(mapReduceCommand);
  }

  private List<DBObject> doGeoNearCollection(String collection, DBObject near, DBObject query, Number limit, Number maxDistance, boolean spherical) {
    FongoDBCollection coll = doGetCollection(collection);
    return coll.geoNear(near, query, limit, maxDistance, spherical);
  }

  //see http://docs.mongodb.org/manual/tutorial/search-for-text/ for mongodb
  private DBObject doTextSearchInCollection(String collection, String search, Integer limit, DBObject project) {
    FongoDBCollection coll = doGetCollection(collection);
    return coll.text(search, limit, project);
  }

//  @Override
//  public Set<String> getCollectionNames() throws MongoException {
//    Set<String> names = new HashSet<String>();
//    for (FongoDBCollection fongoDBCollection : collMap.values()) {
//      int expectedCount = 0;
//      if (fongoDBCollection.getName().startsWith("system.indexes")) {
//        expectedCount = 1;
//      }
//
//      if (fongoDBCollection.count() > expectedCount) {
//        names.add(fongoDBCollection.getName());
//      }
//    }
//
//    return names;
//  }

  @Override
  public DB getSisterDB(String name) {
    return fongo.getDB(name);
  }

  @Override
  public WriteConcern getWriteConcern() {
    return fongo.getWriteConcern();
  }

  @Override
  public ReadConcern getReadConcern() {
    return fongo.getReadConcern();
  }

  @Override
  public ReadPreference getReadPreference() {
    return ReadPreference.primaryPreferred();
  }

  @Override
  public synchronized void dropDatabase() throws MongoException {
    this.fongo.dropDatabase(this.getName());
    for (FongoDBCollection c : new ArrayList<FongoDBCollection>(collMap.values())) {
      c.drop();
    }
  }

  // TODO WDEL
//  @Override
//  CommandResult doAuthenticate(MongoCredential credentials) {
//    this.mongoCredential = credentials;
//    return okResult();
//  }
//
//  @Override
//  MongoCredential getAuthenticationCredentials() {
//    return this.mongoCredential;
//  }

  /**
   * Executes a database command.
   *
   * @param cmd            dbobject representing the command to execute
   * @param readPreference ReadPreferences for this command (nodes selection is the biggest part of this)
   * @return result of command from the database
   * @throws MongoException
   * @dochub commands
   * @see <a href="http://mongodb.onconfluence.com/display/DOCS/List+of+Database+Commands">List of Commands</a>
   */
  @Override
  public CommandResult command(final DBObject cmd, final ReadPreference readPreference, final DBEncoder encoder) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Fongo got command " + cmd);
    }
    if (cmd.containsField("$eval")) {
      CommandResult commandResult = okResult();
      commandResult.append("retval", "null");
      return commandResult;
    } else if (cmd.containsField("getlasterror") || cmd.containsField("getLastError")) {
      return okResult();
    } else if (cmd.containsField("fsync")) {
      return okResult();
    } else if (cmd.containsField("drop")) {
      this.getCollection(cmd.get("drop").toString()).drop();
      return okResult();
    } else if (cmd.containsField("create")) {
      String collectionName = (String) cmd.get("create");
      doGetCollection(collectionName);
      return okResult();
    } else if (cmd.containsField("count")) {
      String collectionName = (String) cmd.get("count");
      Number limit = (Number) cmd.get("limit");
      Number skip = (Number) cmd.get("skip");
      long result = doGetCollection(collectionName).getCount(
          (DBObject) cmd.get("query"),
          null,
          limit == null ? 0L : limit.longValue(),
          skip == null ? 0L : skip.longValue());
      CommandResult okResult = okResult();
      okResult.append("n", (double) result);
      return okResult;
    } else if (cmd.containsField("deleteIndexes")) {
      String collectionName = (String) cmd.get("deleteIndexes");
      String indexName = (String) cmd.get("index");
      if ("*".equals(indexName)) {
        doGetCollection(collectionName)._dropIndexes();
      } else {
        doGetCollection(collectionName)._dropIndex(indexName);
      }
      return okResult();
    } else if (cmd.containsField("aggregate")) {
      @SuppressWarnings(
          "unchecked") List<DBObject> result = doAggregateCollection((String) cmd.get("aggregate"), (List<DBObject>) cmd.get("pipeline"));
      if (result == null) {
        return notOkErrorResult("can't aggregate");
      }
      CommandResult okResult = okResult();
      BasicDBList list = new BasicDBList();
      list.addAll(result);
      okResult.put("result", list);
      return okResult;
    } else if (cmd.containsField("findAndModify")) {
      return runFindAndModify(cmd, "findAndModify");
    } else if (cmd.containsField("findandmodify")) {
      return runFindAndModify(cmd, "findandmodify");
    } else if (cmd.containsField("ping")) {
      return okResult();
    } else if (cmd.containsField("validate")) {
      return okResult();
    } else if (cmd.containsField("buildInfo") || cmd.containsField("buildinfo")) {
      CommandResult okResult = okResult();
      List<Integer> versionList = fongo.getServerVersion().getVersionList();
      okResult.put("version", versionList.get(0) + "." + versionList.get(1) + "." + versionList.get(2));
      okResult.put("maxBsonObjectSize", 16777216);
      return okResult;
    } else if (cmd.containsField("forceerror")) {
      // http://docs.mongodb.org/manual/reference/command/forceerror/
      return notOkErrorResult(10038, null, "exception: forced error");
    } else if (cmd.containsField("mapreduce")) {
      return runMapReduce(cmd, "mapreduce");
    } else if (cmd.containsField("mapReduce")) {
      return runMapReduce(cmd, "mapReduce");
    } else if (cmd.containsField("geoNear")) {
      // http://docs.mongodb.org/manual/reference/command/geoNear/
      // TODO : handle "num" (override limit)
      try {
        List<DBObject> result = doGeoNearCollection((String) cmd.get("geoNear"),
            (DBObject) cmd.get("near"),
            (DBObject) cmd.get("query"),
            (Number) cmd.get("limit"),
            (Number) cmd.get("maxDistance"),
            Boolean.TRUE.equals(cmd.get("spherical")));
        if (result == null) {
          return notOkErrorResult("can't geoNear");
        }
        CommandResult okResult = okResult();
        BasicDBList list = new BasicDBList();
        list.addAll(result);
        okResult.put("results", list);
        return okResult;
      } catch (MongoException me) {
        return errorResult(me.getCode(), me.getMessage());
      }
    } else if (cmd.containsField("renameCollection")) {
      final String renameCollection = (String) cmd.get("renameCollection");
      final String to = (String) cmd.get("to");
      final boolean dropTarget = (Boolean) cmd.get("dropTarget");
      this.renameCollection(renameCollection, to, dropTarget);
      return okResult();
    } else {
      String collectionName = ((Map.Entry<String, DBObject>) cmd.toMap().entrySet().iterator().next()).getKey();
      if (collectionExists(collectionName)) {
        DBObject newCmd = (DBObject) cmd.get(collectionName);
        if ((newCmd.containsField("text") && ((DBObject) newCmd.get("text")).containsField("search"))) {
          DBObject resp = doTextSearchInCollection(collectionName,
              (String) ((DBObject) newCmd.get("text")).get("search"),
              (Integer) ((DBObject) newCmd.get("text")).get("limit"),
              (DBObject) ((DBObject) newCmd.get("text")).get("project"));
          if (resp == null) {
            return notOkErrorResult("can't perform text search");
          }
          CommandResult okResult = okResult();
          okResult.put("results", resp.get("results"));
          okResult.put("stats", resp.get("stats"));
          return okResult;
        } else if ((newCmd.containsField("$text") && ((DBObject) newCmd.get("$text")).containsField("$search"))) {
          DBObject resp = doTextSearchInCollection(collectionName,
              (String) ((DBObject) newCmd.get("$text")).get("$search"),
              (Integer) ((DBObject) newCmd.get("text")).get("limit"),
              (DBObject) ((DBObject) newCmd.get("text")).get("project"));
          if (resp == null) {
            return notOkErrorResult("can't perform text search");
          }
          CommandResult okResult = okResult();
          okResult.put("results", resp.get("results"));
          okResult.put("stats", resp.get("stats"));
          return okResult;
        }
      }
    }
    String command = cmd.toString();
    if (!cmd.keySet().isEmpty()) {
      command = cmd.keySet().iterator().next();
    }
    return notOkErrorResult(null, "no such cmd: " + command);
  }

  public void renameCollection(String renameCollection, String to, boolean dropTarget) {
    String dbRename = renameCollection.substring(0, renameCollection.indexOf('.'));
    String collectionRename = renameCollection.substring(renameCollection.indexOf('.') + 1);
    String dbTo = to.substring(0, to.indexOf('.'));
    String collectionTo = to.substring(to.indexOf('.') + 1);
    FongoDBCollection rename = (FongoDBCollection) this.fongo.getDB(dbRename).getCollection(collectionRename);
    FongoDBCollection fongoDBCollection = new FongoDBCollection((FongoDB) fongo.getDB(dbTo), collectionTo);
    fongoDBCollection.insert(rename.find().toArray());

    for (DBObject index : rename.getIndexInfo()) {
      if (!index.get("name").equals("_id_")) {
        System.out.println(index);
        Boolean unique = (Boolean) index.get("unique");
        fongoDBCollection.createIndex((DBObject) index.get("key"), (String) index.get("name"), unique == null ? false : unique);
      }
    }

//    for (IndexAbstract index : rename.getIndexes()) {
//      fongoDBCollection.createIndex(index.getKeys(), new BasicDBObject("unique", index.isUnique()));
//    }

    rename.dropIndexes();
    rename.remove(new BasicDBObject());
  }

  /**
   * Returns a set containing the names of all collections in this database.
   *
   * @return the names of collections in this database
   * @throws com.mongodb.MongoException
   * @mongodb.driver.manual reference/method/db.getCollectionNames/ getCollectionNames()
   */
  @Override
  public Set<String> getCollectionNames() {
    List<String> collectionNames = new ArrayList<String>();
    Iterator<DBObject> collections = getCollection("system.namespaces").find(new BasicDBObject());
    while (collections.hasNext()) {
      String collectionName = collections.next().get("name").toString();
      if (!collectionName.contains("$")) {
        collectionNames.add(collectionName.substring(getName().length() + 1));
      }
    }

    Collections.sort(collectionNames);
    return new LinkedHashSet<String>(collectionNames);
  }

  public CommandResult okResult() {
    final BsonDocument result = new BsonDocument("ok", new BsonDouble(1.0));
    return new CommandResult(result, fongo.getServerAddress());
  }

  public CommandResult okErrorResult(int code, String err) {
    final BsonDocument result = new BsonDocument("ok", new BsonDouble(1.0));
    result.put("code", new BsonInt32(code));
    if (err != null) {
      result.put("err", new BsonString(err));
    }
    return new CommandResult(result, fongo.getServerAddress());
  }

  private BsonDocument bsonResultNotOk(int code, String err) {
    final BsonDocument result = new BsonDocument("ok", new BsonDouble(0.0));
    if (err != null) {
      result.put("err", new BsonString(err));
    }
    result.put("code", new BsonInt32(code));
    return result;
  }

  public CommandResult notOkErrorResult(String err) {
    return notOkErrorResult(err, null);
  }

  public CommandResult notOkErrorResult(String err, String errmsg) {
    final BsonDocument result = new BsonDocument("ok", new BsonDouble(0.0));
    if (err != null) {
      result.put("err", new BsonString(err));
    }
    if (errmsg != null) {
      result.put("errmsg", new BsonString(errmsg));
    }
    return new CommandResult(result, fongo.getServerAddress());
  }

  public CommandResult notOkErrorResult(int code, String err) {
    final BsonDocument result = bsonResultNotOk(code, err);
    return new CommandResult(result, fongo.getServerAddress());
  }

  public WriteConcernException writeConcernException(int code, String err) {
    final BsonDocument result = bsonResultNotOk(code, err);
    return new WriteConcernException(result, fongo.getServerAddress(), WriteConcernResult.unacknowledged());
  }

  public WriteConcernException duplicateKeyException(int code, String err) {
    final BsonDocument result = bsonResultNotOk(code, err);
    return new DuplicateKeyException(result, fongo.getServerAddress(), WriteConcernResult.unacknowledged());
  }

  public CommandResult notOkErrorResult(int code, String err, String errmsg) {
    CommandResult result = notOkErrorResult(err, errmsg);
    result.put("code", code);
    return result;
  }

  public CommandResult errorResult(int code, String err) {
    final BsonDocument result = new BsonDocument();
    if (err != null) {
      result.put("err", new BsonString(err));
    }
    result.put("code", new BsonInt32(code));
    result.put("ok", BsonBoolean.FALSE);
    return new CommandResult(result, fongo.getServerAddress());
  }

  @Override
  public String toString() {
    return "FongoDB." + this.getName();
  }

  public synchronized void removeCollection(FongoDBCollection collection) {
    this.collMap.remove(collection.getName());
    this.getCollection(SYSTEM_NAMESPACES).remove(new BasicDBObject("name", collection.getFullName()));
    this.namespaceDeclarated.remove(collection.getFullName());
  }

  public void addCollection(FongoDBCollection collection) {
    this.collMap.put(collection.getName(), collection);
    if (!collection.getName().startsWith("system.")) {
      if (!this.namespaceDeclarated.contains(collection.getFullName())) {
        this.getCollection(SYSTEM_NAMESPACES).insert(new BasicDBObject("name", collection.getFullName()).append("options", new BasicDBObject()));
        if (this.namespaceDeclarated.size() == 0) {
          this.getCollection(SYSTEM_NAMESPACES).insert(new BasicDBObject("name", collection.getDB().getName() + ".system.indexes").append("options", new BasicDBObject()));
        }
        this.namespaceDeclarated.add(collection.getFullName());
      }
    }
  }

  private CommandResult runFindAndModify(DBObject cmd, String key) {
    if (!cmd.containsField("remove") && !cmd.containsField("update")) {
      return notOkErrorResult(null, "need remove or update");
    }

    DBObject result = findAndModify(
        (String) cmd.get(key),
        (DBObject) cmd.get("query"),
        (DBObject) cmd.get("sort"),
        Boolean.TRUE.equals(cmd.get("remove")),
        (DBObject) cmd.get("update"),
        Boolean.TRUE.equals(cmd.get("new")),
        (DBObject) cmd.get("fields"),
        Boolean.TRUE.equals(cmd.get("upsert")));
    CommandResult okResult = okResult();
    okResult.put("value", result);
    return okResult;
  }

  private CommandResult runMapReduce(DBObject cmd, String key) {
    MapReduceOutput result = doMapReduce(
        (String) cmd.get(key),
        (String) cmd.get("map"),
        (String) cmd.get("reduce"),
        (String) cmd.get("finalize"),
        (Map) cmd.get("scope"),
        (DBObject) cmd.get("out"),
        (DBObject) cmd.get("query"),
        (DBObject) cmd.get("sort"),
        (Number) cmd.get("limit"));
    if (result == null) {
      return notOkErrorResult("can't mapReduce");
    }
    CommandResult okResult = okResult();
    if (result.results() instanceof List) {
      // INLINE case.
      okResult.put("results", result.results());
    } else {
      okResult.put("result", result.getCommand());
    }
    return okResult;
  }
}
