/**
 * Copyright (C) 2015 Deveryware S.A. All Rights Reserved.
 */
package com.github.fakemongo;

import com.mongodb.AggregationOutput;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoNamespace;
import com.mongodb.WriteConcern;
import com.mongodb.WriteConcernResult;
import com.mongodb.WriteResult;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.bulk.DeleteRequest;
import com.mongodb.bulk.InsertRequest;
import com.mongodb.bulk.UpdateRequest;
import static com.mongodb.bulk.WriteRequest.Type.REPLACE;
import com.mongodb.connection.ClusterId;
import com.mongodb.connection.Connection;
import com.mongodb.connection.ConnectionDescription;
import com.mongodb.connection.QueryResult;
import com.mongodb.connection.ServerId;
import com.mongodb.internal.validator.CollectibleDocumentFieldNameValidator;
import com.mongodb.internal.validator.UpdateFieldNameValidator;
import com.mongodb.operation.FongoBsonArrayWrapper;
import com.mongodb.util.JSON;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.bson.BsonArray;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonDocumentReader;
import org.bson.BsonDocumentWriter;
import org.bson.BsonInt64;
import org.bson.BsonValue;
import org.bson.FieldNameValidator;
import org.bson.codecs.Decoder;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.EncoderContext;
import org.bson.json.JsonReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class FongoConnection implements Connection {
  private final static Logger LOG = LoggerFactory.getLogger(FongoConnection.class);

  private final Fongo fongo;

  public FongoConnection(final Fongo fongo) {
    this.fongo = fongo;
  }

  @Override
  public Connection retain() {
    LOG.debug("retain()");
    return this;
  }

  @Override
  public ConnectionDescription getDescription() {
    return new ConnectionDescription(new ServerId(new ClusterId(), fongo.getServerAddress()));
  }

  @Override
  public WriteConcernResult insert(MongoNamespace
                                       namespace, boolean ordered, WriteConcern writeConcern, List<InsertRequest> inserts) {
    LOG.info("insert() namespace:{} inserts:{}", namespace, inserts);
    final DBCollection collection = dbCollection(namespace);
    for (InsertRequest insert : inserts) {
      final DBObject parse = dbObject(insert.getDocument());
      collection.insert(parse, writeConcern);
      LOG.debug("insert() namespace:{} insert:{}, parse:{}", namespace, insert.getDocument(), parse.getClass());
    }
    return WriteConcernResult.acknowledged(inserts.size(), false, null);
  }

  @Override
  public WriteConcernResult update(MongoNamespace namespace, boolean ordered, WriteConcern writeConcern, List<UpdateRequest> updates) {
    LOG.info("update() namespace:{} updates:{}", namespace, updates);
    final DBCollection collection = dbCollection(namespace);

    boolean isUpdateOfExisting = false;
    BsonValue upsertedId = null;
    int count = 0;

    for (UpdateRequest update : updates) {
      FieldNameValidator validator;
      if (update.getType() == REPLACE) {
        validator = new CollectibleDocumentFieldNameValidator();
      } else {
        validator = new UpdateFieldNameValidator();
      }
      for (String updateName : update.getUpdate().keySet()) {
        if (!validator.validate(updateName)) {
          throw new IllegalArgumentException("Invalid BSON field name " + updateName);
        }
      }
      final WriteResult writeResult = collection.update(dbObject(update.getFilter()), dbObject(update.getUpdate()));
      if (writeResult.isUpdateOfExisting()) {
        isUpdateOfExisting = true;
      }
      count += writeResult.getN();
    }
    return WriteConcernResult.acknowledged(count, isUpdateOfExisting, upsertedId);
  }

  @Override
  public WriteConcernResult delete(MongoNamespace namespace, boolean ordered, WriteConcern writeConcern, List<DeleteRequest> deletes) {
    LOG.info("delete() namespace:{} deletes:{}", namespace, deletes);
    final DBCollection collection = dbCollection(namespace);
    int count = 0;
    for (DeleteRequest delete : deletes) {
      final DBObject parse = dbObject(delete.getFilter());
      if (delete.isMulti()) {
        final WriteResult writeResult = collection.remove(parse, writeConcern);
        count += writeResult.getN();
      } else {
        final DBObject dbObject = collection.findAndRemove(parse);
        if (dbObject != null) {
          count++;
        }
      }
    }
    return WriteConcernResult.acknowledged(count, count != 0, null);
  }

  @Override
  public BulkWriteResult insertCommand(MongoNamespace namespace, boolean ordered, WriteConcern writeConcern, List<InsertRequest> inserts) {
    LOG.info("insertCommand() namespace:{} inserts:{}", namespace, inserts);
    return null;
  }

  @Override
  public BulkWriteResult updateCommand(MongoNamespace namespace, boolean ordered, WriteConcern writeConcern, List<UpdateRequest> updates) {
    LOG.info("updateCommand() namespace:{} updates:{}", namespace, updates);
    return null;
  }

  @Override
  public BulkWriteResult deleteCommand(MongoNamespace namespace, boolean ordered, WriteConcern writeConcern, List<DeleteRequest> deletes) {
    LOG.info("deleteCommand() namespace:{} deletes:{}", namespace, deletes);
    return null;
  }

  @Override
  public <T> T command(String database, BsonDocument command, boolean slaveOk, FieldNameValidator fieldNameValidator, Decoder<T> commandResultDecoder) {
    final DB db = fongo.getDB(database);
    LOG.info("command() database:{}, command:{}", database, command);
    if (command.containsKey("count")) {
      final DBCollection dbCollection = db.getCollection(command.get("count").asString().getValue());
      final DBObject query = dbObject(command, "query");
      final long limit = command.containsKey("limit") ? command.getInt64("limit").longValue() : -1;
      final long skip = command.containsKey("skip") ? command.getInt64("skip").longValue() : 0;

      return (T) new BsonDocument("n", new BsonInt64(dbCollection.getCount(query, null, limit, skip)));
    } else if (command.containsKey("findandmodify")) {
      final DBCollection dbCollection = db.getCollection(command.get("findandmodify").asString().getValue());
      final DBObject query = dbObject(command, "query");
      final DBObject update = dbObject(command, "update");
      final DBObject fields = dbObject(command, "fields");
      final DBObject sort = dbObject(command, "sort");
      final boolean returnNew = BsonBoolean.TRUE.equals(command.getBoolean("new", BsonBoolean.FALSE));
      final boolean upsert = BsonBoolean.TRUE.equals(command.getBoolean("upsert", BsonBoolean.FALSE));
      final boolean remove = BsonBoolean.TRUE.equals(command.getBoolean("remove", BsonBoolean.FALSE));

      if (update != null) {
        final FieldNameValidator validatorUpdate = fieldNameValidator.getValidatorForField("update");
        for (String updateName : update.keySet()) {
          if (!validatorUpdate.validate(updateName)) {
            throw new IllegalArgumentException("Invalid BSON field name " + updateName);
          }
        }
      }

      final DBObject andModify = dbCollection.findAndModify(query, fields, sort, remove, update, returnNew, upsert);
      return reencode(commandResultDecoder, "value", andModify);
    } else if (command.containsKey("distinct")) {
      final DBCollection dbCollection = db.getCollection(command.get("distinct").asString().getValue());
      final DBObject query = dbObject(command, "query");
      final List<Object> distincts = dbCollection.distinct(command.getString("key").getValue(), query);
      return (T) new BsonDocument("values", FongoBsonArrayWrapper.bsonArrayWrapper(distincts));
    } else if (command.containsKey("aggregate")) {
      final DBCollection dbCollection = db.getCollection(command.get("aggregate").asString().getValue());
      final AggregationOutput aggregate = dbCollection.aggregate(dbObjects(command, "pipeline"));
      final String resultField = "result";
      final Iterable<DBObject> results = aggregate.results();
      return reencode(commandResultDecoder, resultField, results);
    } else {
      throw new FongoException("Not implemented for command : " + JSON.serialize(command));
    }
  }

  private <T> T reencode(Decoder<T> commandResultDecoder, String resultField, Iterable<DBObject> results) {
    return commandResultDecoder.decode(new JsonReader(new BsonDocument(resultField, new BsonArray(bsonDocuments(results))).toJson()), decoderContext());
  }

  private <T> T reencode(Decoder<T> commandResultDecoder, String resultField, DBObject result) {
    return commandResultDecoder.decode(new JsonReader(new BsonDocument(resultField, bsonDocument(result)).toJson()), decoderContext());
  }

  @Override
  public <T> QueryResult<T> query(MongoNamespace namespace, BsonDocument queryDocument, BsonDocument fields, int numberToReturn, int skip, boolean slaveOk, boolean tailableCursor, boolean awaitData, boolean noCursorTimeout, boolean partial, boolean oplogReplay, Decoder<T> resultDecoder) {
    LOG.info("query() namespace:{} queryDocument:{}, fields:{}", namespace, queryDocument, fields);
    final DBCollection collection = dbCollection(namespace);

    final List<DBObject> objects = collection
        .find(dbObject(queryDocument), dbObject(fields))
        .limit(numberToReturn)
        .skip(skip)
        .toArray();

    return new QueryResult(namespace, decode(objects, resultDecoder), 1, fongo.getServerAddress());
  }

  @Override
  public <T> QueryResult<T> getMore(MongoNamespace namespace, long cursorId, int numberToReturn, Decoder<T> resultDecoder) {
    LOG.info("getMore() namespace:{} cursorId:{}", namespace, cursorId);
    // 0 means Cursor exhausted.
    return new QueryResult(namespace, Collections.emptyList(), 0, fongo.getServerAddress());
  }

  @Override
  public void killCursor(List<Long> cursors) {
    LOG.info("killCursor() cursors:{}", cursors);
  }

  @Override
  public int getCount() {
    LOG.info("getCount()");
    return 0;
  }

  @Override
  public void release() {
    LOG.info("release()");
  }


  private DBObject dbObject(BsonDocument document) {
    if (document == null) {
      return null;
    }
    return MongoClient.getDefaultCodecRegistry().get(DBObject.class).decode(new BsonDocumentReader(document),
        DecoderContext.builder().build());
  }

  private DBCollection dbCollection(MongoNamespace namespace) {
    return fongo.getDB(namespace.getDatabaseName()).getCollection(namespace.getCollectionName());
  }

  private <T> List<T> decode(final Iterable<DBObject> objects, Decoder<T> resultDecoder) {
    final List<T> list = new ArrayList<T>();
    for (final DBObject object : objects) {
      list.add(decode(object, resultDecoder));
    }
    return list;
  }

  private <T> T decode(DBObject object, Decoder<T> resultDecoder) {
    final BsonDocument document = bsonDocument(object);
    return resultDecoder.decode(new BsonDocumentReader(document), decoderContext());
  }

  private DecoderContext decoderContext() {
    return DecoderContext.builder().build();
  }

  private DBObject dbObject(final BsonDocument queryDocument, final String key) {
    return queryDocument.containsKey(key) ? dbObject(queryDocument.getDocument(key)) : null;
  }

  private List<DBObject> dbObjects(final BsonDocument queryDocument, final String key) {
    final BsonArray values = queryDocument.containsKey(key) ? queryDocument.getArray(key) : null;
    if (values == null) {
      return null;
    }
    List<DBObject> list = new ArrayList<DBObject>();
    for (BsonValue value : values) {
      list.add(dbObject((BsonDocument) value));
    }
    return list;
  }

  private BsonDocument bsonDocument(DBObject dbObject) {
    if (dbObject == null) {
      return null;
    }

    final BsonDocument bsonDocument = new BsonDocument();
    MongoClient.getDefaultCodecRegistry().get(DBObject.class)
        .encode(new BsonDocumentWriter(bsonDocument), dbObject, EncoderContext.builder().build());

    return bsonDocument;
  }

  private List<BsonDocument> bsonDocuments(Iterable<DBObject> dbObjects) {
    if (dbObjects == null) {
      return null;
    }
    List<BsonDocument> list = new ArrayList<BsonDocument>();
    for (DBObject dbObject : dbObjects) {
      list.add(bsonDocument(dbObject));
    }
    return list;
  }
}
