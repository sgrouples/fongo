/**
 * Copyright (C) 2015 Deveryware S.A. All Rights Reserved.
 */
package com.github.fakemongo;

import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MongoNamespace;
import com.mongodb.ServerAddress;
import com.mongodb.WriteConcern;
import com.mongodb.WriteConcernResult;
import com.mongodb.WriteResult;
import com.mongodb.binding.ConnectionSource;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.bulk.DeleteRequest;
import com.mongodb.bulk.InsertRequest;
import com.mongodb.bulk.UpdateRequest;
import com.mongodb.connection.ClusterId;
import com.mongodb.connection.Connection;
import com.mongodb.connection.ConnectionDescription;
import com.mongodb.connection.QueryResult;
import com.mongodb.connection.ServerConnectionState;
import com.mongodb.connection.ServerDescription;
import com.mongodb.connection.ServerId;
import com.mongodb.connection.ServerVersion;
import com.mongodb.operation.FongoBsonArrayWrapper;
import com.mongodb.util.JSONCallback;
import com.mongodb.util.JSON;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.bson.BsonDocument;
import org.bson.BsonDocumentWrapper;
import org.bson.BsonInt64;
import org.bson.BsonValue;
import org.bson.FieldNameValidator;
import org.bson.codecs.Decoder;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.DocumentCodec;
import org.bson.json.JsonReader;
import org.bson.json.JsonWriterSettings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class FongoConnectionSource implements ConnectionSource {
  private final static Logger LOG = LoggerFactory.getLogger(FongoConnectionSource.class);

  private final Fongo fongo;

  public FongoConnectionSource(Fongo fongo) {
    this.fongo = fongo;
  }

  @Override
  public ServerDescription getServerDescription() {
    return ServerDescription.builder().address(new ServerAddress()).state(ServerConnectionState.CONNECTED).version(new ServerVersion(3, 0)).build();
  }

  @Override
  public Connection getConnection() {
    return new Connection() {
      @Override
      public Connection retain() {
        LOG.debug("retain()");
        return this;
      }

      @Override
      public ConnectionDescription getDescription() {
        return new ConnectionDescription(new ServerId(new ClusterId(), new ServerAddress()));
      }

      @Override
      public WriteConcernResult insert(MongoNamespace namespace, boolean ordered, WriteConcern writeConcern, List<InsertRequest> inserts) {
        LOG.info("insert() namespace:{} inserts:{}", namespace, inserts);
        final DBCollection collection = dbCollection(namespace);
        for (InsertRequest insert : inserts) {
          // TODO : more clever way
          final DBObject parse = dbObject(insert.getDocument());
          collection.insert(parse, writeConcern);
          LOG.debug("insert() namespace:{} insert:{}, parse:{}", namespace, insert.getDocument(), parse.getClass());
//            insert.getDocument()
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
          final Boolean remove = command.containsKey("remove") ? command.getBoolean("remove").getValue() : false;

          final DBObject andModify = dbCollection.findAndModify(query, null, null, remove, update, false, false);
          return (T) new BsonDocument("value", new BsonDocumentWrapper(decode(andModify, new DocumentCodec()), null));
        } else if (command.containsKey("distinct")) {
          final DBCollection dbCollection = db.getCollection(command.get("distinct").asString().getValue());
          final DBObject query = dbObject(command, "query");
          final List<Object> distincts = dbCollection.distinct(command.getString("key").getValue(), query);
          return (T) new BsonDocument("values", FongoBsonArrayWrapper.bsonArrayWrapper(distincts));
        }
        // Will throw an exception.
        return null;
      }

      @Override
      public <T> QueryResult<T> query(MongoNamespace namespace, BsonDocument queryDocument, BsonDocument fields, int numberToReturn, int skip, boolean slaveOk, boolean tailableCursor, boolean awaitData, boolean noCursorTimeout, boolean partial, boolean oplogReplay, Decoder<T> resultDecoder) {
        LOG.info("query() namespace:{} queryDocument:{}, fields:{}", namespace, queryDocument, fields);
        final DBCollection collection = dbCollection(namespace);
        final DBObject sort = dbObject(queryDocument, "$orderby");

        final List<DBObject> objects = collection
            .find(dbObject(queryDocument.getDocument("$query")), dbObject(fields))
            .sort(sort)
            .limit(numberToReturn)
            .skip(skip)
            .toArray();

        return new QueryResult(namespace, documents(objects, resultDecoder), 1, fongo.getServerAddress());
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
        return (DBObject) JSON.parse(document.toJson(new JsonWriterSettings()), new JSONCallback());
      }

      private DBCollection dbCollection(MongoNamespace namespace) {
        return fongo.getDB(namespace.getDatabaseName()).getCollection(namespace.getCollectionName());
      }

      private <T> List<T> documents(final List<DBObject> objects, Decoder<T> resultDecoder) {
        final List<T> list = new ArrayList<T>(objects.size());
        for (final DBObject object : objects) {
          list.add(decode(object, resultDecoder));
        }
        return list;
      }

      private <T> T decode(DBObject object, Decoder<T> resultDecoder) {
        // TODO : performance killer.
        return resultDecoder.decode(new JsonReader(JSON.serialize(object)), DecoderContext.builder().build());
      }

      private DBObject dbObject(final BsonDocument queryDocument, final String key) {
        return queryDocument.containsKey(key) ? dbObject(queryDocument.getDocument(key)) : null;
      }

      private BsonDocument bsonDocument(DBObject dbObject) {
        if (dbObject == null) {
          return null;
        }
        return BsonDocument.parse(dbObject.toString());
      }

      private BsonDocument bsonDocument(List<DBObject> dbObjects) {
        if (dbObjects == null) {
          return null;
        }
        return BsonDocument.parse(dbObjects.toString());
      }

    };
  }

  @Override
  public ConnectionSource retain() {
    return this;
  }

  @Override
  public int getCount() {
    return 0;
  }

  @Override
  public void release() {

  }

}
