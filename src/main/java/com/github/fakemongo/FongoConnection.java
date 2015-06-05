/**
 * Copyright (C) 2015 Deveryware S.A. All Rights Reserved.
 */
package com.github.fakemongo;

import com.mongodb.AggregationOutput;
import com.mongodb.BulkUpdateRequestBuilder;
import com.mongodb.BulkWriteOperation;
import com.mongodb.BulkWriteRequestBuilder;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.FongoDBCollection;
import static com.mongodb.FongoDBCollection.bsonDocument;
import static com.mongodb.FongoDBCollection.bsonDocuments;
import static com.mongodb.FongoDBCollection.dbObject;
import static com.mongodb.FongoDBCollection.dbObjects;
import static com.mongodb.FongoDBCollection.decode;
import static com.mongodb.FongoDBCollection.decoderContext;
import com.mongodb.MongoClient;
import com.mongodb.MongoNamespace;
import com.mongodb.WriteConcern;
import com.mongodb.WriteConcernResult;
import com.mongodb.WriteResult;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.bulk.BulkWriteUpsert;
import com.mongodb.bulk.DeleteRequest;
import com.mongodb.bulk.InsertRequest;
import com.mongodb.bulk.UpdateRequest;
import com.mongodb.bulk.WriteRequest;
import static com.mongodb.bulk.WriteRequest.Type.INSERT;
import static com.mongodb.bulk.WriteRequest.Type.REPLACE;
import com.mongodb.connection.BulkWriteBatchCombiner;
import com.mongodb.connection.ClusterId;
import com.mongodb.connection.Connection;
import com.mongodb.connection.ConnectionDescription;
import com.mongodb.connection.QueryResult;
import com.mongodb.connection.ServerId;
import com.mongodb.connection.ServerVersion;
import com.mongodb.internal.connection.IndexMap;
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
import org.bson.BsonInt64;
import org.bson.BsonNull;
import org.bson.BsonObjectId;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.bson.Document;
import org.bson.FieldNameValidator;
import org.bson.codecs.Codec;
import org.bson.codecs.Decoder;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class FongoConnection implements Connection {
  private final static Logger LOG = LoggerFactory.getLogger(FongoConnection.class);

  private final Fongo fongo;
  private final ConnectionDescription connectionDescription;

  public FongoConnection(final Fongo fongo) {
    this.fongo = fongo;
    this.connectionDescription = new ConnectionDescription(new ServerId(new ClusterId(), fongo.getServerAddress())) {
      @Override
      public ServerVersion getServerVersion() {
        return fongo.getServerVersion();
      }
    };
  }

  @Override
  public Connection retain() {
    LOG.debug("retain()");
    return this;
  }

  @Override
  public ConnectionDescription getDescription() {
    return connectionDescription;
  }

  @Override
  public WriteConcernResult insert(MongoNamespace namespace, boolean ordered, WriteConcern writeConcern, List<InsertRequest> inserts) {
    LOG.info("insert() namespace:{} inserts:{}", namespace, inserts);
    final DBCollection collection = dbCollection(namespace);
    for (InsertRequest insert : inserts) {
      final DBObject parse = dbObject(insert.getDocument());
      collection.insert(parse, writeConcern);
      LOG.debug("insert() namespace:{} insert:{}, parse:{}", namespace, insert.getDocument(), parse.getClass());
    }
    if (writeConcern.isAcknowledged()) {
      return WriteConcernResult.acknowledged(inserts.size(), false, null);
    } else {
      return WriteConcernResult.unacknowledged();
    }
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
      final WriteResult writeResult = collection.update(dbObject(update.getFilter()), dbObject(update.getUpdate()), update.isUpsert(), update.isMulti());
      if (writeResult.isUpdateOfExisting()) {
        isUpdateOfExisting = true;
        count += writeResult.getN();
      } else {
        count++;
      }
    }
    if (writeConcern.isAcknowledged()) {
      return WriteConcernResult.acknowledged(count, isUpdateOfExisting, upsertedId);
    } else {
      return WriteConcernResult.unacknowledged();
    }
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
    if (writeConcern.isAcknowledged()) {
      return WriteConcernResult.acknowledged(count, count != 0, null);
    } else {
      return WriteConcernResult.unacknowledged();
    }
  }

  @Override
  public BulkWriteResult insertCommand(MongoNamespace namespace, boolean ordered, WriteConcern writeConcern, List<InsertRequest> inserts) {
    LOG.info("insertCommand() namespace:{} inserts:{}", namespace, inserts);
    final DBCollection collection = dbCollection(namespace);
    final List<BulkWriteUpsert> list = new ArrayList<BulkWriteUpsert>();
    int i = 0;
    for (InsertRequest insert : inserts) {
      final DBObject parse = dbObject(insert.getDocument());
      final WriteResult writeResult = collection.insert(parse, writeConcern);
      if (writeResult.getUpsertedId() != null) {
        list.add(new BulkWriteUpsert(i, new BsonObjectId((ObjectId) writeResult.getUpsertedId())));
      }
      i++;
      LOG.debug("insertCommand() namespace:{} insert:{}, parse:{}", namespace, insert.getDocument(), parse.getClass());
    }
    if (writeConcern.isAcknowledged()) {
      return BulkWriteResult.acknowledged(WriteRequest.Type.INSERT, inserts.size(), writeConcern.isAcknowledged() ? inserts.size() : null, list);
    } else {
      return BulkWriteResult.unacknowledged();
    }
  }

  @Override
  public BulkWriteResult updateCommand(MongoNamespace namespace, boolean ordered, WriteConcern writeConcern, List<UpdateRequest> updates) {
    LOG.info("updateCommand() namespace:{} updates:{}", namespace, updates);
    final DBCollection collection = dbCollection(namespace);


    final BulkWriteOperation bulkWriteOperation = collection.initializeOrderedBulkOperation();
    BulkWriteBatchCombiner bulkWriteBatchCombiner = new BulkWriteBatchCombiner(fongo.getServerAddress(), ordered, writeConcern);
    IndexMap indexMap = IndexMap.create();

    for (UpdateRequest update : updates) {
      FieldNameValidator validator;
      if (update.getType() == REPLACE || update.getType() == INSERT) {
        validator = new CollectibleDocumentFieldNameValidator();
      } else {
        validator = new UpdateFieldNameValidator();
      }
      for (String updateName : update.getUpdate().keySet()) {
        if (!validator.validate(updateName)) {
          throw new IllegalArgumentException("Invalid BSON field name " + updateName);
        }
      }

      switch (update.getType()) {
        case REPLACE:
          bulkWriteOperation.find(dbObject(update.getFilter())).replaceOne(dbObject(update.getUpdate()));
          break;
        case INSERT:
          bulkWriteOperation.insert(dbObject(update.getUpdate()));
          break;
        case UPDATE: {
          if (update.isUpsert()) {
            final BulkUpdateRequestBuilder upsert = bulkWriteOperation.find(dbObject((update.getFilter()))).upsert();
            if (update.isMulti()) {
              upsert.update(dbObject(update.getUpdate()));
            } else {
              upsert.updateOne(dbObject(update.getUpdate()));
            }
          } else {
            BulkWriteRequestBuilder bulkWriteRequestBuilder = bulkWriteOperation.find(dbObject((update.getFilter())));
            if (update.isMulti()) {
              bulkWriteRequestBuilder.update(dbObject(update.getUpdate()));
            } else {
              bulkWriteRequestBuilder.updateOne(dbObject(update.getUpdate()));
            }
          }
        }
        break;
        case DELETE:
          bulkWriteOperation.find(dbObject((update.getFilter()))).removeOne();
      }
      final com.mongodb.BulkWriteResult bulkWriteResult = bulkWriteOperation.execute(writeConcern);
      indexMap = indexMap.add(1, 0);
      bulkWriteBatchCombiner.addResult(bulkWriteResult(bulkWriteResult), indexMap);
    }
    return bulkWriteBatchCombiner.getResult();
  }

  @Override
  public BulkWriteResult deleteCommand(MongoNamespace namespace, boolean ordered, WriteConcern writeConcern, List<DeleteRequest> deletes) {
    LOG.info("deleteCommand() namespace:{} deletes:{}", namespace, deletes);
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
    if (writeConcern.isAcknowledged()) {
      return BulkWriteResult.acknowledged(WriteRequest.Type.DELETE, count, writeConcern.isAcknowledged() ? deletes.size() : null, Collections.<BulkWriteUpsert>emptyList());
    } else {
      return BulkWriteResult.unacknowledged();
    }
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
      final boolean v3 = command.containsKey("cursor");
      final String resultField = v3 ? "cursor" : "result";
      final Iterable<DBObject> results = aggregate.results();
      if (!v3) {
        return reencode(commandResultDecoder, resultField, results);
      } else {
        // TODO : better way.
        final Codec<Document> documentCodec = MongoClient.getDefaultCodecRegistry().get(Document.class);
        final List<Document> each = new ArrayList<Document>();
        for (DBObject result : results) {
          final Document decode = documentCodec.decode(new BsonDocumentReader(bsonDocument(result)),
              decoderContext());
          each.add(decode);
        }
        return (T) new BsonDocument("cursor", new BsonDocument("id", new BsonInt64(0))
            .append("ns", new BsonString(dbCollection.getFullName()))
            .append("firstBatch", FongoBsonArrayWrapper.bsonArrayWrapper(each)));
      }
    } else {
      throw new FongoException("Not implemented for command : " + JSON.serialize(command));
    }
  }

  private <T> T reencode(final Decoder<T> commandResultDecoder, final String resultField, final Iterable<DBObject> results) {
    return commandResultDecoder.decode(new BsonDocumentReader(new BsonDocument(resultField, new BsonArray(bsonDocuments(results)))), decoderContext());
  }

  private <T> T reencode(final Decoder<T> commandResultDecoder, final String resultField, final DBObject result) {
    final BsonValue value;
    if (result == null) {
      value = new BsonNull();
    } else {
      value = bsonDocument(result);
    }
    return commandResultDecoder.decode(new BsonDocumentReader(new BsonDocument(resultField, value)), decoderContext());
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

  private DBCollection dbCollection(MongoNamespace namespace) {
    return fongo.getDB(namespace.getDatabaseName()).getCollection(namespace.getCollectionName());
  }

  private BulkWriteResult bulkWriteResult(com.mongodb.BulkWriteResult bulkWriteResult) {
    if (!bulkWriteResult.isAcknowledged()) {
      return BulkWriteResult.unacknowledged();
    }
    return BulkWriteResult.acknowledged(bulkWriteResult.getInsertedCount(), bulkWriteResult.getMatchedCount() - bulkWriteResult.getUpserts().size(), bulkWriteResult.getRemovedCount(), bulkWriteResult.getModifiedCount(), FongoDBCollection.translateBulkWriteUpsertsToNew(bulkWriteResult.getUpserts(), null));
  }

}
