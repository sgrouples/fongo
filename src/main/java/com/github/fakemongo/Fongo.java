package com.github.fakemongo;

import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.FongoDB;
import com.mongodb.MockMongoClient;
import com.mongodb.MongoClient;
import com.mongodb.MongoNamespace;
import com.mongodb.ReadPreference;
import com.mongodb.ServerAddress;
import com.mongodb.WriteConcern;
import com.mongodb.WriteConcernResult;
import com.mongodb.WriteResult;
import com.mongodb.binding.ConnectionSource;
import com.mongodb.binding.ReadBinding;
import com.mongodb.binding.WriteBinding;
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
import com.mongodb.operation.AggregateOperation;
import com.mongodb.operation.CommandReadOperation;
import com.mongodb.operation.CountOperation;
import com.mongodb.operation.DistinctOperation;
import com.mongodb.operation.FindOperation;
import com.mongodb.operation.GroupOperation;
import com.mongodb.operation.ListCollectionsOperation;
import com.mongodb.operation.ListDatabasesOperation;
import com.mongodb.operation.ListIndexesOperation;
import com.mongodb.operation.MapReduceWithInlineResultsOperation;
import com.mongodb.operation.ParallelCollectionScanOperation;
import com.mongodb.operation.ReadOperation;
import com.mongodb.operation.UserExistsOperation;
import com.mongodb.operation.WriteOperation;
import com.mongodb.util.JSON;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.bson.BsonDocument;
import org.bson.BsonInt64;
import org.bson.BsonValue;
import org.bson.Document;
import org.bson.FieldNameValidator;
import org.bson.codecs.Decoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Faked out version of com.mongodb.Mongo
 * <p>
 * This class doesn't implement Mongo, but does provide the same basic interface
 * </p>
 * Usage:
 * <pre>
 * {@code
 * Fongo fongo = new Fongo("test server");
 * com.mongodb.DB db = fongo.getDB("mydb");
 * // if you need an instance of com.mongodb.Mongo
 * com.mongodb.MongoClient mongo = fongo.getMongo();
 * }
 * </pre>
 *
 * @author jon
 * @author twillouer
 */
public class Fongo {
  private final static Logger LOG = LoggerFactory.getLogger(Fongo.class);

  private final Map<String, FongoDB> dbMap = Collections.synchronizedMap(new HashMap<String, FongoDB>());
  private final ServerAddress serverAddress;
  private final MongoClient mongo;
  private final String name;

  /**
   * @param name Used only for a nice toString in case you have multiple instances
   */
  public Fongo(String name) {
    this.name = name;
    this.serverAddress = new ServerAddress(new InetSocketAddress(ServerAddress.defaultHost(), ServerAddress.defaultPort()));
    this.mongo = createMongo();
  }

  /**
   * equivalent to getDB in driver
   * multiple calls to this method return the same DB instance
   *
   * @param dbname name of the db.
   * @return the DB associated to this name.
   */
  public DB getDB(String dbname) {
    synchronized (dbMap) {
      FongoDB fongoDb = dbMap.get(dbname);
      if (fongoDb == null) {
        fongoDb = new FongoDB(this, dbname);
        dbMap.put(dbname, fongoDb);
      }
      return fongoDb;
    }
  }

  /**
   * Get databases that have been used
   *
   * @return database names.
   */
  public Collection<DB> getUsedDatabases() {
    return new ArrayList<DB>(dbMap.values());
  }

  /**
   * Get database names that have been used
   *
   * @return database names.
   */
  public List<String> getDatabaseNames() {
    return new ArrayList<String>(dbMap.keySet());
  }

  /**
   * Drop db and all data from memory
   *
   * @param dbName name of the database.
   */
  public void dropDatabase(String dbName) {
    FongoDB db = dbMap.remove(dbName);
    if (db != null) {
      db.dropDatabase();
    }
  }

  /**
   * This will always be localhost:27017
   *
   * @return the server address.
   */
  public ServerAddress getServerAddress() {
    return serverAddress;
  }

  /**
   * A mocked out instance of com.mongodb.Mongo
   * All methods calls are intercepted and execute associated Fongo method
   *
   * @return the mongo client
   */
  public MongoClient getMongo() {
    return this.mongo;
  }

  public WriteConcern getWriteConcern() {
    return mongo.getWriteConcern();
  }

  private MongoClient createMongo() {
    return MockMongoClient.create(this);
  }

  public <T> T execute(final String databaseName, final ReadOperation<T> operation, final ReadPreference readPreference) {
    if (operation instanceof DistinctOperation) {
      DistinctOperation<T> distinctOperation = (DistinctOperation<T>) operation;
    } else if (operation instanceof UserExistsOperation) {
      UserExistsOperation userExistsOperation = (UserExistsOperation) operation;
    } else if (operation instanceof CommandReadOperation) {
      CommandReadOperation<T> tCommandReadOperation = (CommandReadOperation<T>) operation;

    } else if (operation instanceof AggregateOperation) {
      AggregateOperation<T> tAggregateOperation = (AggregateOperation<T>) operation;

    } else if (operation instanceof FindOperation) {
      FindOperation findOperation = (FindOperation) operation;

    } else if (operation instanceof MapReduceWithInlineResultsOperation) {
      MapReduceWithInlineResultsOperation mapReduceWithInlineResultsOperation = (MapReduceWithInlineResultsOperation) operation;

    } else if (operation instanceof ListDatabasesOperation) {
      ListDatabasesOperation<T> listDatabasesOperation = (ListDatabasesOperation) operation;


    } else if (operation instanceof CountOperation) {
      CountOperation countOperation = (CountOperation) operation;
//      countOperation.namespace()
    } else if (operation instanceof GroupOperation) {
      GroupOperation groupOperation = (GroupOperation) operation;

    } else if (operation instanceof ListIndexesOperation) {
      ListIndexesOperation listIndexesOperation = (ListIndexesOperation) operation;

    } else if (operation instanceof ListCollectionsOperation) {
      ListCollectionsOperation listCollectionsOperation = (ListCollectionsOperation) operation;

    } else if (operation instanceof ParallelCollectionScanOperation) {
      ParallelCollectionScanOperation parallelCollectionScanOperation = (ParallelCollectionScanOperation) operation;

    }
    return operation.execute(new ReadBinding() {
      @Override
      public ReadPreference getReadPreference() {
        return ReadPreference.primary();
      }

      @Override
      public ConnectionSource getReadConnectionSource() {
        return new MockConnectionSource();
      }

      @Override
      public ReadBinding retain() {
        return this;
      }

      @Override
      public int getCount() {
        return 0;
      }

      @Override
      public void release() {

      }
    });
  }

  public <T> T execute(final String databaseName, final WriteOperation<T> operation) {
    return operation.execute(new WriteBinding() {
      @Override
      public ConnectionSource getWriteConnectionSource() {
        return new MockConnectionSource();
      }

      @Override
      public WriteBinding retain() {
        return this;
      }

      @Override
      public int getCount() {
        return 0;
      }

      @Override
      public void release() {

      }
    });
  }


  @Override
  public String toString() {
    return "Fongo (" + this.name + ")";
  }

  private class MockConnectionSource implements ConnectionSource {
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
          final DB db = getDB(database);
          LOG.info("command() database:{}, command:{}", database, command);
          if (command.containsKey("count")) {
            final DBCollection dbCollection = db.getCollection(command.get("count").asString().getValue());
            final DBObject query = command.containsKey("query") ? dbObject(command.getDocument("query")) : null;
            final long limit = command.containsKey("limit") ? command.getInt64("limit").longValue() : -1;
            final long skip = command.containsKey("skip") ? command.getInt64("skip").longValue() : 0;

            return (T) new BsonDocument().append("n", new BsonInt64(dbCollection.getCount(query, null, limit, skip)));
          } else if (command.containsKey("findandmodify")) {
            final DBCollection dbCollection = db.getCollection(command.get("findandmodify").asString().getValue());
            final DBObject query = command.containsKey("query") ? dbObject(command.getDocument("query")) : null;
            final Boolean remove = command.containsKey("remove") ? command.getBoolean("remove").getValue() : false;

            final DBObject andModify = dbCollection.findAndModify(query, null, null, remove, null, false, false);
            return (T) bsonDocument(andModify);
          }
          return null;
        }

        @Override
        public <T> QueryResult<T> query(MongoNamespace namespace, BsonDocument queryDocument, BsonDocument fields, int numberToReturn, int skip, boolean slaveOk, boolean tailableCursor, boolean awaitData, boolean noCursorTimeout, boolean partial, boolean oplogReplay, Decoder<T> resultDecoder) {
          LOG.info("query() namespace:{} queryDocument:{}, fields:{}", namespace, queryDocument, fields);
          final DBCollection collection = dbCollection(namespace);

          final List<DBObject> objects = collection.find(dbObject(queryDocument.getDocument("$query"))).limit(numberToReturn).skip(skip).toArray();
          return new QueryResult(namespace, documents(objects), 1, new ServerAddress());
        }

        @Override
        public <T> QueryResult<T> getMore(MongoNamespace namespace, long cursorId, int numberToReturn, Decoder<T> resultDecoder) {
          LOG.info("getMore() namespace:{} cursorId:{}", namespace, cursorId);
          // 0 means Cursor exhausted.
          return new QueryResult(namespace, Collections.emptyList(), 0, new ServerAddress());
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
          return (DBObject) JSON.parse(document.toString());
        }

        private DBCollection dbCollection(MongoNamespace namespace) {
          return getDB(namespace.getDatabaseName()).getCollection(namespace.getCollectionName());
        }

        private List<Document> documents(final List<DBObject> objects) {
          final List<Document> list = new ArrayList<Document>(objects.size());
          for (final DBObject dbObject : objects) {
            list.add(document(dbObject));
          }
          return list;
        }

        private Document document(DBObject dbObject) {
          // TODO : performance killer.
          return Document.parse(dbObject.toString());
        }

        private BsonDocument bsonDocument(DBObject dbObject) {
          if (dbObject == null) {
            return null;
          }
          return BsonDocument.parse(dbObject.toString());
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
}
