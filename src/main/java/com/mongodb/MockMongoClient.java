package com.mongodb;

import com.github.fakemongo.Fongo;
import com.mongodb.async.SingleResultCallback;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.bulk.DeleteRequest;
import com.mongodb.bulk.InsertRequest;
import com.mongodb.bulk.UpdateRequest;
import com.mongodb.client.MongoDatabase;
import com.mongodb.connection.AsyncConnection;
import com.mongodb.connection.Cluster;
import com.mongodb.connection.ClusterDescription;
import com.mongodb.connection.Connection;
import com.mongodb.connection.ConnectionDescription;
import com.mongodb.connection.QueryResult;
import com.mongodb.connection.Server;
import com.mongodb.connection.ServerDescription;
import com.mongodb.operation.OperationExecutor;
import com.mongodb.operation.ReadOperation;
import com.mongodb.operation.WriteOperation;
import com.mongodb.selector.ServerSelector;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import org.bson.BsonDocument;
import org.bson.FieldNameValidator;
import org.bson.codecs.Decoder;
import org.objenesis.ObjenesisStd;

public class MockMongoClient extends MongoClient {

  // this is immutable 
  private final static MongoClientOptions clientOptions = MongoClientOptions.builder().build();

  private Fongo fongo;
  private MongoOptions options;

  public static MockMongoClient create(Fongo fongo) {
    // using objenesis here to prevent default constructor from spinning up background threads.
    MockMongoClient client = new ObjenesisStd().getInstantiatorOf(MockMongoClient.class).newInstance();
    client.options = new MongoOptions(clientOptions);
    client.fongo = fongo;
    client.setWriteConcern(clientOptions.getWriteConcern());
    return client;
  }

  public MockMongoClient() throws UnknownHostException {

  }

  @Override
  public String toString() {
    return fongo.toString();
  }

  @Override
  public Collection<DB> getUsedDatabases() {
    return fongo.getUsedDatabases();
  }

  @Override
  public List<String> getDatabaseNames() {
    return fongo.getDatabaseNames();
  }

  @Override
  public int getMaxBsonObjectSize() {
    return 16 * 1024 * 1024;
  }

  @Override
  public DB getDB(String dbname) {
    return fongo.getDB(dbname);
  }

  @Override
  public MongoDatabase getDatabase(final String databaseName) {
    return new MongoDatabaseImpl(databaseName, MongoClient.getDefaultCodecRegistry(), ReadPreference.primary(), WriteConcern.ACKNOWLEDGED, new OperationExecutor() {
      @Override
      public <T> T execute(ReadOperation<T> operation, ReadPreference readPreference) {
        return fongo.execute(databaseName, operation, readPreference);
      }

      @Override
      public <T> T execute(WriteOperation<T> operation) {
        return fongo.execute(databaseName, operation);
      }
    });
  }

  @Override
  public void dropDatabase(String dbName) {
    fongo.dropDatabase(dbName);
  }

  @Override
  public MongoOptions getMongoOptions() {
    return options;
  }

  @Override
  public MongoClientOptions getMongoClientOptions() {
    return clientOptions;
  }

  @Override
  public List<ServerAddress> getAllAddress() {
//    if (super.getConnector() != null) return super.getAllAddress();
    if (true) return super.getAllAddress();
    return Collections.emptyList();
  }

  @Override
  public List<ServerAddress> getServerAddressList() {
    return Arrays.asList(new ServerAddress());
  }

  @Override
  public Cluster getCluster() {
    return new Cluster() {
      @Override
      public ClusterDescription getDescription() {
        return null;
      }

      @Override
      public Server selectServer(ServerSelector serverSelector) {
        return new Server() {
          @Override
          public ServerDescription getDescription() {
            return new ObjenesisStd().getInstantiatorOf(ServerDescription.class).newInstance();
          }

          @Override
          public Connection getConnection() {
            return new Connection() {
              @Override
              public Connection retain() {

                return null;
              }

              @Override
              public ConnectionDescription getDescription() {
                return null;
              }

              @Override
              public WriteConcernResult insert(MongoNamespace namespace, boolean ordered, WriteConcern writeConcern, List<InsertRequest> inserts) {
                return null;
              }

              @Override
              public WriteConcernResult update(MongoNamespace namespace, boolean ordered, WriteConcern writeConcern, List<UpdateRequest> updates) {
                return null;
              }

              @Override
              public WriteConcernResult delete(MongoNamespace namespace, boolean ordered, WriteConcern writeConcern, List<DeleteRequest> deletes) {
                return null;
              }

              @Override
              public BulkWriteResult insertCommand(MongoNamespace namespace, boolean ordered, WriteConcern writeConcern, List<InsertRequest> inserts) {
                return null;
              }


              @Override
              public BulkWriteResult updateCommand(MongoNamespace namespace, boolean ordered, WriteConcern writeConcern, List<UpdateRequest> updates) {
                return null;
              }

              @Override
              public BulkWriteResult deleteCommand(MongoNamespace namespace, boolean ordered, WriteConcern writeConcern, List<DeleteRequest> deletes) {
                return null;
              }

              @Override
              public <T> T command(String database, BsonDocument command, boolean slaveOk, FieldNameValidator fieldNameValidator, Decoder<T> commandResultDecoder) {
                return null;
              }

              @Override
              public <T> QueryResult<T> query(MongoNamespace namespace, BsonDocument queryDocument, BsonDocument fields, int numberToReturn, int skip, boolean slaveOk, boolean tailableCursor, boolean awaitData, boolean noCursorTimeout, boolean partial, boolean oplogReplay, Decoder<T> resultDecoder) {
                return null;
              }

              @Override
              public <T> QueryResult<T> getMore(MongoNamespace namespace, long cursorId, int numberToReturn, Decoder<T> resultDecoder) {
                return null;
              }

              @Override
              public void killCursor(List<Long> cursors) {

              }

              @Override
              public int getCount() {
                return 0;
              }

              @Override
              public void release() {

              }
            };
          }

          /**
           * <p>Gets a connection to this server asynchronously.  The connection should be released after the caller is done with it.</p>
           * <p/>
           * <p> Implementations of this method will likely pool the underlying connection, so the effect of closing the returned connection will
           * be to return the connection to the pool. </p>
           *
           * @param callback the callback to execute when the connection is available or an error occurs
           */
          @Override
          public void getConnectionAsync(SingleResultCallback<AsyncConnection> callback) {

          }

        };
      }

      @Override
      public void selectServerAsync(ServerSelector serverSelector, SingleResultCallback<Server> callback) {

      }

      @Override
      public void close() {

      }

      @Override
      public boolean isClosed() {
        return false;
      }
    };
  }

  OperationExecutor createOperationExecutor() {
    return new OperationExecutor() {
      @Override
      public <T> T execute(final ReadOperation<T> operation, final ReadPreference readPreference) {
        return null;
      }

      @Override
      public <T> T execute(final WriteOperation<T> operation) {
        return null;
      }
    };
  }

}
