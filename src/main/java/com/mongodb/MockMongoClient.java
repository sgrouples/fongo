package com.mongodb;

import com.github.fakemongo.Fongo;
import com.mongodb.async.SingleResultCallback;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.bulk.DeleteRequest;
import com.mongodb.bulk.InsertRequest;
import com.mongodb.bulk.UpdateRequest;
import com.mongodb.connection.Cluster;
import com.mongodb.connection.ClusterDescription;
import com.mongodb.connection.Connection;
import com.mongodb.connection.ConnectionDescription;
import com.mongodb.connection.QueryResult;
import com.mongodb.connection.Server;
import com.mongodb.connection.ServerDescription;
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
              public void insertAsync(MongoNamespace namespace, boolean ordered, WriteConcern writeConcern, List<InsertRequest> inserts, SingleResultCallback<WriteConcernResult> callback) {

              }

              @Override
              public WriteConcernResult update(MongoNamespace namespace, boolean ordered, WriteConcern writeConcern, List<UpdateRequest> updates) {
                return null;
              }

              @Override
              public void updateAsync(MongoNamespace namespace, boolean ordered, WriteConcern writeConcern, List<UpdateRequest> updates, SingleResultCallback<WriteConcernResult> callback) {

              }

              @Override
              public WriteConcernResult delete(MongoNamespace namespace, boolean ordered, WriteConcern writeConcern, List<DeleteRequest> deletes) {
                return null;
              }

              @Override
              public void deleteAsync(MongoNamespace namespace, boolean ordered, WriteConcern writeConcern, List<DeleteRequest> deletes, SingleResultCallback<WriteConcernResult> callback) {

              }

              @Override
              public BulkWriteResult insertCommand(MongoNamespace namespace, boolean ordered, WriteConcern writeConcern, List<InsertRequest> inserts) {
                return null;
              }

              @Override
              public void insertCommandAsync(MongoNamespace namespace, boolean ordered, WriteConcern writeConcern, List<InsertRequest> inserts, SingleResultCallback<BulkWriteResult> callback) {

              }

              @Override
              public BulkWriteResult updateCommand(MongoNamespace namespace, boolean ordered, WriteConcern writeConcern, List<UpdateRequest> updates) {
                return null;
              }

              @Override
              public void updateCommandAsync(MongoNamespace namespace, boolean ordered, WriteConcern writeConcern, List<UpdateRequest> updates, SingleResultCallback<BulkWriteResult> callback) {

              }

              @Override
              public BulkWriteResult deleteCommand(MongoNamespace namespace, boolean ordered, WriteConcern writeConcern, List<DeleteRequest> deletes) {
                return null;
              }

              @Override
              public void deleteCommandAsync(MongoNamespace namespace, boolean ordered, WriteConcern writeConcern, List<DeleteRequest> deletes, SingleResultCallback<BulkWriteResult> callback) {

              }

              @Override
              public <T> T command(String database, BsonDocument command, boolean slaveOk, FieldNameValidator fieldNameValidator, Decoder<T> commandResultDecoder) {
                return null;
              }

              @Override
              public <T> void commandAsync(String database, BsonDocument command, boolean slaveOk, FieldNameValidator fieldNameValidator, Decoder<T> commandResultDecoder, SingleResultCallback<T> callback) {

              }

              @Override
              public <T> QueryResult<T> query(MongoNamespace namespace, BsonDocument queryDocument, BsonDocument fields, int numberToReturn, int skip, boolean slaveOk, boolean tailableCursor, boolean awaitData, boolean noCursorTimeout, boolean partial, boolean oplogReplay, Decoder<T> resultDecoder) {
                return null;
              }

              @Override
              public <T> void queryAsync(MongoNamespace namespace, BsonDocument queryDocument, BsonDocument fields, int numberToReturn, int skip, boolean slaveOk, boolean tailableCursor, boolean awaitData, boolean noCursorTimeout, boolean partial, boolean oplogReplay, Decoder<T> resultDecoder, SingleResultCallback<QueryResult<T>> callback) {

              }

              @Override
              public <T> QueryResult<T> getMore(MongoNamespace namespace, long cursorId, int numberToReturn, Decoder<T> resultDecoder) {
                return null;
              }

              @Override
              public <T> void getMoreAsync(MongoNamespace namespace, long cursorId, int numberToReturn, Decoder<T> resultDecoder, SingleResultCallback<QueryResult<T>> callback) {

              }

              @Override
              public void killCursor(List<Long> cursors) {

              }

              @Override
              public void killCursorAsync(List<Long> cursors, SingleResultCallback<Void> callback) {

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

          @Override
          public void getConnectionAsync(SingleResultCallback<Connection> callback) {

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
}
