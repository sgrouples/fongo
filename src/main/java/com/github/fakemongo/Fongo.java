package com.github.fakemongo;

import com.mongodb.DB;
import com.mongodb.FongoDB;
import com.mongodb.MockMongoClient;
import com.mongodb.MongoClient;
import com.mongodb.ReadPreference;
import com.mongodb.ServerAddress;
import com.mongodb.WriteConcern;
import com.mongodb.binding.ConnectionSource;
import com.mongodb.binding.ReadBinding;
import com.mongodb.binding.WriteBinding;
import com.mongodb.client.MongoDatabase;
import com.mongodb.connection.ServerVersion;
import com.mongodb.operation.OperationExecutor;
import com.mongodb.operation.ReadOperation;
import com.mongodb.operation.WriteOperation;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
public class Fongo implements OperationExecutor {
  private final static Logger LOG = LoggerFactory.getLogger(Fongo.class);

  public static final ServerVersion DEFAULT_SERVER_VERSION = new ServerVersion(3, 0);
  public static final ServerVersion OLD_SERVER_VERSION = new ServerVersion(0, 0);

  private final Map<String, FongoDB> dbMap = Collections.synchronizedMap(new HashMap<String, FongoDB>());
  private final ServerAddress serverAddress;
  private final MongoClient mongo;
  private final String name;
  private final ServerVersion serverVersion;

  /**
   * @param name Used only for a nice toString in case you have multiple instances
   */
  public Fongo(final String name) {
    this(name, DEFAULT_SERVER_VERSION);
  }

  /**
   * @param name Used only for a nice toString in case you have multiple instances
   */
  public Fongo(final String name, final ServerVersion serverVersion) {
    this.name = name;
    this.serverAddress = new ServerAddress(new InetSocketAddress(ServerAddress.defaultHost(), ServerAddress.defaultPort()));
    this.serverVersion = serverVersion;
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

  public synchronized MongoDatabase getDatabase(final String databaseName) {
    return mongo.getDatabase(databaseName);
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

  public <T> T execute(final ReadOperation<T> operation, final ReadPreference readPreference) {
    return operation.execute(new ReadBinding() {
      @Override
      public ReadPreference getReadPreference() {
        return readPreference;
      }

      @Override
      public ConnectionSource getReadConnectionSource() {
        return new FongoConnectionSource(Fongo.this);
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

  public <T> T execute(final WriteOperation<T> operation) {
    return operation.execute(new WriteBinding() {
      @Override
      public ConnectionSource getWriteConnectionSource() {
        return new FongoConnectionSource(Fongo.this);
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

  public ServerVersion getServerVersion() {
    return serverVersion;
  }
}
