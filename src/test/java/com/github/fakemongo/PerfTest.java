package com.github.fakemongo;

import ch.qos.logback.classic.Level;
import com.github.fakemongo.impl.ExpressionParser;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.FongoDBCollection;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.slf4j.LoggerFactory;

/**
 * Before :
 * <pre>
 * Took 163 ms
 * Took 184 ms with one useless index.
 * Took 5689 ms with no index.
 * Took 9674 ms with index.
 * Took 13220 ms to remove with index.
 * Took 21751 ms to remove with index (new version). * </pre>
 */
public class PerfTest {
  public static void main(String[] args) {
    // Desactivate logback
    ch.qos.logback.classic.Logger log = (ch.qos.logback.classic.Logger) LoggerFactory.getLogger(FongoDBCollection.class);
    log.setLevel(Level.ERROR);
    log = (ch.qos.logback.classic.Logger) LoggerFactory.getLogger(ExpressionParser.class);
    log.setLevel(Level.ERROR);
    log = (ch.qos.logback.classic.Logger) LoggerFactory.getLogger(FongoConnection.class);
    log.setLevel(Level.ERROR);

    System.out.println("Warming jvm");
    // Microbenchmark warm
    for (int i = 0; i < 10000; i++) {
      doit(100);
      doitFindN(100);
      doitFindUniqueIndex(100);
      doitFindNWithIndex(100);
      doitRemoveWithIndex(100);
      doitRemoveWithIndexNew(100);
    }
    System.out.println("Warming jvm done.");
    long startTime = System.currentTimeMillis();
    doit(10000);
    System.out.println("Took " + (System.currentTimeMillis() - startTime) + " ms");

    startTime = System.currentTimeMillis();
    doitFindUniqueIndex(10000);
    System.out.println("Took " + (System.currentTimeMillis() - startTime) + " ms with one useless index.");

    startTime = System.currentTimeMillis();
    doitFindN(10000);
    System.out.println("Took " + (System.currentTimeMillis() - startTime) + " ms with no index.");

    startTime = System.currentTimeMillis();
    doitFindNWithIndex(10000);
    System.out.println("Took " + (System.currentTimeMillis() - startTime) + " ms with index.");

    startTime = System.currentTimeMillis();
    doitRemoveWithIndex(10000);
    System.out.println("Took " + (System.currentTimeMillis() - startTime) + " ms to remove with index.");

    startTime = System.currentTimeMillis();
    doitRemoveWithIndexNew(10000);
    System.out.println("Took " + (System.currentTimeMillis() - startTime) + " ms to remove with index (new version).");
  }

  public static void doit(int size) {
    Fongo fongo = new Fongo("fongo");
    for (int i = 0; i < 1; i++) {
      DB db = fongo.getDB("db");
      DBCollection collection = db.getCollection("coll");
      for (int k = 0; k < size; k++) {
        collection.insert(new BasicDBObject("_id", k).append("n", new BasicDBObject("a", 1)));
        collection.findOne(new BasicDBObject("_id", k));
      }
      db.dropDatabase();
    }
  }

  public static void doitFindN(int size) {
    Fongo fongo = new Fongo("fongo");
    for (int i = 0; i < 1; i++) {
      DB db = fongo.getDB("db");
      DBCollection collection = db.getCollection("coll");
      for (int k = 0; k < size; k++) {
        collection.insert(new BasicDBObject("_id", k).append("n", new BasicDBObject("a", k)));
        collection.findOne(new BasicDBObject("a", k));
      }
      db.dropDatabase();
    }
  }

  public static void doitFindUniqueIndex(int size) {
    Fongo fongo = new Fongo("fongo");
    for (int i = 0; i < 1; i++) {
      DB db = fongo.getDB("db");
      DBCollection collection = db.getCollection("coll");
      collection.createIndex(new BasicDBObject("n", 1));
      for (int k = 0; k < size; k++) {
        collection.insert(new BasicDBObject("_id", k).append("n", new BasicDBObject("a", 1)));
        collection.findOne(new BasicDBObject("_id", k));
      }
      db.dropDatabase();
    }
  }

  public static void doitFindNWithIndex(int size) {
    Fongo fongo = new Fongo("fongo");
    for (int i = 0; i < 1; i++) {
      DB db = fongo.getDB("db");
      DBCollection collection = db.getCollection("coll");
      collection.createIndex(new BasicDBObject("n", 1));
      for (int k = 0; k < size; k++) {
        collection.insert(new BasicDBObject("_id", k).append("n", k % 100));
        collection.findOne(new BasicDBObject("n.a", k % 100));
      }
      db.dropDatabase();
    }
  }

  public static void doitRemoveWithIndex(int size) {
    Fongo fongo = new Fongo("fongo");
    for (int i = 0; i < 1; i++) {
      DB db = fongo.getDB("db");
      DBCollection collection = db.getCollection("coll");
      collection.createIndex(new BasicDBObject("n", 1));
      for (int k = 0; k < size; k++) {
        collection.insert(new BasicDBObject("_id", k).append("n", k % 100));
      }
      for (int k = 0; k < size; k++) {
        collection.remove(new BasicDBObject("n.a", k % 100));
      }
      db.dropDatabase();
    }
  }

  public static void doitRemoveWithIndexNew(int size) {
    Fongo fongo = new Fongo("fongo");
    for (int i = 0; i < 1; i++) {
      MongoDatabase db = fongo.getDatabase("db");
      MongoCollection<Document> collection = db.getCollection("coll");
      collection.createIndex(new Document("n", 1));
      for (int k = 0; k < size; k++) {
        collection.insertOne(new Document("_id", k).append("n", k % 100));
      }
      System.out.println("Start removing things");
      for (int k = 0; k < size; k++) {
        collection.deleteOne(new Document("n.a", k % 100));
      }
      db.drop();
    }
  }
}
