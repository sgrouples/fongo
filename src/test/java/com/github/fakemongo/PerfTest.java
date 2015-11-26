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
import org.openjdk.jmh.annotations.*;
import org.slf4j.LoggerFactory;

import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

@State(Scope.Benchmark)
public class PerfTest {
  public int size = 1000;

  private Fongo fongo;

  @Setup
  public void prepare() {
    fongo = new Fongo("fongo");
  }

  private DB createDB() {
    return fongo.getDB("db");
  }

  @Benchmark
  public void doit() {
    final DB db = createDB();
    final DBCollection collection = db.getCollection("coll");

    for (int k = 0; k < size; k++) {
      collection.insert(new BasicDBObject("_id", k).append("n", new BasicDBObject("a", 1)));
      collection.findOne(new BasicDBObject("_id", k));
    }

    db.dropDatabase();
  }

  @Benchmark
  public void doitFindN() {
    final DB db = createDB();
    final DBCollection collection = db.getCollection("coll");

    for (int k = 0; k < size; k++) {
      collection.insert(new BasicDBObject("_id", k).append("n", new BasicDBObject("a", k)));
      collection.findOne(new BasicDBObject("a", k));
    }

    db.dropDatabase();
  }

  @Benchmark
  public void doitFindUniqueIndex() {
    final DB db = createDB();
    final DBCollection collection = db.getCollection("coll");

    collection.createIndex(new BasicDBObject("n", 1));
    for (int k = 0; k < size; k++) {
      collection.insert(new BasicDBObject("_id", k).append("n", new BasicDBObject("a", 1)));
      collection.findOne(new BasicDBObject("_id", k));
    }

    db.dropDatabase();
  }

  @Benchmark
  public void doitFindNWithIndex() {
    final DB db = createDB();
    final DBCollection collection = db.getCollection("coll");

    collection.createIndex(new BasicDBObject("n", 1));
    for (int k = 0; k < size; k++) {
      collection.insert(new BasicDBObject("_id", k).append("n", k % 100));
      collection.findOne(new BasicDBObject("n.a", k % 100));
    }

    db.dropDatabase();
  }

  @Benchmark
  public void doitRemoveWithIndexNew() {
    MongoDatabase db = fongo.getDatabase("db");
    MongoCollection<Document> collection = db.getCollection("coll");

    collection.createIndex(new Document("n", 1));
    for (int k = 0; k < size; k++) {
      collection.insertOne(new Document("_id", k).append("n", k % 100));
    }

    for (int k = 0; k < size; k++) {
      collection.deleteOne(new Document("n.a", k % 100));
    }
    db.drop();
  }

  public static void main(String[] args) throws RunnerException {
    // Desactivate logback
    ch.qos.logback.classic.Logger log = (ch.qos.logback.classic.Logger) LoggerFactory.getLogger(FongoDBCollection.class);
    log.setLevel(Level.ERROR);
    log = (ch.qos.logback.classic.Logger) LoggerFactory.getLogger(ExpressionParser.class);
    log.setLevel(Level.ERROR);

    Options opt = new OptionsBuilder()
            .include(PerfTest.class.getSimpleName())
            .forks(1)
            .warmupIterations(10)
            .measurementIterations(20)
            .build();

    new Runner(opt).run();
  }
}
