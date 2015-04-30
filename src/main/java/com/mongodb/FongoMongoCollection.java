/**
 * Copyright (C) 2015 Deveryware S.A. All Rights Reserved.
 */
package com.mongodb;

import com.github.fakemongo.Fongo;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.CountOptions;
import com.mongodb.client.model.FindOneAndDeleteOptions;
import com.mongodb.util.JSON;
import org.bson.Document;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.conversions.Bson;

/**
 *
 */
public class FongoMongoCollection<TDocument> extends MongoCollectionImpl<TDocument> {

  private final Fongo fongo;

  private final DBCollection dbCollection;

  FongoMongoCollection(Fongo fongo, MongoNamespace namespace, Class<TDocument> tDocumentClass, CodecRegistry codecRegistry, ReadPreference readPreference, WriteConcern writeConcern) {
    super(namespace, tDocumentClass, codecRegistry, readPreference, writeConcern, fongo);
    this.fongo = fongo;
    this.dbCollection = fongo.getDB(namespace.getDatabaseName()).getCollection(namespace.getCollectionName());
  }

  @Override
  public <NewTDocument> MongoCollection<NewTDocument> withDocumentClass(Class<NewTDocument> clazz) {
    return new FongoMongoCollection<NewTDocument>(this.fongo, super.getNamespace(), clazz, super.getCodecRegistry(), super.getReadPreference(), super.getWriteConcern());
  }

  @Override
  public MongoCollection<TDocument> withCodecRegistry(CodecRegistry codecRegistry) {
    return new FongoMongoCollection<TDocument>(this.fongo, super.getNamespace(), super.getDocumentClass(), codecRegistry, super.getReadPreference(), super.getWriteConcern());
  }

  @Override
  public MongoCollection<TDocument> withReadPreference(ReadPreference readPreference) {
    return new FongoMongoCollection<TDocument>(this.fongo, super.getNamespace(), super.getDocumentClass(), super.getCodecRegistry(), readPreference, super.getWriteConcern());
  }

  @Override
  public MongoCollection<TDocument> withWriteConcern(WriteConcern writeConcern) {
    return new FongoMongoCollection<TDocument>(this.fongo, super.getNamespace(), super.getDocumentClass(), super.getCodecRegistry(), super.getReadPreference(), writeConcern);
  }

  @Override
  public long count(Bson filter, CountOptions options) {
    final DBObject query = dbObject(filter);
    final int limit = options.getLimit();
    final int skip = options.getSkip();

    return dbCollection.getCount(query, null, limit, skip);
  }
//
//  @Override
//  public <TResult> DistinctIterable<TResult> distinct(String fieldName, Class<TResult> tResultClass) {
//    return new DistinctIterableImpl<TDocument, TResult>(getNamespace(), getDocumentClass(), tResultClass, getCodecRegistry(), getReadPreference(), fongo, fieldName){
//      @Override
//      public MongoCursor<TResult> iterator() {
//        return super.iterator();
//      }
//    };
//  }

  @Override
  public TDocument findOneAndDelete(Bson filter, FindOneAndDeleteOptions options) {
    return super.findOneAndDelete(filter, options);
  }

  private DBObject dbObject(Bson bson) {
    if (bson == null) {
      return null;
    }
    // TODO Performance killer
    return (DBObject) JSON.parse(bson.toBsonDocument(Document.class, super.getCodecRegistry()).toString());
  }
}
