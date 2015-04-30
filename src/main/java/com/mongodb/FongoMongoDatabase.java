/**
 * Copyright (C) 2015 Deveryware S.A. All Rights Reserved.
 */
package com.mongodb;

import com.github.fakemongo.Fongo;
import com.mongodb.client.MongoDatabase;
import org.bson.codecs.configuration.CodecRegistry;

/**
 *
 */
public class FongoMongoDatabase extends MongoDatabaseImpl {

  private final String databaseName;
  private final Fongo fongo;

  public FongoMongoDatabase(final String databaseName, final Fongo fongo) {
    this(databaseName, fongo, MongoClient.getDefaultCodecRegistry(), ReadPreference.primary(), WriteConcern.ACKNOWLEDGED);
  }

  private FongoMongoDatabase(final String databaseName, final Fongo fongo, final CodecRegistry codecRegistry, final ReadPreference readPreference, final WriteConcern writeConcern) {
    super(databaseName, codecRegistry, readPreference, writeConcern, fongo);
    this.databaseName = databaseName;
    this.fongo = fongo;
  }

  @Override
  public MongoDatabase withCodecRegistry(CodecRegistry codecRegistry) {
    return new FongoMongoDatabase(databaseName, fongo, codecRegistry, super.getReadPreference(), super.getWriteConcern());
  }

  @Override
  public MongoDatabase withReadPreference(ReadPreference readPreference) {
    return new FongoMongoDatabase(databaseName, fongo, super.getCodecRegistry(), readPreference, super.getWriteConcern());
  }

  @Override
  public MongoDatabase withWriteConcern(WriteConcern writeConcern) {
    return new FongoMongoDatabase(databaseName, fongo, super.getCodecRegistry(), super.getReadPreference(), writeConcern);
  }
}
