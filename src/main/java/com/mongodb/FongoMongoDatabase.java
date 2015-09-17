package com.mongodb;

import com.github.fakemongo.Fongo;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.codecs.configuration.CodecRegistry;

/**
 *
 */
public class FongoMongoDatabase extends MongoDatabaseImpl {

  private final Fongo fongo;

  public FongoMongoDatabase(final String databaseName, final Fongo fongo) {
    this(databaseName, fongo, MongoClient.getDefaultCodecRegistry(), ReadPreference.primary(), WriteConcern.ACKNOWLEDGED);
  }

  private FongoMongoDatabase(final String databaseName, final Fongo fongo, final CodecRegistry codecRegistry, final ReadPreference readPreference, final WriteConcern writeConcern) {
    super(databaseName, codecRegistry, readPreference, writeConcern, fongo);
    this.fongo = fongo;
  }

  @Override
  public MongoDatabase withCodecRegistry(CodecRegistry codecRegistry) {
    return new FongoMongoDatabase(super.getName(), this.fongo, codecRegistry, super.getReadPreference(), super.getWriteConcern());
  }

  @Override
  public MongoDatabase withReadPreference(ReadPreference readPreference) {
    return new FongoMongoDatabase(super.getName(), this.fongo, super.getCodecRegistry(), readPreference, super.getWriteConcern());
  }

  @Override
  public MongoDatabase withWriteConcern(WriteConcern writeConcern) {
    return new FongoMongoDatabase(super.getName(), this.fongo, super.getCodecRegistry(), super.getReadPreference(), writeConcern);
  }

  @Override
  public <TDocument> MongoCollection<TDocument> getCollection(final String collectionName, final Class<TDocument> documentClass) {
    return new FongoMongoCollection<TDocument>(this.fongo, new MongoNamespace(super.getName(), collectionName), documentClass, super.getCodecRegistry(), super.getReadPreference(),
        super.getWriteConcern());

  }
}
