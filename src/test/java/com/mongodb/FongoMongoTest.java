package com.mongodb;

import com.github.fakemongo.Fongo;
import java.util.Collections;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import org.junit.Test;

public class FongoMongoTest {

  @Test
  public void mongoClientHasOptions() {
    MongoClient mongoClient = new Fongo("test").getMongo();
    assertNotNull(mongoClient.getMongoClientOptions());
    assertNotNull(mongoClient.getMongoOptions());
  }

  @Test
  public void mongoHasWriteConcern() {
    Fongo fongo = new Fongo("test");
    assertEquals(WriteConcern.ACKNOWLEDGED, fongo.getMongo().getWriteConcern());
    assertEquals(WriteConcern.ACKNOWLEDGED, fongo.getWriteConcern());
  }

  @Test
  public void mongoAllAdressOverride() {
    MongoClient mongoClient = new Fongo("test").getMongo();

    assertNotNull(mongoClient.getAllAddress());
    assertEquals(Collections.singletonList(new ServerAddress()), mongoClient.getAllAddress());
  }
}
