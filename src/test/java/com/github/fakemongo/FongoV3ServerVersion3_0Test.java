package com.github.fakemongo;

import com.mongodb.connection.ServerVersion;

public class FongoV3ServerVersion3_0Test extends AbstractFongoV3Test {

  @Override
  public ServerVersion serverVersion() {
    return Fongo.V3_SERVER_VERSION;
  }
}
