package com.github.fakemongo;

import com.mongodb.connection.ServerVersion;

public class FongoV3ServerVersion3Test extends AbstractFongoV3Test {

  @Override
  public ServerVersion serverVersion() {
    return Fongo.DEFAULT_SERVER_VERSION;
  }
}
