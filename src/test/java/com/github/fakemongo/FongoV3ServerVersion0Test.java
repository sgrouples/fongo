package com.github.fakemongo;

import com.mongodb.connection.ServerVersion;

public class FongoV3ServerVersion0Test extends AbstractFongoV3Test {

  @Override
  public ServerVersion serverVersion() {
    return Fongo.OLD_SERVER_VERSION;
  }
}
