package com.github.fakemongo;

import com.mongodb.connection.ServerVersion;

public class FongoV3ServerVersion3_2Test extends AbstractFongoV3Test {

  @Override
  public ServerVersion serverVersion() {
    return Fongo.V3_2_SERVER_VERSION;
  }
}
