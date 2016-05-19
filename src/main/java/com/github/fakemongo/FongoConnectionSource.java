package com.github.fakemongo;

import com.mongodb.binding.ConnectionSource;
import com.mongodb.connection.Connection;
import com.mongodb.connection.ServerConnectionState;
import com.mongodb.connection.ServerDescription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
class FongoConnectionSource implements ConnectionSource {
  private final static Logger LOG = LoggerFactory.getLogger(FongoConnectionSource.class);

  private final Fongo fongo;

  public FongoConnectionSource(Fongo fongo) {
    this.fongo = fongo;
  }

  @Override
  public ServerDescription getServerDescription() {
    return ServerDescription.builder().address(fongo.getServerAddress()).state(ServerConnectionState.CONNECTED).version(fongo.getServerVersion()).build();
  }

  @Override
  public Connection getConnection() {
    return new FongoConnection(this.fongo);
  }

  @Override
  public ConnectionSource retain() {
    return this;
  }

  @Override
  public int getCount() {
    return 0;
  }

  @Override
  public void release() {
  }
}
