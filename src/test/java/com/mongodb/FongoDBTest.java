package com.mongodb;

import com.github.fakemongo.junit.FongoRule;
import static org.assertj.core.api.Assertions.assertThat;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;

/**
 * @author Anton Bobukh <abobukh@yandex-team.ru>
 */
public class FongoDBTest {

  private final ReadPreference preference = ReadPreference.nearest();

  @Rule
  public FongoRule fongoRule = new FongoRule(!true);
  private DB db;

  @Before
  public void setUp() {
    db = fongoRule.getDB();
  }

  @Test
  public void commandGetLastErrorAliases() {
    BasicDBObject command;

    command = new BasicDBObject("getlasterror", 1);
    Assert.assertTrue(db.command(command, preference).containsField("ok"));

    command = new BasicDBObject("getLastError", 1);
    Assert.assertTrue(db.command(command, preference).containsField("ok"));
  }

  @Test
  public void commandFindAndModifyAliases() {
    BasicDBObject command = new BasicDBObject("findandmodify", "test").append("remove", true);
    CommandResult commandResult = db.command(command, preference);
    assertThat(commandResult.toMap()).containsEntry("ok", 1.0).containsKey("value");

    command = new BasicDBObject("findAndModify", "test").append("remove", true);
    commandResult = db.command(command, preference);
    assertThat(commandResult.toMap()).containsEntry("ok", 1.0).containsKey("value");
  }

  @Test
  public void commandFindAndModifyNeedRemoveOrUpdate() {
    BasicDBObject command = new BasicDBObject("findandmodify", "test");
    CommandResult commandResult = db.command(command, preference);
    assertThat(commandResult.toMap()).containsEntry("ok", .0).containsEntry("errmsg", "need remove or update");
  }

  @Test
  public void commandBuildInfoAliases() {
    BasicDBObject command;

    command = new BasicDBObject("buildinfo", 1);
    Assert.assertTrue(db.command(command, preference).containsField("version"));

    command = new BasicDBObject("buildInfo", 1);
    Assert.assertTrue(db.command(command, preference).containsField("version"));
  }

  @Test
  @Ignore("not sure to understant the test")
  public void commandMapReduceAliases() {
    BasicDBObject command;

    command = new BasicDBObject("mapreduce", "test").append("out", new BasicDBObject("inline", 1));
    CommandResult commandResult = db.command(command, preference);
    assertThat(commandResult.toMap()).containsKey("results");
    assertThat(db.command(command, preference).toMap()).containsKey("results");

    command = new BasicDBObject("mapReduce", "test").append("out", new BasicDBObject("inline", 1));
    commandResult = db.command(command, preference);
    assertThat(commandResult.toMap()).containsKey("results");
  }

}
