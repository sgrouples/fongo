package com.github.fakemongo;

import com.github.fakemongo.junit.FongoRule;
import com.mongodb.AggregationOutput;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.util.JSON;
import java.util.List;
import org.assertj.core.api.Assertions;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;

public class FongoAggregateGroupTest {

  @Rule
  public FongoRule fongoRule = new FongoRule(false);

  /**
   * See http://docs.mongodb.org/manual/reference/aggregation/concat/
   */
  @Test
  @Ignore("@twillouer : lots work to do.")
  public void testConcat() {
    DBCollection coll = fongoRule.newCollection();
    fongoRule.insertJSON(coll, "[{ _id: 1, item: { sec: \"dessert\", category: \"pie\", type: \"apple\" } },\n" +
        "{ _id: 2, item: { sec: \"dessert\", category: \"pie\", type: \"cherry\" } },\n" +
        "{ _id: 3, item: { sec: \"main\", category: \"pie\", type: \"shepherd's\" } },\n" +
        "{ _id: 4, item: { sec: \"main\", category: \"pie\", type: \"chicken pot\" } }]");

    DBObject project = fongoRule.parseDBObject("{ $group: { _id:\n" +
        "                                    { $concat: [ \"$item.sec\",\n" +
        "                                                 \": \",\n" +
        "                                                 \"$item.category\"\n" +
        "                                               ]\n" +
        "                                    },\n" +
        "                               count: { $sum: 1 }\n" +
        "                             }\n" +
        "                   }");

    // When
    AggregationOutput output = coll.aggregate(project);

    // Then
    assertTrue(output.getCommandResult().ok());

    List<DBObject> result = (List<DBObject>) output.getCommandResult().get("result");
    assertNotNull(result);
    assertEquals(JSON.parse("[\n" +
        "               { \"_id\" : \"main: pie\", \"count\" : 2 },\n" +
        "               { \"_id\" : \"dessert: pie\", \"count\" : 2 }\n" +
        "             ]"), result);
  }

  @Test
  public void should_$avg_return_the_average_value() {
    // Given
    DBCollection coll = fongoRule.newCollection();
    fongoRule.insertJSON(coll, "[{ \"_id\" : \"1\", \"test\" : 1.0, \"City\" : \"Madrid\", \"groupType\" : \"control\"},\n" +
        "{ \"_id\" : \"2\", \"test\" : 10.0, \"City\" : \"Madrid\", \"groupType\" : \"treatment\"},\n" +
        "{ \"_id\" : \"3\", \"test\" : 2.0, \"City\" : \"Madrid\", \"groupType\" : \"control\"},\n" +
        "{ \"_id\" : \"4\", \"test\" : 20.0, \"City\" : \"Madrid\", \"groupType\" : \"treatment\"},\n" +
        "{ \"_id\" : \"5\", \"test\" : 3.0, \"City\" : \"London\", \"groupType\" : \"control\"},\n" +
        "{ \"_id\" : \"6\", \"test\" : 30.0, \"City\" : \"London\", \"groupType\" : \"treatment\"},\n" +
        "{ \"_id\" : \"7\", \"test\" : 4.0, \"City\" : \"Paris\", \"groupType\" : \"control\"},\n" +
        "{ \"_id\" : \"8\", \"test\" : 40.0, \"City\" : \"Paris\", \"groupType\" : \"treatment\"},\n" +
        "{ \"_id\" : \"9\", \"test\" : 5.0, \"City\" : \"Paris\", \"groupType\" : \"control\"},\n" +
        "{ \"_id\" : \"10\", \"test\" : 50.0, \"City\" : \"Paris\", \"groupType\" : \"treatment\"}]");

    // When
    AggregationOutput output = coll.aggregate(fongoRule.parseList("[{ \n" +
        "  \"$group\" : { \n" + //
        "      \"_id\" : { \"groupType\" : \"$groupType\" , \"City\" : \"$City\"} , \n" + //
        "      \"average\" : { \"$avg\" : \"$test\"}\n" +                                   //
        "  }\n" +                                                                             //
        "}]"));

    // Then
    Assertions.assertThat(output.results()).containsAll(fongoRule.parseList("[{\"_id\":{\"groupType\":\"treatment\", \"City\":\"Paris\"}, \"average\":45.0}, {\"_id\":{\"groupType\":\"treatment\", \"City\":\"London\"}, \"average\":30.0}, {\"_id\":{\"groupType\":\"control\", \"City\":\"London\"}, \"average\":3.0}, {\"_id\":{\"groupType\":\"control\", \"City\":\"Paris\"}, \"average\":4.5}, {\"_id\":{\"groupType\":\"treatment\", \"City\":\"Madrid\"}, \"average\":15.0}, {\"_id\":{\"groupType\":\"control\", \"City\":\"Madrid\"}, \"average\":1.5}]"));
  }
}
