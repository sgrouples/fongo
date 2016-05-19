package com.github.fakemongo;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.fakemongo.junit.FongoRule;
import com.github.fakemongo.test.beans.TestChildBean;
import com.github.fakemongo.test.beans.TestParentBean;
import com.mongodb.AggregationOutput;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import org.apache.commons.lang3.RandomUtils;
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import static org.apache.commons.lang3.RandomStringUtils.randomAlphabetic;
import static org.junit.Assert.*;

/**
 * @author Kollivakkam Raghavan
 * @created 4/22/2016
 */
public class LookupTest {

  private static final Logger LOG = LoggerFactory.getLogger(LookupTest.class);

  @Rule
  public FongoRule fongoRule = new FongoRule(false);

  @Test
  public void mustLookupCollectionFromSecondaryCollectionSimple() throws Exception {
    // start with 2 collections
    DBCollection primaryColl = fongoRule.newCollection();
    fongoRule.insertJSON(primaryColl, "[\n" +
                                      "    {\n" +
                                      "        \"_id\" : \"p1\"\n" +
                                      "    },\n" +
                                      "    {\n" +
                                      "        \"_id\" : \"p2\"\n" +
                                      "    }\n" +
                                      "]\n");
    DBCollection secondaryColl = fongoRule.newCollection();
    fongoRule.insertJSON(secondaryColl, "[" +
                                        "    {\n" +
                                        "        \"_id\" : \"s1\",\n" +
                                        "        \"parentId\" : \"p1\"\n" +
                                        "    },\n" +
                                        "    {\n" +
                                        "        \"_id\" : \"s2\",\n" +
                                        "        \"parentId\" : \"p1\"\n" +
                                        "    },\n" +
                                        "    {\n" +
                                        "        \"_id\" : \"s3\",\n" +
                                        "        \"parentId\" : \"p2\"\n" +
                                        "    },\n" +
                                        "    {\n" +
                                        "        \"_id\" : \"s4\",\n" +
                                        "        \"parentId\" : \"p2\"\n" +
                                        "    }\n" +
                                        "]");
    DBObject lookup = getLookupQuery(secondaryColl);
    List<DBObject> pipeline = new ArrayList<DBObject>();
    pipeline.add(lookup);
    AggregationOutput output = primaryColl.aggregate(pipeline);
    Iterable<DBObject> result = output.results();
    assertNotNull(result);
  }

  @Test
  public void mustLookupCollectionFromSecondaryCollectionAdvanced() throws Exception {
    // create a random number of parent collections
    int randomParentCount = RandomUtils.nextInt(1, 6);
    List<TestParentBean> parentBeans = new ArrayList<TestParentBean>(randomParentCount);

    // keep a map for quick lookup and verification.
    Map<String, TestParentBean> parentBeanMap = new HashMap<String, TestParentBean>();
    Map<String, TestParentBean> childBeanMap = new HashMap<String, TestParentBean>();
    // keep a map of the child counts for verification
    Map<String, Integer> childCountMap = new HashMap<String, Integer>();

    // create some random beans
    List<Integer> childCounts = new ArrayList<Integer>(randomParentCount);
    List<TestChildBean> childBeans = new ArrayList<TestChildBean>();
    for (int i = 0; i < randomParentCount; i++) {
      TestParentBean parentBean = createTestParentBean();
      parentBeans.add(parentBean);
      final String parentBeanId = parentBean.getId();
      parentBeanMap.put(parentBeanId, parentBean);
      // for each parent bean create a random set of child beans.
      int randomChildCount = RandomUtils.nextInt(1, 4);
      childCountMap.put(parentBeanId, randomChildCount);
      childCounts.add(randomChildCount);
      for (int j = 0; j < randomChildCount; j++) {
        TestChildBean childBean = getTestChildBean(parentBean);
        childBeans.add(childBean);
        childBeanMap.put(childBean.getId(), childBean);
      }
    }
    ObjectMapper objectMapper = new ObjectMapper();
    // start with 2 collections
    DBCollection primaryColl = fongoRule.newCollection();
    fongoRule.insertJSON(primaryColl, objectMapper.writeValueAsString(parentBeans));
    DBCollection secondaryColl = fongoRule.newCollection();
    fongoRule.insertJSON(secondaryColl, objectMapper.writeValueAsString(childBeans));
    DBObject lookup = getLookupQuery(secondaryColl);
    List<DBObject> pipeline = new ArrayList<DBObject>();
    pipeline.add(lookup);
    AggregationOutput output = primaryColl.aggregate(pipeline);
    Iterable<DBObject> result = output.results();
    assertNotNull(result);
    verifyResults(result, parentBeanMap, childBeanMap, childCountMap);
  }

  @Test
  public void mustReturnNoValuesWhenParentCollectionHasNoChildren() throws JsonProcessingException {
    int randomParentCount = RandomUtils.nextInt(1, 6);
    List<TestParentBean> parentBeans = new ArrayList<TestParentBean>(randomParentCount);

    // keep a map for quick lookup and verification.
    final Map<String, TestParentBean> parentBeanMap = new HashMap<String, TestParentBean>();

    for (int i = 0; i < randomParentCount; i++) {
      TestParentBean parentBean = createTestParentBean();
      parentBeans.add(parentBean);
      final String parentBeanId = parentBean.getId();
      parentBeanMap.put(parentBeanId, parentBean);
    }
    // create the 2 collections
    DBCollection primaryColl = fongoRule.newCollection();
    DBCollection secondaryColl = fongoRule.newCollection();
    final ObjectMapper objectMapper = new ObjectMapper();
    // start with 2 collections
    fongoRule.insertJSON(primaryColl, objectMapper.writeValueAsString(parentBeans));
    DBObject lookup = getLookupQuery(secondaryColl);
    List<DBObject> pipeline = new ArrayList<DBObject>();
    pipeline.add(lookup);
    AggregationOutput output = primaryColl.aggregate(pipeline);
    Iterable<DBObject> result = output.results();
    assertNotNull(result);
    final int[] actualCount = {0};
    result.forEach(new Consumer<DBObject>() {
      @Override
      public void accept(DBObject dbObject) {
        actualCount[0]++;
        try {
          TestParentBean actualParentBean = objectMapper.readValue(dbObject.toString(), TestParentBean.class);
          assertTrue(actualParentBean.equals(parentBeanMap.get(actualParentBean.getId())));
          assertEquals(actualParentBean.getSecondaryCollItems().size(), 0);
        }
        catch (IOException e) {
          LOG.error("Error deserializing dbobject", e);
          assertTrue(false);
        }
      }
    });
    assertEquals(actualCount[0], randomParentCount);
  }

  private DBObject getLookupQuery(DBCollection secondaryColl) {
    return fongoRule.parseDBObject("{$lookup : {\n" +
                                   "        \"from\" :\"" + secondaryColl.getName() + "\"," +
                                   "        \"localField\" : \"_id\",\n" +
                                   "        \"foreignField\" : \"parentId\",\n" +
                                   "        \"as\" : \"secondaryCollItems\"\n" +
                                   "    }\n" +
                                   "}");
  }

  @Test
  public void mustReturnNoValuesWhenParentCollectionIsEmpty() {
    // create the 2 collections
    DBCollection primaryColl = fongoRule.newCollection();
    DBCollection secondaryColl = fongoRule.newCollection();
    DBObject lookup = getLookupQuery(secondaryColl);
    List<DBObject> pipeline = new ArrayList<DBObject>();
    pipeline.add(lookup);
    AggregationOutput output = primaryColl.aggregate(pipeline);
    Iterable<DBObject> result = output.results();
    assertNotNull(result);
    final int[] actualCount = {0};
    result.forEach(new Consumer<DBObject>() {
      @Override
      public void accept(DBObject dbObject) {
        actualCount[0]++;
      }
    });
    assertEquals(actualCount[0], 0);
  }

  private void verifyResults(Iterable<DBObject> result, final Map<String, TestParentBean> parentBeanMap,
                             final Map<String, TestParentBean> childBeanMap, final Map<String, Integer> childCountMap) {
    final ObjectMapper mapper = new ObjectMapper();
    result.forEach(new Consumer<DBObject>() {
      @Override
      public void accept(DBObject dbObject) {
        String jsonString = dbObject.toString();
        try {
          TestParentBean parentBean = mapper.readValue(jsonString, TestParentBean.class);
          final String parentBeanId = parentBean.getId();
          assertTrue("Parent bean equality", parentBean.equals(parentBeanMap.get(parentBeanId)));
          int childCount = parentBean.getSecondaryCollItems() != null ? parentBean.getSecondaryCollItems().size() : 0;
          assertTrue("Match child counts", childCount == childCountMap.get(parentBeanId));
          // finally match all the children
          for (TestChildBean childBean : parentBean.getSecondaryCollItems()) {
            final String childBeanId = childBean.getId();
            assertTrue("Child bean equality", childBean.equals(childBeanMap.get(childBeanId)));
          }
        }
        catch (IOException e) {
          LOG.error("Error deserializing JSON {}", jsonString, e);
          assertTrue(false);
        }
      }
    });
  }

  private TestChildBean getTestChildBean(TestParentBean parentBean) {
    TestChildBean childBean = new TestChildBean();
    childBean.setId(randomAlphabetic(10));
    childBean.setAttr(randomAlphabetic(10));
    childBean.setParentId(parentBean.getId());
    return childBean;
  }

  private TestParentBean createTestParentBean() {
    TestParentBean parentBean = new TestParentBean();
    parentBean.setId(randomAlphabetic(10));
    parentBean.setAttr(randomAlphabetic(10));
    return parentBean;
  }

}