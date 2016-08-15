package com.github.fakemongo.impl.aggregation;

import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.annotations.ThreadSafe;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 */
@ThreadSafe
public class Sample extends PipelineKeyword {
  public static final Sample INSTANCE = new Sample();

  private static final Random RANDOM = new Random();

  private Sample() {
  }

  /**
   */
  @Override
  public DBCollection apply(DB originalDB, DBCollection coll, DBObject object) {
    DBObject dbObject = (DBObject) object.get(getKeyword());
    int size = ((Number) dbObject.get("size")).intValue();

    List<DBObject> objects = new ArrayList<DBObject>(size);
    int count = (int) coll.count();
    if (count != 0) {
      for (int i = 0; i < size; i++) {
        objects.addAll(coll.find().skip(RANDOM.nextInt(count)).limit(1).toArray());
      }
    }

    return dropAndInsert(coll, objects);
  }

  @Override
  public String getKeyword() {
    return "$sample";
  }
}
