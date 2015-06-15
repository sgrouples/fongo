package com.github.fakemongo.impl.aggregation;

import com.mongodb.DBCollection;
import com.mongodb.DBObject;

import java.util.List;

/**
 * User: gdepourtales
 * 2015/06/15
 */
public class Out extends PipelineKeyword {

    public static final Out INSTANCE = new Out();

    @Override
    public DBCollection apply(DBCollection coll, DBObject object) {
        List<DBObject> objects = coll.find().toArray();
        DBCollection newCollection = coll.getDB().getCollection(object.get(getKeyword()).toString());
        newCollection.insert(objects);
        return coll;
    }

    @Override
    public String getKeyword() {
        return "$out";
    }
}
