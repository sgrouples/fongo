package com.mongodb;
import com.github.fakemongo.Fongo;
import org.jongo.Jongo;
import org.jongo.MongoCollection;
import org.junit.Test;

public class FongoCastTest {

    @Test(expected=DuplicateKeyException.class)
    public void checkFongoSupportsMajority() {
        Jongo jongo = new Jongo(new Fongo("fake-mongo-server").getDB("fake-database"));
        MongoCollection turnips = jongo.getCollection("turnip").withWriteConcern(WriteConcern.MAJORITY);
        turnips.insert(new Turnip("turnip"));
        turnips.insert(new Turnip("turnip"));
    }

    private class Turnip {
        private String _id;

        public Turnip() {
        }

        public Turnip(String _id) {
            this._id = _id;
        }
    }
}
