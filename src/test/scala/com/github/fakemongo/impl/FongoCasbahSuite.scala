package com.github.fakemongo.impl

import java.util.Date

import com.github.fakemongo.Fongo
import com.mongodb.casbah.Imports._
import org.scalatest.{FunSuite, ShouldMatchers}

class FongoCasbahSuite extends FunSuite with ShouldMatchers {

  test("Fongo bug with $in") {
                               //                               val fongo = MongoClient("localhost") //"in-memory MongoDB")
                               val fongo = new Fongo("in-memory MongoDB")
                               val collection = fongo.getDB("test").asScala.getCollection("items").asScala

                               collection.insert(DBObject("name" -> "ABC"), DBObject("name" -> "DEF"))

                               val query = "name" $in Seq("ABC", "bunch of monkeys")
                               collection.find(query).hasNext shouldBe true
                             }

  test("Fongo with $or") {
                           //                                                                                                            val fongo = MongoClient("localhost")
                           val fongo = new Fongo("in-memory MongoDB")
                           val collection = fongo.getDB("test").getCollection("items")

                           val now = new Date()
                           collection.insert(DBObject("published" -> true, "expiration" -> now))

                           val query: DBObject = $and(
                                                       "published" $ne false,
                                                       $or("startDate" $exists false, "startDate" $lt now),
                                                       $or("expiration" $exists false, "expiration" $gte now)
                                                     )
                           collection.find(query).hasNext shouldBe true
                         }
}
