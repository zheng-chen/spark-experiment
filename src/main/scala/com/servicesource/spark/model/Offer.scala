package com.servicesource.spark.model

import play.api.libs.json._
import play.api.libs.functional.syntax._

import org.apache.spark.sql._

import com.mongodb.casbah.Imports._
import com.servicesource.spark.Settings

import org.bson.BSONObject
import org.bson.BasicBSONObject

import java.util.Date

case class Offer(id: String, amount: Option[Double],  productId : Option[String], customerId:Option[String], 
    result:Option[String]) extends CollectionModel {
  
  implicit val jsonReads: Reads[CollectionModel] = (
    (__ \ "_id" \ "$oid").read[String] and
    (__ \ "amount" \ "normalizedAmount" \ "amount").readOpt[Double] and
    ((__ \ "relationships" \ "product" \ "targets") (0) \ "key").readOpt[String] and
    ((__ \ "relationships" \ "customer" \ "targets") (0) \ "key").readOpt[String] and
    (__ \ "result" \ "name" ).readOpt[String]
    )(Offer.apply _)
}

object Offer {
  
  val name = "app.offers"

  val emptyObj : Offer = new Offer(null, None, None, None, None)
  
  val offerByProdCol = MongoConnection()(Settings.get("cubeDbName"))("spark.offers_by_product")
  
  val tableName = "offers"
    
  val mongoQuery = "{\"result.name\":\"houseAccount\"}"
  
  val sqlQuery = Seq ("SELECT productId, result, count(id) FROM offers group by productId, result")
      
  def sqlForeachHandler (row : Row) = {
//    println("Product: " + row(0) + ", Result: " + row(1) + ", Count: " + row(2))
    val existingEntry = offerByProdCol.findOne(MongoDBObject("product" -> row(0)))
    val res = row(1).toString
    
    existingEntry match {
      case Some(entry) => {
        val results = entry.as[DBObject] ("results")
        if (results.get(res)==null) {
          results.put(res, row.getLong(2).toInt)
        }
        val q = MongoDBObject("_id" -> entry.as[DBObject]("_id"))
        val update = $set("results"->results)
        offerByProdCol.update(q, update)
      }
      case None => {
        val input = MongoDBObject.newBuilder
        input +=  "product" -> row(0)
        input += "results" -> DBObject(res -> row.getLong(2).toInt) 
        offerByProdCol += input.result
      }
    }
  }
    
  def mapper (item : (Object, BSONObject)) : Offer = {
    emptyObj.mapper(item).asInstanceOf[Offer]
  }
  
}