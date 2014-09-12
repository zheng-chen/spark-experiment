package com.servicesource.spark.model

import play.api.libs.json._
import play.api.libs.functional.syntax._

import org.apache.spark.sql._

import org.bson.BSONObject
import org.bson.BasicBSONObject

import java.util.Date

case class Offer(id: String, amount: Option[Double],  productDisp : Option[String], customerId:Option[String], 
    result:Option[String]) extends CollectionModel {
  
  implicit val jsonReads: Reads[CollectionModel] = (
    (__ \ "_id" \ "$oid").read[String] and
    (__ \ "amount" \ "normalizedAmount" \ "amount").readOpt[Double] and
    (__ \ "product" \ "displayName" ).readOpt[String] and
    ((__ \ "relationships" \ "customer" \ "targets") (0) \ "key").readOpt[String] and
    (__ \ "result" \ "name" ).readOpt[String]
    )(Offer.apply _)
}

object Offer {
  
  val name = "app.offers"

  val emptyObj : Offer = new Offer(null, None, None, None, None)
  
  val tableName = "offers"
    
  val mongoQuery = "{\"result.name\":{$exists:true}}"
  
  val sqlQuery = Seq ("SELECT productDisp, result, count(id) FROM offers group by productDisp, result")
      
  def sqlForeachHandler (row : Row) = {
    println("Product: " + row(0) + ", Result: " + row(1) + ", Count: " + row(2))
  }
    
  def mapper (item : (Object, BSONObject)) : Offer = {
    emptyObj.mapper(item).asInstanceOf[Offer]
  }
  
}