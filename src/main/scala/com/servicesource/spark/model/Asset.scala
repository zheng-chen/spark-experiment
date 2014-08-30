package com.servicesource.spark.model

import play.api.libs.json._
import play.api.libs.functional.syntax._

import org.bson.BSONObject
import org.bson.BasicBSONObject

case class Asset(id: String, disp: String, amount : Option[Double], customer : Option[String], 
    product : Option[String]) extends CollectionModel {
  
  implicit val jsonReads: Reads[CollectionModel] = (
    (__ \ "_id" \ "$oid").read[String] and
    (__ \ "displayName").read[String] and 
    (__ \ "amount" \ "normalizedAmount" \ "amount").readOpt[Double] and
    ((__ \ "relationships" \ "customer" \ "targets") (0) \ "displayName").readOpt[String] and
    ((__ \ "relationships" \ "product" \ "targets") (0) \ "id").readOpt[String]
    )(Asset.apply _)
  
}

object Asset {
  
  val name = "app.assets"
  val emptyObj : Asset = new Asset(null, null, None, None, None)
    
  def mapper (item : (Object, BSONObject)) : Asset = {
    emptyObj.mapper(item).asInstanceOf[Asset]
  }
}