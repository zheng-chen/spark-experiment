package com.servicesource.spark.model

import play.api.libs.json._
import play.api.libs.functional.syntax._

import org.bson.BSONObject
import org.bson.BasicBSONObject

import java.util.Date

case class Quote(id: String, amount: Option[Double], amountCode : Option[String],     
    isBase: Option[Boolean], isDraft : Option[Boolean], isInvalid : Option[Boolean], 
    isPrimary : Option[Boolean]) extends CollectionModel {
  
  implicit val jsonReads: Reads[CollectionModel] = (
    (__ \ "_id" \ "$oid").read[String] and
    (__ \ "amount" \ "amount" ).readOpt[Double] and
    (__ \ "amount" \ "code" \ "name").readOpt[String] and
    (__ \ "isBase").readOpt[Boolean] and
    (__ \ "isDraft").readOpt[Boolean] and
    (__ \ "isInvalid").readOpt[Boolean] and
    (__ \ "isPrimary" ).readOpt[Boolean] 
    )(Quote.apply _)
}

object Quote {
  
  val name = "app.quotes"

  val emptyObj : Quote = new Quote(null, None, None, None, None, None, None)
    
  def mapper (item : (Object, BSONObject)) : Quote = {
    emptyObj.mapper(item).asInstanceOf[Quote]
  }
  
}