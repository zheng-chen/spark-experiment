package com.servicesource.spark.model

import play.api.libs.json._
import play.api.libs.functional.syntax._

import org.bson.BSONObject
import org.bson.BasicBSONObject

case class Opportunity(id: String, disp : Option[String],  amount: Option[Double], amountCode : Option[String],     
    targetAmount: Option[Double], targetAmountCode : Option[String], baseQuote : Option[String], 
    primaryQuote : Option[String], result : Option [String]) extends CollectionModel {
  
  implicit val jsonReads: Reads[CollectionModel] = (
    (__ \ "_id" \ "$oid").read[String] and
    (__ \ "displayName") .readOpt[String] and
    (__ \ "amount" \ "amount" ).readOpt[Double] and
    (__ \ "amount" \ "code" \ "name").readOpt[String] and
    (__ \ "targetAmount" \ "amount" ).readOpt[Double] and
    (__ \ "targetAmount" \ "code" \ "name").readOpt[String] and
    ((__ \ "relationships" \ "baseQuote" \ "targets") (0) \ "id").readOpt[String] and 
    ((__ \ "relationships" \ "primaryQuote" \ "targets") (0) \ "id").readOpt[String] and 
    (__ \ "result" \ "name" ).readOpt[String] 
    )(Opportunity.apply _)
    
}

object Opportunity {
  
  val name = "app.opportunities"
    
  val emptyObj : Opportunity = new Opportunity(null, None, None, None, None, None, None, None, None)
    
  def mapper (item : (Object, BSONObject)) : CollectionModel = {
    emptyObj.mapper(item)
  }
  
}