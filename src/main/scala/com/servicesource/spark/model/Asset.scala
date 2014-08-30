package com.servicesource.spark.model

import play.api.libs.json._
import play.api.libs.functional.syntax._

import org.bson.BSONObject
import org.bson.BasicBSONObject

case class Asset(id: String, disp: String, amount : Option[Double], customer : Option[String], product : Option[String])

object Asset {
  
  val name = "app.assets"

  implicit val jsonReads: Reads[Asset] = (
    (__ \ "_id" \ "$oid").read[String] and
    (__ \ "displayName").read[String] and 
    (__ \ "amount" \ "normalizedAmount" \ "amount").readOpt[Double] and
    ((__ \ "relationships" \ "customer" \ "targets") (0) \ "displayName").readOpt[String] and
    ((__ \ "relationships" \ "product" \ "targets") (0) \ "id").readOpt[String]
    )(Asset.apply _)
    
  def mapper (item : (Object, BSONObject)) : Asset= {
    var ret : Asset = Asset(null, null, Some(0), None, None);
    if (item!=null) {
	    val json = Json.parse(item._2.toString)
	    json.validate[Asset] match {
	      case s: JsSuccess[Asset] => ret = s.get
	      case e: JsError => {
	        println("Errors: "+ JsError.toFlatJson(e).toString())
	      }
	    }
    } 
    ret
  }
}