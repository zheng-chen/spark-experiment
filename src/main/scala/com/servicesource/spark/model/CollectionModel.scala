package com.servicesource.spark.model

import play.api.libs.json._
import play.api.libs.functional.syntax._

import org.bson.BSONObject
import org.bson.BasicBSONObject

abstract class CollectionModel extends Product {
  
  override def toString () : String = {
    val  items = this.productIterator.toList.map {
        case Some(s:String) => "\""+s+"\""
        case Some(d:Double) => d
        case s: String => "\""+s+"\""
        case _ => "\"\""
    }
    items.mkString (",")
  }
  
  implicit val jsonReads: Reads[CollectionModel]
  
  def mapper (item : (Object, BSONObject)) : CollectionModel = {
    if (item!=null) {
	    val json = Json.parse(item._2.toString)
	    json.validate[CollectionModel] match {
	      case s: JsSuccess[CollectionModel] => s.get
	      case e: JsError => {
	        println("Errors: "+ JsError.toFlatJson(e).toString())
	        null
	      }
	    }
    } else {
      null
    }
  }
 
}
