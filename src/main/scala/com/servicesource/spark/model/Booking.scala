package com.servicesource.spark.model

import play.api.libs.json._
import play.api.libs.functional.syntax._

import org.bson.BSONObject
import org.bson.BasicBSONObject

case class Booking(id: String, stateName: Option[String], soNumber : Option[String]) extends CollectionModel {
  implicit val jsonReads: Reads[CollectionModel] = (
    (__ \ "_id" \ "$oid").read[String] and
    (__ \ "flows" \ "bookingStages" \ "state" \ "name").readOpt[String] and
    (__ \ "soNumber") .readOpt[String]
    )(Booking.apply _)
}

object Booking {
  
  val name = "app.bookings"
    
  val emptyObj : Booking = new Booking(null, None, None)
  
  def mapper (item : (Object, BSONObject)) : Booking = {
    emptyObj.mapper(item).asInstanceOf[Booking]
  }
  
}