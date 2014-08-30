package com.servicesource.spark.model

import play.api.libs.json._
import play.api.libs.functional.syntax._

import org.bson.BSONObject
import org.bson.BasicBSONObject

case class Task(id: String, disp: String) extends CollectionModel {
  implicit override val jsonReads: Reads[CollectionModel] = (
    (__ \ "_id" \ "$oid").read[String] and
    (__ \ "displayName").read[String] )(Task.apply _)
}

object Task {
  
  val emptyObj : Task = new Task(null, null)
  
  val name = "app.tasks"
    
  def mapper (item : (Object, BSONObject)) : CollectionModel = {
    emptyObj.mapper(item)
  }
}