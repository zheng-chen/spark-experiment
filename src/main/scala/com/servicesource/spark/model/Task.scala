package com.servicesource.spark.model

import play.api.libs.json._
import play.api.libs.functional.syntax._

import org.apache.spark.sql._

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
    
  val tableName = "tasks"
    
  val mongoQuery : String = null
  
  val sqlQuery = Seq ("SELECT id, disp FROM tasks WHERE disp LIKE \"%TASK%\"")
      
  def sqlForeachHandler (row : Row) = {
    println("ID: " + row(0) + ", DisplayName: " + row(1))
  }
    
  def mapper (item : (Object, BSONObject)) : Task = {
    emptyObj.mapper(item).asInstanceOf[Task]
  }  
  
}