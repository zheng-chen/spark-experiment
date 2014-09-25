package com.servicesource.spark.model

import play.api.libs.json._
import play.api.libs.functional.syntax._

import org.apache.spark.sql._

import org.bson.BSONObject
import org.bson.BasicBSONObject

case class AppProduct (id: String, disp: String) extends CollectionModel {
  
  implicit override val jsonReads: Reads[CollectionModel] = (
    (__ \ "_id" \ "$oid").read[String] and
    (__ \ "displayName").read[String] 
    )(AppProduct.apply _)
}

object AppProduct {
  
  val emptyObj : AppProduct = new AppProduct(null, null)
  
  val name = "app.products"
    
  val tableName = "products"
    
  def sqlForeachHandler (row : Row) = {
    println("ID: " + row(0) + ", DisplayName: " + row(1))
  }
    
  def mapper (item : (Object, BSONObject)) : AppProduct = {
    emptyObj.mapper(item).asInstanceOf[AppProduct]
  }  
  
  def mapper (line : String) : AppProduct = {
    emptyObj.mapper(line).asInstanceOf[AppProduct]
  }
  
}