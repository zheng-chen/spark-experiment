package com.servicesource.spark

import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory

object Settings {
  
  private val conf = ConfigFactory.load()
  
  def get (key: String) : String = {
    conf.getString(key)
  }
  
  def getInt (key: String) : Int = {
    conf.getInt(key)
  }
  
  def getDbConnection (collection: String) = {
    val conn = "mongodb://"+get("dbHost")+":"+ get("dbPort") +"/"+ get("dbName") 
//    val conn = "mongodb://127.0.0.1:27017/testdata"
    if (collection!=null){
      conn + "." + collection
    } else {
      conn
    } 
  } 
  
}