package com.servicesource.spark.job

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.hadoop.conf.Configuration
import org.bson.BSONObject
import org.bson.BasicBSONObject

import com.servicesource.spark._
import com.servicesource.spark.model._

object Transformer {

  def run () {
    val conf = new SparkConf().setAppName("Data Transformation")
    val sc = new SparkContext(conf)
//    val sc = new SparkContext("local", "Testing program")
    
    val typeObject  =  Quote // Opportunity //Booking
    val collectionConfig = new Configuration()
    collectionConfig.set("mongo.input.uri", Settings.getDbConnection(typeObject.name))
//    collectionConfig.set("mongo.input.split.use_range_queries", "true")
    
    val collRdd = sc.newAPIHadoopRDD(collectionConfig, classOf[com.mongodb.hadoop.MongoInputFormat], 
         classOf[Object], classOf[BSONObject])
         
    val transformed = collRdd map { Quote.mapper }
    
    transformed.foreach(println)
    
//    transformed.saveAsTextFile("hdfs:///transform."+typeObject.name)
  }
}