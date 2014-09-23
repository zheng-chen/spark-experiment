package com.servicesource.spark.sql

import org.apache.spark.SparkContext._
import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.sql._
import org.apache.spark.sql.SchemaRDD
import org.apache.spark.rdd.RDD
import org.apache.hadoop.conf.Configuration
import org.bson.BSONObject
import org.bson.BasicBSONObject

import scala.util.Try

import com.servicesource.spark.model._
import com.servicesource.spark.Settings

import spark.jobserver._

import com.typesafe.config.{ Config, ConfigFactory }

object SqlRunner extends SparkJob with NamedRddSupport {

  def main(args: Array[String]) {
    val sc = new SparkContext("local", "Testing program")
    loadFromMongo(sc)
  }

  private def loadFromMongo(sc:SparkContext): RDD[Row] = {
    val modelObj = Task

    val config = new Configuration()
    config.set("mongo.input.uri", Settings.getDbConnection(modelObj.name))
    config.set("mongo.input.notimeout", "true");
    config.set("mongo.splitter.class", "com.servicesource.spark.mongo.CustomCollectionSplitter")

    if (modelObj.mongoQuery != null) {
      config.set("mongo.input.query", modelObj.mongoQuery)
    }

    val collRdd = sc.newAPIHadoopRDD(config, classOf[com.mongodb.hadoop.MongoInputFormat],
      classOf[Object], classOf[BSONObject])

    val sqlContext: SQLContext = new SQLContext(sc)
  	import sqlContext._
  	
    val mappedColl = collRdd map (Task.mapper)
    mappedColl.cache
    mappedColl.registerAsTable(modelObj.tableName)
    //    for (query <- modelObj.sqlQuery) {
    //      sqlContext.sql(query).foreach(Task.sqlForeachHandler)
    //    }
    mappedColl
  }
  
  private def loadFromS3(sc: SparkContext): (SQLContext, RDD[Row]) = {
    val modelObj = Offer
    val collRdd = sc.textFile("s3n://ssi-spark/offers_result_ml/export_sample")

    val sqlContext: SQLContext = new SQLContext(sc)
  	import sqlContext._
    val mappedColl = collRdd map (Offer.mapper)
    mappedColl.cache
    mappedColl.registerAsTable(modelObj.tableName)
    (sqlContext, mappedColl)
    
  }

  override def validate(sc: SparkContext, config: Config): SparkJobValidation = {
    Try(config.getString("input.sql"))
      .map(x => SparkJobValid)
      .getOrElse(SparkJobInvalid("No input.sql config param"))
  }

  override def runJob(sc: SparkContext, config: Config): Any = {

//    var rdd = this.namedRdds.get[Row]("cachedRdd").getOrElse(null)
//
//    if (rdd == null) {
//      
//      this.namedRdds.update("cachedRdd", rdd)
//    }
    
    val (context, rdd) = loadFromS3(sc)

    val sql = config.getString("input.sql")
    context.sql(sql).collect

  }
}