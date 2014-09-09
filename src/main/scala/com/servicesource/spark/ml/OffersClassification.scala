package com.servicesource.spark.ml

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql._
import org.apache.hadoop.conf.Configuration
import org.bson.BSONObject
import org.bson.BasicBSONObject

import com.servicesource.spark.model._
import com.servicesource.spark.Settings

object OffersClassification {

  def main(args: Array[String]) {
     val sc = new SparkContext("local", "Testing program")
     val config = new Configuration()
     config.set("mongo.input.uri", Settings.getDbConnection(Offer.name))
     config.set("mongo.input.query", "{\"result.name\":{$exists:true}}")
     val offersRdd = sc.newAPIHadoopRDD(config, classOf[com.mongodb.hadoop.MongoInputFormat], 
         classOf[Object], classOf[BSONObject])
         
     // sql conversion
     val sqlContext = new org.apache.spark.sql.SQLContext(sc)
     import sqlContext._
     
     val offers = offersRdd map ( Offer.mapper )
     offers.cache
     offers.registerAsTable("offers")
     
//     val groupbyProduct = sqlContext.sql("SELECT productDisp, result, count(id) FROM offers group by productDisp, result")
//     groupbyProduct.foreach {row => println("row: "+row)}
     
     val groupbyCust = sqlContext.sql("SELECT customerId, result, count(id) FROM offers group by customerId, result")
//     groupbyCust.foreach {row => println("row: "+row)}
     val scaledResults = groupbyCust.groupBy {_(0)} map { group => {
       group._2 match {
         case s:Seq[Row] => {(group._1,  scaleResult(s) )}
       }
     }
     }
     scaledResults.foreach {row => println("row: "+row)}
   }
  
  def scaleResult (results : Seq[Row]) : Double = {
    var winCount = 0L;
    var totalCount = 0L;
    
    for (res <- results) {
      val r = res.getString(1)
      val count = res.getLong(2)
      if (r.equals("win")) {
        winCount = count;
      }
      totalCount = totalCount + count
    }
    if (totalCount ==0) {
      0
    } else if (winCount!=totalCount) {
      winCount/totalCount.toDouble
    } else { // All wins
      1
    }        
  }
}