package com.servicesource.spark.job

import collection.JavaConversions
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.hadoop.conf.Configuration
import org.bson.BSONObject
import org.bson.BasicBSONObject
import com.servicesource.spark._
import com.servicesource.spark.util._

object FindMissingLinks {
  
  val mainSc = new SparkContext("local", "FindMissingLinks")
    
  val oppConfig = new Configuration()
  oppConfig.set("mongo.input.uri", Settings.getDbConnection("app.opportunities"))
//  oppConfig.set("mongo.input.query", "{\"_id\":{\"$oid\":\"51a67d8a72338ce8f001f202\"}}")
  
  val contactsConfig = new Configuration()
  contactsConfig.set("mongo.input.uri", Settings.getDbConnection("core.contacts"))
  
  val rRdd = mainSc.newAPIHadoopRDD(contactsConfig, classOf[com.mongodb.hadoop.MongoInputFormat], classOf[Object], classOf[BSONObject])
  
  val oppRdd = mainSc.newAPIHadoopRDD(oppConfig, classOf[com.mongodb.hadoop.MongoInputFormat], classOf[Object], classOf[BSONObject])

  def extractIds(obj: BSONObject) = {
    obj.get("relationships") match {
      case rels: BSONObject => {
        val relMap: collection.mutable.HashMap[String, List[String]] = collection.mutable.HashMap.empty
        for (key <- JavaConversions.asScalaSet(rels.keySet())) {
          CollectionUtil.getKeyFromRelation(rels.get(key), relMap)
        }
//        println(relMap)
        val ret = relMap flatMap {
          case (coll, ids) => {
            if (coll.equals("core.contact")) {
              ids.map({ (_, obj.get("_id")) })
            } else {
              List()
            }
          }
        }
//        println (ret)
        ret
      }
    }
  }

  def run () {  
    
    val oppMap = oppRdd flatMap ( { item => extractIds (item._2) })
	
	val contactMap = rRdd map ( { item => (item._2.get("_id").toString(), item._2.get("displayName").toString())} )
	
	val joinRdd = oppMap.leftOuterJoin(contactMap).flatMap {
      case (relKey, (recId, relObj)) => {
//        println (relKey+"/"+recId+"/"+relObj)
        relObj match {
          case None => List((relKey, recId))
          case _ => Nil
        }
      }
    }
	
	val result = joinRdd.reduce((a, b) => {
	  // TODO: persist 
	  a
	});
	println(result)
  }
}