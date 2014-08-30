package com.servicesource.spark.util

import collection.JavaConversions
import collection.mutable
import org.bson.BSONObject
import org.bson.BasicBSONObject
import org.bson.types.BasicBSONList

import play.api.libs.json._
import play.api.libs.functional.syntax._

import com.mongodb.casbah.Imports._

import com.servicesource.spark.Settings

object CollectionUtil {
  
  val mongoClient = MongoClient(Settings.get("dbHost"), Settings.getInt("dbPort"))
  val db = mongoClient(Settings.get("dbName"))

  def getKeyFromRelation(relation: AnyRef,
    result: mutable.HashMap[String, List[String]] = mutable.HashMap.empty) = {
    relation match {
      case rel: BSONObject => {
        rel.get("targets") match {
          case targets: BasicBSONList => {
            for (index <- JavaConversions.asScalaSet(targets.keySet())) {
              val target = targets.get(index).asInstanceOf[BSONObject]
              val targetType = target.get("type")
              val targetKey = target.get("key")
              if (targetType != null && targetKey != null) {
                val key = targetType.toString().split("/")(0)
                if (result.contains(key)) {
                  result(key) = result(key) :+ targetKey.toString()
                } else {
                  result(key) = List(targetKey.toString())
                }
              }
            }
          }
        }
      }
    }
    result
  }

}

