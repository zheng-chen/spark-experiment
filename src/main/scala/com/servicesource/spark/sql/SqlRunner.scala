package com.servicesource.spark.sql

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql._
import org.apache.hadoop.conf.Configuration
import org.bson.BSONObject
import org.bson.BasicBSONObject

import com.servicesource.spark.model._
import com.servicesource.spark.Settings

object SqlRunner {

   def main(args: Array[String]) {
     val sc = new SparkContext("local", "Testing program")
     val config = new Configuration()
     config.set("mongo.input.uri", Settings.getDbConnection(Task.name))
     val tasksRdd = sc.newAPIHadoopRDD(config, classOf[com.mongodb.hadoop.MongoInputFormat], 
         classOf[Object], classOf[BSONObject])
         
     // sql conversion
     val sqlContext = new org.apache.spark.sql.SQLContext(sc)
     import sqlContext._
     
     val task = tasksRdd map ( Task.mapper )
     task.cache
     task.registerAsTable("tasks")
     
     val oneTask = sqlContext.sql("SELECT id, disp FROM tasks WHERE id = \"51a67c7572338ce8f0003ef6\"")
     oneTask.map(t => "ID: " + t(0) + ", DisplayName: " + t(1)).collect().foreach(println)
     
     val multiplTasks = sqlContext.sql("SELECT id, disp FROM tasks WHERE disp LIKE \"%TASK%\"")
     multiplTasks.map(t => "ID: " + t(0) + ", DisplayName: " + t(1)).collect().foreach(println)
     
//     val config = new Configuration()
//     config.set("mongo.input.uri", Settings.getDbConnection(Asset.name))
//     val tasksRdd = sc.newAPIHadoopRDD(config, classOf[com.mongodb.hadoop.MongoInputFormat], 
//         classOf[Object], classOf[BSONObject])
//         
//     // sql conversion
//     val sqlContext = new org.apache.spark.sql.SQLContext(sc)
//     import sqlContext._
//     
//     val task = tasksRdd map ( Asset.mapper )
//     task.cache
//     task.registerAsTable("assets")
//     
//     val oneTask = sqlContext.sql("SELECT id,disp,amount,customer,product FROM assets WHERE id = \"51a67c8b72338ce8f0007f5d\"")
//     oneTask.foreach({r=>println("row: "+r)})
//     
//     val multiplTasks = sqlContext.sql("SELECT sum(amount), count(id), customer, product FROM assets WHERE disp LIKE \"%Remedy%\" "+
//         "group by customer, product")
//     multiplTasks.foreach({r=>println("row: "+r)})
//     
//     val multiplTasks1 = sqlContext.sql("SELECT count(id),product FROM assets WHERE disp LIKE \"%Remedy%\" group by product")
//     multiplTasks1.foreach({r=>println("row: "+r)})
   }
}