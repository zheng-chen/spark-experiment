package com.servicesource.spark

import org.scalatest.FunSuite
import org.scalatest.matchers.ShouldMatchers
import org.scalatest.junit.JUnitRunner
import org.scalatest.BeforeAndAfter

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.hadoop.conf.Configuration

import org.junit.runner.RunWith

import org.bson.BSONObject
import org.bson.BasicBSONObject

@RunWith(classOf[JUnitRunner])
class BasicIntegrationSuite extends FunSuite with BeforeAndAfter {
  
  val sc = new SparkContext("local", "Basic Integration Suite")
  val config = new Configuration()

  val rdd = sc.newAPIHadoopFile("src/test/resources/data/app.tasks.bson",
      classOf[com.mongodb.hadoop.BSONFileInputFormat].asSubclass(
          classOf[org.apache.hadoop.mapreduce.lib.input.FileInputFormat[Object, BSONObject]]), 
          classOf[Object], classOf[BSONObject], config)
    
  
  test("simple count") {
    assert(rdd.count()==127);
  }

}