package com.servicesource.spark.ml

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.sql._
import org.apache.hadoop.conf.Configuration
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.classification._
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.mllib.optimization.{ SquaredL2Updater, L1Updater }

import org.bson.BSONObject
import org.bson.BasicBSONObject

import com.servicesource.spark.model._
import com.servicesource.spark.Settings

object OffersClassification {

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("Offers Classification")
    val sc = new SparkContext(conf)
    //    val sc = new SparkContext("local", "Testing program")
    //    val config = new Configuration()
    //    config.set("mongo.input.uri", Settings.getDbConnection(Offer.name))
    //    config.set("mongo.input.query", "{\"result.name\":{$exists:true}}")
    //    val offersRdd = sc.newAPIHadoopRDD(config, classOf[com.mongodb.hadoop.MongoInputFormat],
    //      classOf[Object], classOf[BSONObject])

    val offersRdd = sc.textFile("s3n://ssi-spark/offers_result_ml/export_sample")

    // sql conversion
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext._

    val offers = offersRdd map (Offer.mapper)
    offers.cache
    offers.registerAsTable("offers")

    val scaledResultsByProd = sc.textFile("s3n://ssi-spark/offers_groupby_scale/offers_groupby_product_scale").map({ line =>
      val splits = line.split(",")
      (splits(0), splits(1).toDouble)
    })

    val scaledResultsByProdBC = sc.broadcast(scaledResultsByProd.collectAsMap)

    val scaledResultsByCust = sc.textFile("s3n://ssi-spark/offers_groupby_scale/offers_groupby_customer_scale").map({ line =>
      val splits = line.split(",")
      (splits(0), splits(1).toDouble)
    })

    val scaledResultsByCustBC = sc.broadcast(scaledResultsByCust.collectAsMap)

    val allRecordsWithResult = sqlContext.sql("SELECT result, productId, customerId FROM offers")
    val allPoints = allRecordsWithResult.map { record =>
      {
        if (record.isNullAt(0) || record.isNullAt(1) || record.isNullAt(2)) {
          LabeledPoint(0, Vectors.dense(0.0, 0.0))
        } else {
          val label = if ("win".equals(record.getString(0))) 1 else 0
          val productId = record.getString(1)
          val custId = record.getString(2)
          val prodVal = scaledResultsByProdBC.value.filterKeys(_.equals(productId)).get(productId).getOrElse(0.0)
          val custVal = scaledResultsByCustBC.value.filterKeys(_.equals(custId)).get(custId).getOrElse(0.0)
          LabeledPoint(label, Vectors.dense(prodVal, custVal))
        }
      }
    }
    allPoints.cache
    //    allPoints.foreach { row => println("row: " + row) }
    val splits = allPoints.randomSplit(Array(0.8, 0.2))
    val training = splits(0).cache()
    val test = splits(1).cache()

    val numTraining = training.count()
    val numTest = test.count()
    println(s"Training: $numTraining, test: $numTest.")
    allPoints.unpersist(blocking = false)

    val algorithm = new LogisticRegressionWithSGD()
    algorithm.optimizer
      .setNumIterations(100)
      .setUpdater(new L1Updater())
      .setRegParam(0.1)
    val model = algorithm.run(training).clearThreshold()

    val prediction = model.predict(test.map(_.features))
    val predictionAndLabel = prediction.zip(test.map(_.label))
    val metrics = new BinaryClassificationMetrics(predictionAndLabel)
    println(s"Test areaUnderPR = ${metrics.areaUnderPR()}.")
    println(s"Test areaUnderROC = ${metrics.areaUnderROC()}.")
    sc.stop()
  }

}