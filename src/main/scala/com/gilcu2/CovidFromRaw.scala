package com.gilcu2

import com.gilcu2.interfaces.Time._
import com.github.nscala_time.time
import com.github.nscala_time.time.Imports._
import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

object CovidFromRaw extends LazyLogging {

  def main(implicit args: Array[String]): Unit = {

    val beginTime = beginMain(args)

    implicit val spark: SparkSession = createSpark

    val raw=readCSVRawResults("data/query-emergencias_20200413_19 - Raw.csv")

    val rawKO=raw.filter(col("Estado")==="KO").withColumn("KO",col("Count"))

    rawKO.show

    val rawOK=raw.filter(col("Estado")==="OK").withColumn("OK",col("Count"))

    rawOK.show



    endMain(beginTime)
  }

  private def readCSVRawResults(fileName: String)(implicit spark: SparkSession) = {
    val lines = spark.read.textFile(fileName)

    import org.apache.spark.sql.types._

    val customSchema = StructType(Array(
      StructField("Fecha", StringType, true),
      StructField("Provincia", StringType, true),
      StructField("Destino", StringType, true),
      StructField("Estado", StringType, true),
      StructField("Count", IntegerType, true),
      StructField("Comunidad", StringType, true)

    ))

    val df = spark.read
      .option("delimiter", ",")
      .schema(customSchema)
      .csv(lines)
    df
  }


  private def createSpark = {
    implicit val conf = ConfigFactory.load
    val appName = conf.getString("app")
    val sparkConf = new SparkConf().setAppName(appName)
    implicit val spark = SparkSession
      .builder()
      .config(sparkConf)
      .getOrCreate()
    spark
  }

  private def endMain(beginTime: time.Imports.DateTime) = {
    val endTime = getCurrentTime
    val humanTime = getHumanDuration(beginTime, endTime)
    logger.info(s"End: $endTime Total: $humanTime")
    println(s"End: $endTime Total: $humanTime")
  }

  private def beginMain(args: Array[String]): DateTime = {
    val beginTime = getCurrentTime
    logger.info(s"Begin: $beginTime")
    logger.info(s"Arguments: $args")
    beginTime
  }
}
