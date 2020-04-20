package com.gilcu2

import com.typesafe.scalalogging.LazyLogging
import com.gilcu2.interfaces.Time._
import com.github.nscala_time.time
import com.github.nscala_time.time.Imports._
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

object DataCleaningMain extends LazyLogging {

  def main(implicit args: Array[String]): Unit = {

    val beginTime = beginMain(args)

    implicit val spark: SparkSession = createSpark

    val df: DataFrame = readCSV(args)

    df.show()

    endMain(beginTime)
  }

  private def readCSV(args: Array[String])(implicit spark: SparkSession) = {
    val lines = spark.read.textFile(args(0))

    import org.apache.spark.sql.types._

    val customSchema = StructType(Array(
      StructField("LongCode", StringType, true),
      StructField("ShortCode", StringType, true))
    )

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

  private def beginMain(args: Array[String]):DateTime = {
    val beginTime = getCurrentTime
    logger.info(s"Begin: $beginTime")
    logger.info(s"Arguments: $args")
    beginTime
  }
}
