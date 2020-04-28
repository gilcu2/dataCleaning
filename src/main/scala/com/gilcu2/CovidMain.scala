package com.gilcu2

import com.gilcu2.interfaces.Time._
import com.github.nscala_time.time
import com.github.nscala_time.time.Imports._
import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._

object CovidMain extends LazyLogging {

  def main(implicit args: Array[String]): Unit = {

    val beginTime = beginMain(args)

    implicit val spark: SparkSession = createSpark

    val prefijosProvinciaComunidad = readCSVPrefijosProvincias("data/prefijos_provincias.csv")
    val normalizedProvince = normalizeProvincia(prefijosProvinciaComunidad)
    val provinciaComunidad=normalizedProvince.select("Provincia","Comunidad").drop().distinct()

//    provinciaComunidad.coalesce(1).write.format("csv")
//      .option("header",true)
//      .mode("overwrite").option("sep",",").save("data/provincia_comunidad.csv")

    provinciaComunidad.orderBy("Provincia").show(50)
    println(provinciaComunidad.count())

    val llamadasMovel900=readCSVLlamadas("data/query-900_provincias_20200420_26.csv").drop()
    llamadasMovel900.show()
    println(s"Llamadas: ${llamadasMovel900.count()}")

    // Check provincias
//    val provinciasDeLlamadas=llamadasMovel900.select("Provincia").distinct()
//    provinciasDeLlamadas.show()
//    val join=provinciasDeLlamadas.join(provinciaComu,
//      provinciasDeLlamadas("Provincia")===provinciaComu("Provincia"),"left_outer")



    val join=llamadasMovel900.join(provinciaComunidad,usingColumn = "Provincia")

    join.show(60)
    println(s"Join: ${join.count()}")

    val aggregate=join.groupBy("Comunidad").sum("Intentos","Completadas","Rechazadas")
    aggregate.show()

    endMain(beginTime)
  }

  private def readCSVPrefijosProvincias(fileName: String)(implicit spark: SparkSession) = {
    val lines = spark.read.textFile(fileName)

    import org.apache.spark.sql.types._

    val customSchema = StructType(Array(
      StructField("Prefijo", StringType, true),
      StructField("Provincia", StringType, true),
      StructField("Comunidad", StringType, true)
    ))

    val df = spark.read
      .option("delimiter", "|")
      .schema(customSchema)
      .csv(lines)
//    df.orderBy("Provincia")
    df
  }

  def normalizeProvincia(prefijos: DataFrame)(
    implicit spark: SparkSession): DataFrame = {

    prefijos.withColumn("Provincia",upper(col("Provincia")))
  }

  private def readCSVLlamadas(fileName: String)(implicit spark: SparkSession) = {
    val lines = spark.read.textFile(fileName)

    import org.apache.spark.sql.types._

    val customSchema = StructType(Array(
      StructField("Provincia", StringType, true),
      StructField("Destino", StringType, false),
      StructField("Intentos", IntegerType, false),
      StructField("Completadas", IntegerType, false),
      StructField("Rechazadas", IntegerType, false)
    ))

    val df = spark.read
      .option("delimiter", ",")
      .option("header","true")
      .schema(customSchema)
      .csv(lines)
    //    df.orderBy("Provincia")
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
