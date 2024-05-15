package org.dezyre

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Dataset, Row, SparkSession}

import scala.io.Source

object FitnessTrackerDatajob {
  def main(args: Array[String]) = {
    println("Analytics of Fitness Tracker Data")

    val sparkSession: SparkSession = getSparkSession()
    val rawFitnessData: Dataset[Row] = scanRawFitnessData(sparkSession)
    //rawFitnessData.show( truncate = false)

    val activityFormattedData: Dataset[Row] = formatActivityColumn(rawFitnessData)
    //activityFormattedData.show( truncate = false)

    activityFormattedData.printSchema()

    val timeStampFormattedData: Dataset[Row] = formatTheTimeStampColumn(activityFormattedData)
    //timeStampFormattedData.show( truncate = false)

    timeStampFormattedData.printSchema()

    val formattedFitnessData: Dataset[Row] = timeStampFormattedData
    //formattedFitnessData.show(false)

    formattedFitnessData.cache()

    val highestToLowestUsersCalorieBurnt: Dataset[Row] =
      caloriesBurntBestToWorst(formattedFitnessData)

    highestToLowestUsersCalorieBurnt.show( truncate = false)

    val famalesBestToWorstActivity: Dataset[Row] =
      activityUsedBestToWorstAmongFemales(formattedFitnessData)

    famalesBestToWorstActivity.show( truncate = false)

    val famalesBestToWorstActivity2: Dataset[Row] =
      activityUsedBestToWorstAmongFemalesSQLStyle(sparkSession, formattedFitnessData)

    famalesBestToWorstActivity2.show( truncate = false)

    //Thread.sleep(1000000)
    sparkSession.stop()
  }

  def viewData(dataset: Dataset[Row]) = {
    dataset.show( truncate = false)
  }

  def getSparkSession(): SparkSession = {
    SparkSession.
      builder().
      appName( name = "Testing Spark Application").
      config("spark.sql.legacy.timeParserPolicy", "LEGACY").
      master( master = "local").getOrCreate();

  }

  def scanRawFitnessData(sparkSession: SparkSession): Dataset[Row] = {
    sparkSession.
      read.format( source = "csv").
      option("header", true).
      option("inferSchema", true).
      csv(System.getenv("path")+ "/Fitness_tracker_data.csv")
  }

  def formatActivityColumn(dataset: Dataset[Row]): Dataset[Row] = {
    dataset.withColumnRenamed( existingName = "activity",
        newName = "activity_raw").
      withColumn( colName = "activity",
        regexp_replace(col( colName = "activity_raw"), pattern = "_", replacement = "")).
      select(dataset.columns.head,
        dataset.columns.tail: _*)
  }

  def formatTheTimeStampColumn(dataset: Dataset[Row]): Dataset[Row] = {
    dataset.withColumnRenamed( existingName = "time_stamp", newName = "time_stamp_raw").
      withColumn(colName = "time_stamp",
        to_timestamp(col( colName = "time_stamp_raw"), fmt = "dd:MM:yy HH:mm")).
      select(dataset.columns.head, dataset.columns.tail: _*)
  }

  def caloriesBurntBestToWorst(dataset: Dataset[Row]): Dataset[Row] = {
    dataset.groupBy( col1 = "user_id").
      agg(sum( columnName = "calories").as( alias = "calories")).
      orderBy(col( colName = "calories").desc)
  }

  def activityUsedBestToWorstAmongFemales(dataset: Dataset[Row]): Dataset[Row] = {
    dataset.filter( conditionExpr = "gender == 'F' ").
      groupBy( col1 = "activity").
      agg(approx_count_distinct(col( colName = "user_id")).as( alias = "count_of_users")).
      orderBy(col( colName = "count_of_users").desc)


  }

  def activityUsedBestToWorstAmongFemalesSQLStyle(sparkSession: SparkSession, dataset: Dataset[Row]): Dataset[Row] = {
    dataset.createOrReplaceTempView("data")

    sparkSession.sql(
      """select
        |activity,
        |count(distinct user_id) as count_of_users
        |from data where gender = 'F'
        |group by activity
        |order by count_of_users
        |desc
        |""".stripMargin)
  }


}
