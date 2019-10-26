package com.raz.gddtwitter.service

import com.raz.gddtwitter.config.properties.AppProperties
import javax.annotation.PostConstruct
import org.apache.spark.sql.{Column, DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service

@Service
class DataProviderService @Autowired()(private val sparkSession: SparkSession,
                                       private val appProperties: AppProperties) extends Serializable {

  private val CREATED_AT = "created_at"
  private val TEXT = "text"
  private val TWITTER_CREATED_AT = "twitter." + CREATED_AT
  private val TWITTER_TEXT = "twitter." + TEXT
  private val DATE_FORMAT_SAMPLE = "EEE MMM dd HH:mm:ss Z yyyy"
  private val DATE_FORMAT_TWITTER_SAMPLE ="EEE, dd MMM yyyy HH:mm:ss Z"
  private val COORDINATES_PREFIX = "{\"coordinates\":"
  private val TWITTER_TYPE_PATTERN = "\"type\":\"twitter\""
  private val RETWEET_PATTERN = "\"retweet\":{"

  @PostConstruct
  def init() = {
    println(appProperties.sampleHdfsPath)
    val dsSample = readAndFilterSampleJson(appProperties.sampleHdfsPath,
      (str: String) => str.startsWith(COORDINATES_PREFIX))

    val dsTwitterSample = readAndFilterSampleJson(appProperties.twitterSampleHdfsPath,
      (str: String) => str.contains(TWITTER_TYPE_PATTERN) && !str.contains(RETWEET_PATTERN))

    toTwitterCreatedAtAndTextDf(dsTwitterSample)
  }

  def readAndFilterSampleJson(hdfsPath: String, filterFunction: String => Boolean): Dataset[String] = {
    sparkSession.read
      .textFile(hdfsPath)
      .filter(filterFunction)
  }

  def toCreatedAtAndTextDf(stringDataset: Dataset[String]): DataFrame = {
    val filterDf = sparkSession.read.json(stringDataset)
      .select(col(CREATED_AT), col(TEXT))
      .where(createdAtAndTextAreNotNullOrEmptyStringsCondition())
      .withColumn(CREATED_AT, asTimestamp(DATE_FORMAT_SAMPLE))

    filterDf
  }

  def toTwitterCreatedAtAndTextDf(stringDataset: Dataset[String]): DataFrame = {
    val filterDf = sparkSession.read.json(stringDataset)
      .select(col(TWITTER_CREATED_AT), col(TWITTER_TEXT))
      .where(createdAtAndTextAreNotNullOrEmptyStringsCondition())
      .withColumn(CREATED_AT, asTimestamp(DATE_FORMAT_TWITTER_SAMPLE))

    filterDf
  }

  private def asTimestamp(dateFormat: String):Column = {
    to_timestamp(col(CREATED_AT), dateFormat)
  }

  private def createdAtAndTextAreNotNullOrEmptyStringsCondition(): Column = {
    col(CREATED_AT).isNotNull && col(CREATED_AT) =!= "" && col(TEXT).isNotNull && col(TEXT) =!= ""
  }

}
