package com.raz.gddtwitter.service

import com.raz.gddtwitter.config.properties.AppProperties
import com.raz.gddtwitter.service.SchemaConstants._
import javax.annotation.PostConstruct
import org.apache.spark.sql.{Column, DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service

@Service
class DataProviderService @Autowired()(private val sparkSession: SparkSession,
                                       private val appProperties: AppProperties) extends Serializable {

  def getTwitterTextDf: DataFrame = {
    val filteredSampleDataset = readAndFilterSampleJson(appProperties.sampleHdfsPath,
      (str: String) => str.startsWith(COORDINATES_PREFIX))

    val filteredTwitterSampleDataset = readAndFilterSampleJson(appProperties.twitterSampleHdfsPath,
      (str: String) => str.contains(TWITTER_TYPE_PATTERN) && !str.contains(RETWEET_PATTERN))

    toTwitterCreatedAtAndTextDf(filteredTwitterSampleDataset)
      .union(toCreatedAtAndTextDf(filteredSampleDataset))
  }

  private def readAndFilterSampleJson(hdfsPath: String, filterFunction: String => Boolean): Dataset[String] = {
    sparkSession.read
      .textFile(hdfsPath)
      .filter(filterFunction)
  }

  private def toCreatedAtAndTextDf(stringDataset: Dataset[String]): DataFrame = {
    val filterDf = sparkSession.read.json(stringDataset)
      .select(col(CREATED_AT), col(TEXT))
      .where(createdAtAndTextAreNotNullOrEmptyStringsCondition())
      .withColumn(CREATED_AT, asTimestamp(DATE_FORMAT_SAMPLE))

    filterDf
  }

  private def toTwitterCreatedAtAndTextDf(stringDataset: Dataset[String]): DataFrame = {
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