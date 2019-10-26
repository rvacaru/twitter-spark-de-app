package com.raz.gddtwitter.service

import com.raz.gddtwitter.config.properties.AppProperties
import javax.annotation.PostConstruct
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service

@Service
class DataProviderService @Autowired()(private val sparkSession: SparkSession,
                                       private val appProperties: AppProperties) extends Serializable {

  private val CREATED_AT = "created_at"
  private val TEXT = "text"
  private val DATE_FORMAT = "EEE MMM dd HH:mm:ss Z yyyy"
  private val COORDINATES = "{\"coordinates\":"

  @PostConstruct
  def init() = {
    println(appProperties.sampleHdfsPath)
    toCreatedAtAndTextDf(readAndFilterSampleJson())
  }

  def readAndFilterSampleJson(): Dataset[String] = {
    val df: Dataset[String] = sparkSession.read.textFile(appProperties.sampleHdfsPath)

    df.filter(str => str.startsWith(COORDINATES))
  }

  def toCreatedAtAndTextDf(stringDataset: Dataset[String]): DataFrame = {
    val filterDf = sparkSession.read.json(stringDataset)
      .select(col(CREATED_AT), col(TEXT))
      .where(col(CREATED_AT).isNotNull && col(CREATED_AT) =!= "" &&
        col(TEXT).isNotNull && col(TEXT) =!= "")
      .withColumn(CREATED_AT, to_timestamp(col(CREATED_AT), DATE_FORMAT))

    filterDf
  }





}
