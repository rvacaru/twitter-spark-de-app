package com.raz.gddtwitter.service

import com.raz.gddtwitter.config.properties.AppProperties
import javax.annotation.PostConstruct
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service

@Service
class DataProviderService @Autowired()(private val sparkSession: SparkSession,
                                       private val appProperties: AppProperties) {

  private val CREATED_AT = "created_at"
  private val TEXT = "text"

  @PostConstruct
  def init() = {
    println(appProperties.sampleHdfsPath)
    readSampleJsonIntoCreatedAtAndTextDf()
  }

  def readSampleJsonIntoCreatedAtAndTextDf() = {
    val df: DataFrame = sparkSession.read.json(appProperties.sampleHdfsPath)

    val filterDf = df
      .select(col(CREATED_AT), col(TEXT))
      .where(col(CREATED_AT).isNotNull && col(CREATED_AT) =!= "" &&
        col(TEXT).isNotNull && col(TEXT) =!= "")

    filterDf
  }

}
