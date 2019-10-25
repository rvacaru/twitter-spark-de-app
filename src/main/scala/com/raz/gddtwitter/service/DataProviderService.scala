package com.raz.gddtwitter.service

import com.raz.gddtwitter.config.properties.AppProperties
import javax.annotation.PostConstruct
import org.apache.spark.sql.SparkSession
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service

@Service
class DataProviderService @Autowired()(private val sparkSession: SparkSession,
                                       private val appProperties: AppProperties) {

  @PostConstruct
  def init() = {
    println(appProperties.topicsHdfsPath)
  }

}
