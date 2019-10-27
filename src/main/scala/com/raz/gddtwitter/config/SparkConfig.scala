package com.raz.gddtwitter.config

import com.raz.gddtwitter.config.properties.AppProperties
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.springframework.context.annotation.{Bean, Configuration, Import}

@Configuration
@Import(Array(classOf[PropertiesConfig]))
class SparkConfig {

  @Bean
  def sparkSession(sparkConf: SparkConf): SparkSession = {
    SparkSession
      .builder()
      .config(sparkConf)
      .getOrCreate()
  }

  @Bean
  def sparkContext(sparkSession: SparkSession, appProperties: AppProperties): SparkContext = {
    val sparkCtx = sparkSession.sparkContext
    sparkCtx.hadoopConfiguration
      .set("fs.default.name", appProperties.hdfsName)

    sparkCtx
  }

  @Bean
  def sparkConf(appProperties: AppProperties): SparkConf = {
    new SparkConf()
      .setAppName("Spark Twitter App")
      .set("spark.master", appProperties.sparkMaster)

    //eventually more configuration
  }
}