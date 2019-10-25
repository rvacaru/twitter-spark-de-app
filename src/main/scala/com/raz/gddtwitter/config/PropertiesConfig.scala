package com.raz.gddtwitter.config

import com.raz.gddtwitter.config.properties.AppProperties
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.{Bean, Configuration}

@Configuration
class PropertiesConfig {

  @Value("${topics.hdfs.path:hdfs:///default/path/sample.json}") private val topicsHdfsPath: String = null

  @Bean
  def appProperties: AppProperties = {
    new AppProperties(topicsHdfsPath)
  }

}
