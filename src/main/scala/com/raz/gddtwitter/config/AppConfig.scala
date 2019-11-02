package com.raz.gddtwitter.config

import org.springframework.context.annotation.{Configuration, Import}

/**
 * Spring configuration of the whole application.
 * It's a configuration container aggregating the other configurations:
 *
 * - Application properties configuration
 * - Web spring configuration for the rest api
 * - Spark configuration for the top trending topics spark job
 */
@Configuration
@Import(Array(
  classOf[PropertiesConfig],
  classOf[WebConfig],
  classOf[SparkConfig]
))
class AppConfig {

}
