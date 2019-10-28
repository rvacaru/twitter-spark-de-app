package com.raz.gddtwitter.config

import org.springframework.context.annotation.{Configuration, Import}

@Configuration
@Import(Array(
  classOf[PropertiesConfig],
  classOf[WebConfig],
  classOf[SparkConfig]
))
class AppConfig {

}
