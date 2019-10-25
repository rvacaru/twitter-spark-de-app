package com.raz.gddtwitter.config

import org.springframework.context.annotation.{Configuration, Import}

@Configuration
@Import(Array(
  classOf[WebScalaJacksonConfig],
  classOf[SparkConfig]
))
class AppConfig {

}
