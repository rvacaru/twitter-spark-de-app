package com.raz.gddtwitter.config

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.springframework.context.annotation.{Bean, Configuration}
import org.springframework.http.converter.HttpMessageConverter
import org.springframework.http.converter.json.MappingJackson2HttpMessageConverter
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer

@Configuration
class WebScalaJacksonConfig extends WebMvcConfigurer {

  override def configureMessageConverters(converters: java.util.List[HttpMessageConverter[_]]): Unit = {
    def mapper: ObjectMapper = createObjectMapper
    def converter: MappingJackson2HttpMessageConverter = createJacksonHttpMessageConverter

    mapper.registerModule(DefaultScalaModule)
    converter.setObjectMapper(mapper)

    converters.add(converter)
  }

  @Bean
  def createJacksonHttpMessageConverter: MappingJackson2HttpMessageConverter = {
    new MappingJackson2HttpMessageConverter
  }

  @Bean
  def createObjectMapper: ObjectMapper = {
    new ObjectMapper
  }
}
