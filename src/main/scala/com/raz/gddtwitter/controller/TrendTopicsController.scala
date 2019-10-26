package com.raz.gddtwitter.controller

import com.raz.gddtwitter.service.TrendTopicsService
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.web.bind.annotation.{GetMapping, RequestMapping, RequestParam, RestController}

@RestController
@RequestMapping(Array("/api"))
class TrendTopicsController @Autowired()(private val trendTopicsService: TrendTopicsService) {

  @GetMapping(Array("/trending_topics"))
  def getTopTrendingTopics(@RequestParam(value = "noTopics", defaultValue = "5") noTopics: Int,
                           @RequestParam(value = "window", defaultValue = "500") minutesWindow: Long): Long = {

    42L
  }

}
