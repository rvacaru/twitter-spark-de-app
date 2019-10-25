package com.raz.gddtwitter.controller

import com.raz.gddtwitter.service.TrendTopicsService
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.web.bind.annotation.{GetMapping, RequestMapping, RequestParam, RestController}

@RestController
@RequestMapping(Array("/trendTopics"))
class TrendTopicsController @Autowired()(private val trendTopicsService: TrendTopicsService) {

  @GetMapping(Array("/topTrendingTopics"))
  def getTopTrendingTopics(@RequestParam("noTopics") noTopics: Int, @RequestParam("window") minutesWindow: Long): Long = {
    42L
  }

}
