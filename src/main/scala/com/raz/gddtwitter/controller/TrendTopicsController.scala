package com.raz.gddtwitter.controller

import java.util

import com.raz.gddtwitter.domain.TrendingTopicsWindowApi
import com.raz.gddtwitter.service.TrendTopicsService
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.web.bind.annotation.{GetMapping, RequestMapping, RequestParam, RestController}

@RestController
@RequestMapping(Array("/api"))
class TrendTopicsController @Autowired()(private val trendTopicsService: TrendTopicsService) {

  @GetMapping(Array("/trending_topics"))
  def getTopTrendingTopics(@RequestParam(value = "noTopics", defaultValue = "5") noTopics: Int,
                           @RequestParam(value = "minutesWindow", defaultValue = "500") minutesWindow: Long): util.List[TrendingTopicsWindowApi] = {

    trendTopicsService.getTopTrendingTopicsPerWindowAsList(noTopics, minutesWindow)
  }

}
