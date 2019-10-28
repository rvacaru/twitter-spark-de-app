package com.raz.gddtwitter.controller

import java.util

import com.raz.gddtwitter.domain.TrendingTopicsWindowApi
import com.raz.gddtwitter.service.TrendTopicsService
import javax.validation.constraints.{Min, NotBlank}
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.validation.annotation.Validated
import org.springframework.web.bind.annotation.{GetMapping, RequestMapping, RequestParam, RestController}

@RestController
@Validated
@RequestMapping(Array("/api"))
class TrendTopicsController @Autowired()(private val trendTopicsService: TrendTopicsService) {

  /**
   * Gets the top N trending topics for each time window from the available data
   *
   * @param noTopics number of top trending topics per window, should be greater than 0
   * @param windowPhrase window definition as positive integer followed by second, minute, hour, day or week
   * @return a list of windows with the top N trending topics for each one of them
   */
  @GetMapping(Array("/trending_topics"))
  def getTopTrendingTopics(
    @RequestParam(value = "noTopics", defaultValue = "5") @Min(1) noTopics: Int,
    @RequestParam(value = "minutesWindow", defaultValue = "1 day") @NotBlank windowPhrase: String): util.List[TrendingTopicsWindowApi] = {

    trendTopicsService.getTopTrendingTopicsPerWindowAsList(noTopics, windowPhrase)
  }

}
