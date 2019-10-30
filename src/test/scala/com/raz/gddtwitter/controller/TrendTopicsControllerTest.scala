package com.raz.gddtwitter.controller

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.raz.gddtwitter.domain.TrendingTopicsWindowApi
import com.raz.gddtwitter.service.TrendTopicsService
import org.hamcrest.Matchers._
import org.junit.runner.RunWith
import org.junit.{Before, Test}
import org.mockito.Mockito.when
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest
import org.springframework.boot.test.mock.mockito.MockBean
import org.springframework.test.context.junit4.SpringRunner
import org.springframework.test.web.servlet.MockMvc
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get
import org.springframework.test.web.servlet.result.MockMvcResultMatchers.{content, jsonPath, status}
import org.springframework.web.util.NestedServletException

@RunWith(classOf[SpringRunner])
@WebMvcTest(Array(classOf[TrendTopicsController]))
class TrendTopicsControllerTest {

  private val BASE_URL = "/api/trending_topics"
  private val NO_TOPICS = "noTopics"
  private val WINDOW_PHRASE = "windowPhrase"

  @MockBean
  private val trendTopicsService: TrendTopicsService = null

  @Autowired
  private var mockMvc: MockMvc = _
  private var mapper: ObjectMapper = _

  @Before
  def setup(): Unit = {
    mapper = new ObjectMapper
    mapper.registerModule(new DefaultScalaModule)

    when(trendTopicsService.getTopTrendingTopicsPerWindowAsSet(5, "1 day"))
      .thenReturn(getMockTrendingTopicsList())
  }

  @Test
  def testGetTopTrendingTopicsWithDefaultParamsExpectOk(): Unit = {
    mockMvc.perform(get(BASE_URL))
      .andExpect(status().isOk())
      .andExpect(jsonPath("$[0].start", is("start-time1")))
      .andExpect(content().json(mapper.writeValueAsString(getMockTrendingTopicsList())))
  }

  @Test
  def testGetTopTrendingTopicsWithNoTopicsParamMissingExpectOk(): Unit = {
    mockMvc.perform(get(BASE_URL)
      .param(WINDOW_PHRASE, "1 day"))
      .andExpect(status().isOk())
      .andExpect(jsonPath("$[1].end", is("end-time2")))
      .andExpect(content().json(mapper.writeValueAsString(getMockTrendingTopicsList())))
  }

  @Test
  def testGetTopTrendingTopicsWithWindowPhraseParamMissingExpectOk(): Unit = {
    mockMvc.perform(get(BASE_URL)
      .param(NO_TOPICS, "5"))
      .andExpect(status().isOk())
      .andExpect(jsonPath("$[0].topics", hasSize(3)))
      .andExpect(content().json(mapper.writeValueAsString(getMockTrendingTopicsList())))
  }

  @Test
  def testGetTopTrendingTopicsExpectOk(): Unit = {
    mockMvc.perform(get(BASE_URL)
      .param(NO_TOPICS, "5")
      .param(WINDOW_PHRASE, "1 day"))
      .andExpect(status().isOk())
      .andExpect(jsonPath("$[0].start", is("start-time1")))
      .andExpect(content().json(mapper.writeValueAsString(getMockTrendingTopicsList())))
  }

  @Test
  def testGetTopTrendingTopicsWithNoTopicsNaNExpectBadRequest(): Unit = {
    mockMvc.perform(get(BASE_URL)
      .param(NO_TOPICS, "NaN"))
      .andExpect(status().isBadRequest())
  }

  @Test(expected = classOf[NestedServletException])
  def testGetTopTrendingTopicsWithNegativeNoTopicsExpectNestedServletException(): Unit = {
    mockMvc.perform(get(BASE_URL)
      .param(NO_TOPICS, "-3"))
  }

  @Test(expected = classOf[NestedServletException])
  def testGetTopTrendingTopicsWithInvalidWindowPhraseExpectOk(): Unit = {
    val invalidPhrase: String = "1 invalid phrase"
    when(trendTopicsService.getTopTrendingTopicsPerWindowAsSet(5, invalidPhrase))
        .thenThrow(new IllegalArgumentException)

    mockMvc.perform(get(BASE_URL)
      .param(WINDOW_PHRASE, invalidPhrase))
  }

  private def getMockTrendingTopicsList(): Set[TrendingTopicsWindowApi] = {
    Set(TrendingTopicsWindowApi("start-time1", "end-time1", Seq(("top1", 9), ("top2", 6), ("top3", 1))),
      TrendingTopicsWindowApi("start-time2", "end-time2", Seq(("topic1", 8), ("topic2", 4), ("topic3", 2))))
  }

}
