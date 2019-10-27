package com.raz.gddtwitter.domain

case class TrendingTopicsWindowApi(start: String, end: String, topics: Seq[(String, Int)])
