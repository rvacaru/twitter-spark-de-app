package com.raz.gddtwitter.domain

import java.sql.Timestamp

case class TrendingTopicsWindow(start: Timestamp, end: Timestamp, topics: Seq[(String, Int)])
