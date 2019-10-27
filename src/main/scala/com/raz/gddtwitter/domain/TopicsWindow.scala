package com.raz.gddtwitter.domain

import java.sql.Timestamp

case class TopicsWindow(start: Timestamp, end: Timestamp, topics: List[String])
