package com.raz.gddtwitter.controller

import org.springframework.web.bind.annotation.{GetMapping, RestController}

@RestController
class HealthCheckController {

  @GetMapping(Array("/healthcheck"))
  def healthCheck(): Int = {
    0
  }

}
