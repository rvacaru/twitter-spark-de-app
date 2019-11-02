package com.raz.gddtwitter.controller

import org.springframework.web.bind.annotation.{GetMapping, RestController}

/**
 * HealthcheckController exposes a simple healthcheck Api to be used by external systems.
 *
 * The status of the microservice can be monitored via this Api to recognize when the microservice has started
 * or when the microservice has crashed. It can be used to failover to another healthy instance of the service in case
 * the healthcheck fails.
 */
@RestController
class HealthCheckController {

  @GetMapping(Array("/healthcheck"))
  def healthCheck(): Int = {
    0
  }

}
