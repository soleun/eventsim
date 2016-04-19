package com.interana.eventsim.channels

import com.interana.eventsim.Channel
import com.interana.eventsim.buildin.RandomUserAgentGenerator

class Website extends Channel(name = "Website") {
  val userAgent = RandomUserAgentGenerator.randomThing
  attributes.put("userAgent", userAgent._1.replaceAll("\"", ""))
  attributes.put("browser", userAgent._2.trim())
}