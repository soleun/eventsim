package com.interana.eventsim.buildin

import com.interana.eventsim.WeightedRandomThingGenerator

import scala.io.Source

object RandomEmailGenerator {
  def randomThing = {
    val username = RandomStringGenerator.randomAlphanumericSpaceString(10)
    val domainSelector = new WeightedRandomThingGenerator[String]()
    domainSelector.add("gmail.com", 425.0)
    domainSelector.add("hotmail.com", 360.0)
    domainSelector.add("yahoo.com", 200.0)
    
    username+"@"+domainSelector.randomThing
  }
}