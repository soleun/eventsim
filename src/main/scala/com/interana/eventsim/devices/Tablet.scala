package com.interana.eventsim.devices

import com.interana.eventsim.WeightedRandomThingGenerator
import com.interana.eventsim.buildin.{RandomStringGenerator, RandomOSGenerator}
import com.interana.eventsim.Extractable

class Tablet extends Device (dType = "Tablet") {
  private val randomCarrierGenerator = new WeightedRandomThingGenerator[String]()
  randomCarrierGenerator.add("Verizon", 100)
  randomCarrierGenerator.add("AT&T", 100)
  randomCarrierGenerator.add("T-Mobile", 100)
  randomCarrierGenerator.add("Sprint", 100)
  
  val os:(String, String) = RandomOSGenerator.generators.get("mobile").get.randomThing
  
  attributes.put("osName", os._1)
  attributes.put("osVersion", os._2)
  attributes.put("carrier", randomCarrierGenerator.randomThing)
  
//  supportedChannels.append("Email")
//  supportedChannels.append("Social")
  supportedChannels.append("Website")
  supportedChannels.append("MobileApp")
//  supportedChannels.append("SMS")
//  supportedChannels.append("DisplayAd")
//  supportedChannels.append("SearchEngine")
  
  override def toString = {
    "type: "+dType+", deviceId: "+deviceId+", carrier: "+attributes.get("carrier")+", language: "+language+", osName: "+attributes.get("osName")+", osVersion: "+attributes.get("osVersion")+", supportedChannels: "+supportedChannels
  }
}