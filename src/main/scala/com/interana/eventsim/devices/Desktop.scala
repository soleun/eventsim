package com.interana.eventsim.devices

import com.interana.eventsim.WeightedRandomThingGenerator
import com.interana.eventsim.buildin.{RandomStringGenerator, RandomOSGenerator}
import com.interana.eventsim.Extractable

class Desktop extends Device (dType = "Desktop") {
  val os:(String, String) = RandomOSGenerator.generators.get("desktop").get.randomThing
  
  attributes.put("osName", os._1)
  attributes.put("osVersion", os._2)
  
  //supportedChannels.append("Email")
  //supportedChannels.append("Social")
  supportedChannels.append("Website")
  //supportedChannels.append("DisplayAd")
  //supportedChannels.append("SearchEngine")
  
  override def toString = {
    "type: "+dType+", deviceId: "+deviceId+", osName: "+attributes.get("osName")+", osVersion: "+attributes.get("osVersion")+", supportedChannels: "+supportedChannels
  }
}