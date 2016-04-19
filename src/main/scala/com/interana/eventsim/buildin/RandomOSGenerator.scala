package com.interana.eventsim.buildin

import com.interana.eventsim.WeightedRandomThingGenerator

import scala.io.Source
import scala.collection.mutable

/**
 * Data originally from https://developers.google.com/ad-exchange/rtb/downloads
 * Mobile OS marketshare from http://www.idc.com/prodserv/smartphone-os-market-share.jsp
 * 
 */
object RandomOSGenerator {
  val generators = mutable.HashMap[String, WeightedRandomThingGenerator[(String, String)]]()
  val mobilePlatforms:mutable.HashMap[String, Double] = mutable.HashMap("iOS" -> 13.9, "Android" -> 82.8, "PalmWebOS" -> 0.05, "BlackBerry" -> 0.3, "WindowsPhone" -> 2.0, "WindowsPhone7" -> 0.4, "MobileAndTablet" -> 0.0)
  val s = Source.fromFile("data/mobile-os.csv","ISO-8859-1")
  val lines = s.getLines().drop(1)
  
  for (l <- lines) {
    val fields = l.split(",")
    val platform:String = fields(1).replaceAll("\"", "")
    val version = fields(2)+"."+fields(3)+"."+fields(4)
    
    if(mobilePlatforms.keySet.contains(platform)) {
      if(!generators.keySet.contains("mobile")) {
        generators.put("mobile", new WeightedRandomThingGenerator[(String, String)])
      }
      generators.get("mobile").get.add((platform, version), mobilePlatforms.get(platform).get)
    } else {
      if(!generators.keySet.contains("desktop")) {
        generators.put("desktop", new WeightedRandomThingGenerator[(String, String)])
      }
      generators.get("desktop").get.add((platform, version), 1.0)
    }
    
  }
  s.close()
}