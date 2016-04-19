package com.interana.eventsim.sessions

import com.pointillist.util.RandomNumberGenerator

class MobileBrowser { 
  var ipaddr:String = List(RandomNumberGenerator.getPositiveRandomNumber(256), RandomNumberGenerator.getPositiveRandomNumber(256), RandomNumberGenerator.getPositiveRandomNumber(256), RandomNumberGenerator.getPositiveRandomNumber(256)).mkString(".")
  var geo:String = ""
}