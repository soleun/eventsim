package com.interana.eventsim.sessions

import com.interana.eventsim.buildin.RandomStringGenerator
import com.pointillist.util.RandomNumberGenerator

class Web {
  var ipaddr:String = List(RandomNumberGenerator.getPositiveRandomNumber(256), RandomNumberGenerator.getPositiveRandomNumber(256), RandomNumberGenerator.getPositiveRandomNumber(256), RandomNumberGenerator.getPositiveRandomNumber(256)).mkString(".")
  var cookieid:String = RandomStringGenerator.randomAlphanumericString(40)
}