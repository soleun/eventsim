package com.pointillist.util

import scala.util.Random

class GaussianRandomNumberGenerator(
    val expected: Double,
    val min: Double,
    val max: Double,
    val sd: Double = 0) {
  def getRandomNumber = {
    val r = scala.util.Random
    var notvalid = true
    var value:Integer = 0

    do {
     value = math.round((r.nextGaussian() * sd + expected).toFloat)
     if (value <= max.toInt && value >= min.toInt) {
       notvalid = false
     }
    } while(notvalid)
    
    value
  }
}