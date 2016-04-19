package com.pointillist.util

import scala.util.Random

object RandomNumberGenerator {
  def getRandomNumber(min: Double, max: Double) = {
    val r = scala.util.Random
    math.round((r.nextDouble() * (max-min) + min).toFloat)
  }
  
  def getPositiveRandomNumber(max: Double) = {
    getRandomNumber(0, max)
  }
}