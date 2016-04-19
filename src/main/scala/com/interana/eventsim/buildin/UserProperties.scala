package com.interana.eventsim.buildin

import java.time.{ZoneOffset, LocalDateTime}
import java.text.SimpleDateFormat
import java.util.Date

import scala.collection.mutable
import com.interana.eventsim.{Constants, Main, TimeUtilities,WeightedRandomThingGenerator}
import com.pointillist.util.GaussianRandomNumberGenerator

object UserProperties {
  // utilities for generating random properties for users

  def randomProps = {
    val secondsSinceRegistration =
      Math.min(
        TimeUtilities.exponentialRandomValue(Main.growthRate.getOrElse(0.0)*Constants.SECONDS_PER_YEAR).toInt,
        (Constants.SECONDS_PER_YEAR * 5).toInt)

    val registrationTime = Main.startTime.minusSeconds(secondsSinceRegistration)
    val firstNameAndGender = RandomFirstNameGenerator.randomThing
    val location = RandomLocationGenerator.randomThing

    Map[String,Any](
      "actors.CUSTOMER.LAST_NAME" -> RandomLastNameGenerator.randomThing,
      "actors.CUSTOMER.FIRST_NAME" -> firstNameAndGender._1,
      "numbers.REGISTRATION_TIME" -> new Date(registrationTime.toInstant(ZoneOffset.UTC).toEpochMilli),
      "location.LOCATION" -> location,
      "labels.USER_AGENT" -> RandomUserAgentGenerator.randomThing._1
    )
  }
  
  def customProps(m: mutable.HashMap[String,Any]) = {
    var props:mutable.HashMap[String,Any] = mutable.HashMap[String,Any]()
    for ((k,v) <- m) {
      v match {
        case _: WeightedRandomThingGenerator[String] => 
          props.put(k, v.asInstanceOf[WeightedRandomThingGenerator[String]].randomThing)
        case _: GaussianRandomNumberGenerator =>
          props.put(k, v.asInstanceOf[GaussianRandomNumberGenerator].getRandomNumber)
      }
    }
    
    props
  }

  def randomNewProps(dt: LocalDateTime) =
    randomProps + ("registration" -> dt.toInstant(ZoneOffset.UTC).toEpochMilli)

}
