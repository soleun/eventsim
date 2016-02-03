package com.interana.eventsim.buildin

import java.time.{ZoneOffset, LocalDateTime}
import java.text.SimpleDateFormat
import java.util.Date

import com.interana.eventsim.{Constants, Main, TimeUtilities}

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
      "actors.CUSTOMER.GENDER" -> firstNameAndGender._2,
      "actors.CUSTOMER.REGISTRATION_TIME" -> new Date(registrationTime.toInstant(ZoneOffset.UTC).toEpochMilli),
      "actors.CUSTOMER.LOCATION" -> location,
      "actors.CUSTOMER.USER_AGENT" -> RandomUserAgentGenerator.randomThing._1
    )
  }

  def randomNewProps(dt: LocalDateTime) =
    randomProps + ("registration" -> dt.toInstant(ZoneOffset.UTC).toEpochMilli)

}
