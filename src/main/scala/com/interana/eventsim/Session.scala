package com.interana.eventsim

import java.time.LocalDateTime

import com.interana.eventsim.TimeUtilities._
import com.interana.eventsim.buildin.{RandomProductGenerator, RandomSongGenerator }
import com.interana.eventsim.config.ConfigFromFile

/**
 * Object to capture session related calculations and properties
 */
class Session(var nextEventTimeStamp: Option[LocalDateTime],
              val alpha: Double, // expected request inter-arrival time
              val beta: Double, // expected session inter-arrival time
              val initialStates: scala.collection.Map[(String, String), WeightedRandomThingGenerator[State]],
              val auth: String,
              val level: String) {

  val sessionId = Counters.nextSessionId
  var itemInSession = 0
  var done = false
  var currentState: State = initialStates((auth, level)).randomThing
  var previousState: State = initialStates((auth, level)).randomThing
  val previousEventTimeStamp: Option[LocalDateTime] = nextEventTimeStamp
  var currentProduct: Option[(Option[String], Option[String], Option[String], Option[Double], Option[String], Option[Map[String, List[String]]], Option[Map[String, Double]], Option[String], Option[List[List[String]]])] = Some(RandomProductGenerator.nextProduct())
  var currentSong: Option[(String, String, String, Double)] =
    if (currentState.page == "NextSong") Some(RandomSongGenerator.nextSong()) else None
  var currentSongEnd: Option[LocalDateTime] =
    if (currentState.page == "NextSong") Some(nextEventTimeStamp.get.plusSeconds(currentSong.get._4.toInt)) else None

  def incrementEvent() = {
    val nextStateObject = currentState.nextState(rng)
    if (nextStateObject == None) {
      done = true
    } else {
      previousState = currentState

      val nextState = Some(nextStateObject.get._1)
      val tt = Some(nextStateObject.get._2._3)

      nextState match {
        case x if 300 until 399 contains x.get.status =>
          nextEventTimeStamp = Some(nextEventTimeStamp.get.plusSeconds(1))
          currentState = nextState.get
          itemInSession += 1

        case x if x.get.page == "NextSong" =>
          if (currentSong.isEmpty) {
            nextEventTimeStamp = Some(nextEventTimeStamp.get.plusSeconds(exponentialRandomValue(alpha * tt.get).toInt))
            currentSong = Some(RandomSongGenerator.nextSong())
          } else if (nextEventTimeStamp.get.isBefore(currentSongEnd.get)) {
            nextEventTimeStamp = currentSongEnd
            currentSong = Some(RandomSongGenerator.nextSong(currentSong.get._1))
          } else {
            nextEventTimeStamp = Some(nextEventTimeStamp.get.plusSeconds(exponentialRandomValue(alpha * tt.get).toInt))
            currentSong = Some(RandomSongGenerator.nextSong(currentSong.get._1))
          }
          currentSongEnd = Some(nextEventTimeStamp.get.plusSeconds(currentSong.get._4.toInt))
          currentState = nextState.get
          itemInSession += 1

        case _ =>
          nextEventTimeStamp = Some(nextEventTimeStamp.get.plusSeconds(exponentialRandomValue(alpha * tt.get).toInt))
          currentState = nextState.get
          itemInSession += 1

      }
    }
  }

  def nextSession =
    new Session(Some(Session.pickNextSessionStartTime(nextEventTimeStamp.get, beta)),
      alpha, beta, initialStates, currentState.auth, currentState.level)

}

object Session {

  def pickFirstTimeStamp(st: LocalDateTime,
                         alpha: Double, // expected request inter-arrival time
                         beta: Double // expected session inter-arrival time
                         ): LocalDateTime = {
    // pick random start point, iterate to steady state
    val startPoint = st.minusSeconds(beta.toInt * 2)
    var candidate = pickNextSessionStartTime(startPoint, beta)
    while (candidate.isBefore(st.minusSeconds(beta.toInt))) {
      candidate = pickNextSessionStartTime(candidate, beta)
    }
    candidate
  }

  def pickNextSessionStartTime(lastTimeStamp: LocalDateTime, beta: Double): LocalDateTime = {
    val randomGap = exponentialRandomValue(beta).toInt + ConfigFromFile.sessionGap
    val nextTimestamp: LocalDateTime = TimeUtilities.standardWarp(lastTimeStamp.plusSeconds(randomGap))
    assert(randomGap > 0)

    if (nextTimestamp.isBefore(lastTimeStamp)) {
      // force forward progress
      pickNextSessionStartTime(lastTimeStamp.plusSeconds(ConfigFromFile.sessionGap), beta)
    } else if (keepThisDate(lastTimeStamp, nextTimestamp)) {
      nextTimestamp
    } else
      pickNextSessionStartTime(nextTimestamp, beta)
  }
}
