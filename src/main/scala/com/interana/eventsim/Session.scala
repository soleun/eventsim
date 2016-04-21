package com.interana.eventsim

import java.time.LocalDateTime
import scala.collection.mutable

import com.interana.eventsim.TimeUtilities._
import com.interana.eventsim.buildin.{RandomProductGenerator, RandomSongGenerator, RandomStringGenerator}
import com.interana.eventsim.config.ConfigFromFile
import com.interana.eventsim.devices.Device

/**
 * Object to capture session related calculations and properties
 */
class Session(var nextEventTimeStamp: Option[LocalDateTime],
              val alpha: Double, // expected request inter-arrival time
              val beta: Double, // expected session inter-arrival time
//              val initialStates: scala.collection.Map[(String, String), WeightedRandomThingGenerator[State]],
//              val auth: String,
//              val level: String,
              val user: User) extends Extractable {

  def getNamespace() = {
    "Session"
  }
  
  /*
   * TODO: restructure session. Session needs following
   * 	channel which connects current device from user
   * 	current event with pointers to next event (based on the channel)
   * 
   * TODO:
   * 	remove state
   * 	simplify incrementEvent()
   */
  
  //reset userAttributes
  user.exposedAttributes = Set[String]()
  
  val entryPoints = user.entryPoints
//  println(user.devices)
//  println(user.channelPreferences.ab)
  
  var channelName = user.channelPreferences.randomThing
  var currentChannel = user.channels.get(channelName).get
  var currentDevice = user.getDeviceForChannel(currentChannel)
//  println(channelName, currentDevice)
  var currentEventType: EventType = entryPoints.get(currentChannel.name).get.randomThing
  var currentEvent: Event = currentEventType.generateEvent(nextEventTimeStamp, user, currentChannel, currentDevice)
  var previousEvent: Option[Event] = None
  val events = mutable.ArrayBuffer[Event]()
  events.append(currentEvent)
  
  val sessionId = RandomStringGenerator.randomAlphanumericString(30)
//  var itemInSession = 0
  var done = false
//  var currentState: State = initialStates((user.auth, user.level)).randomThing
//  var previousState: Option[State] = None
  val previousEventTimeStamp: Option[LocalDateTime] = nextEventTimeStamp
  
  def getCSVMap() = {
    Map("id" -> sessionId)
  }
  
  def getId() = {
    sessionId
  }
  
  val attributes = new mutable.HashMap[String, Any]()
  
  def getAttributeMap() = {
    val attributeMap = new mutable.HashMap[String, Any]()
    attributeMap.put("id", getId())
    attributes.foreach{ a => 
      attributeMap.put(a._1, a._2)
    }
    
    attributeMap
  }
  
  def incrementEvent() = {
    val nextEventTransition = currentEvent.eventType.nextCompatibleTransition(currentChannel, user)
    if (nextEventTransition == None) {
      done = true
//      println("session:"+events.map { x => x.toString() })
    } else {
      val nextEventTime = currentEvent.generateNextEventTime(nextEventTransition.get)
      val nextEvent = nextEventTransition.get.to.generateEvent(nextEventTime, user, currentChannel, currentDevice)
      
      nextEventTimeStamp = nextEventTime
      
      previousEvent = Some(currentEvent)
      events.append(nextEvent)
      currentEvent = nextEvent
    }
  }

  def nextSession() =
    new Session(Some(Session.pickNextSessionStartTime(nextEventTimeStamp.get, beta)),
      alpha, beta, user)
  
}

object Session extends Actor {

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
  
  def createSession(user:User) = {
    new Session(Some(Session.pickFirstTimeStamp(user.startTime, user.alpha, user.beta)), user.alpha, user.beta, user)
  }
  
  def getNamespace() = {
    "Session"
  }
  
  def getCSVHeaders() = {
    List("id")
  }
}
