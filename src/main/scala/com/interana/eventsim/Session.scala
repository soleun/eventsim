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
              val initialStates: scala.collection.Map[(String, String), WeightedRandomThingGenerator[State]],
              val auth: String,
              val level: String,
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
  val events = mutable.ArrayBuffer[Event]()
  events.append(currentEvent)
  
  val sessionId = RandomStringGenerator.randomAlphanumericString(30)
//  var itemInSession = 0
  var done = false
  var currentState: State = initialStates((user.auth, user.level)).randomThing
  var previousState: Option[State] = None
  val previousEventTimeStamp: Option[LocalDateTime] = nextEventTimeStamp
//  var currentProduct: Product = RandomProductGenerator.nextProduct()
//  var currentSong: Option[(String, String, String, Double)] =
//    if (currentState.page == "NextSong") Some(RandomSongGenerator.nextSong()) else None
//  var currentSongEnd: Option[LocalDateTime] =
//    if (currentState.page == "NextSong") Some(nextEventTimeStamp.get.plusSeconds(currentSong.get._4.toInt)) else None
  
  def getCSVMap() = {
    Map("id" -> sessionId)
  }
  
  def getId() = {
    sessionId
  }
  
  def attributes = new mutable.HashMap[String, Any]()
  
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
      
      events.append(nextEvent)
      currentEvent = nextEvent
    }
    
    
    val nextStateObject = currentState.nextState(rng)
    if (nextStateObject == None) {
      done = true
    } else {
      previousState = Some(currentState)

      val nextState = Some(nextStateObject.get._1)
      val tt = Some(nextStateObject.get._2._3)

      nextState match {
        case x if 300 until 399 contains x.get.status =>
          nextEventTimeStamp = Some(nextEventTimeStamp.get.plusSeconds(1))
          currentState = nextState.get
//          itemInSession += 1

//        case x if x.get.page == "NextSong" =>
//          if (currentSong.isEmpty) {
//            nextEventTimeStamp = Some(nextEventTimeStamp.get.plusNanos(exponentialRandomValue(alpha * tt.get * 1000000000).toLong))
//            currentSong = Some(RandomSongGenerator.nextSong())
//          } else if (nextEventTimeStamp.get.isBefore(currentSongEnd.get)) {
//            nextEventTimeStamp = currentSongEnd
//            currentSong = Some(RandomSongGenerator.nextSong(currentSong.get._1))
//          } else {
//            nextEventTimeStamp = Some(nextEventTimeStamp.get.plusNanos(exponentialRandomValue(alpha * tt.get * 1000000000).toLong))
//            currentSong = Some(RandomSongGenerator.nextSong(currentSong.get._1))
//          }
//          currentSongEnd = Some(nextEventTimeStamp.get.plusSeconds(currentSong.get._4.toInt))
//          currentState = nextState.get
//          itemInSession += 1

        case _ =>
          nextEventTimeStamp = Some(nextEventTimeStamp.get.plusNanos(exponentialRandomValue(alpha * tt.get * 1000000000).toLong))
          currentState = nextState.get
//          itemInSession += 1

      }
    }
  }

  def nextSession() =
    new Session(Some(Session.pickNextSessionStartTime(nextEventTimeStamp.get, beta)),
      alpha, beta, initialStates, currentState.auth, currentState.level, user)
  
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
    new Session(Some(Session.pickFirstTimeStamp(user.startTime, user.alpha, user.beta)), user.alpha, user.beta, user.initialSessionStates, user.auth, user.level, user)
  }
  
  def getNamespace() = {
    "Session"
  }
  
  def getCSVHeaders() = {
    List("id")
  }
}
