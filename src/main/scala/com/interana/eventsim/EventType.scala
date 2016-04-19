package com.interana.eventsim

import java.time.LocalDateTime
import scala.collection.mutable
import com.interana.eventsim.Channel
import com.interana.eventsim.devices.Device

/*
 * Supported User Attributes
 * 	userId
 * 	email
 * 	firstName
 * 	lastName
 * 	gender
 * 	race
 * 	income
 * 	age
 * 	education
 * 	employment
 * 	marital status
 * 	car
 * 	interest
 * 	activity
 */

class EventType(val name:String, val compatibleChannels:mutable.HashMap[String,Channel], val endOfSessionTimeGap:Integer, val additionalActors:List[Map[String,String]], val userAttributes:(Set[String], Set[String])) {
  val transitions = new mutable.HashMap[(EventType, EventType), EventTransition]()
  val transitionProbilities = new mutable.HashMap[(Double, Double), EventTransition]()
  val eventSelectorByChannel = new mutable.HashMap[String, WeightedRandomThingGenerator[EventType]]()
  val appendUserAttributes = userAttributes._1
  val removeUserAttributes = userAttributes._2
  
  private def maxP() =
    if (transitionProbilities.nonEmpty) transitionProbilities.map(_._1._2).max else 0.0

  def maxTransitionsP = maxP()
  
  private def inRange(v: Double, s:((Double, Double), EventTransition)) = v >= s._1._1 && v < s._1._2
  
  def addTransition(t: EventTransition) = {
    val oldMax = this.maxP()
    val p = t.probability
    if (oldMax + p > 1.0) {
      throw new Exception(
        "Adding a transition from " + t.from + " to " + t.to + " with probability " + p +
          " would make the total transition probability greater than 1")
    }
    transitions.put((this, t.to), t)
    transitionProbilities.put((oldMax, oldMax+p), t)
  }
  
  def nextCompatibleTransition(channel:Channel, user:User) = {
    val filteredTransitions = filterTransitionsByChannel(channel)
    val randomSelector = new WeightedRandomThingGenerator[EventTransition]()
    if(filteredTransitions.length > 0) {
      filteredTransitions.foreach { t => 
        randomSelector.add(t, t.probability)
      }
      Some(randomSelector.randomThing)
    } else {
      None
    }
  }
  
  def nextCompatibleEventType(channel:Channel, user:User) = {
    val filteredTransitions = filterTransitionsByChannel(channel)
    val randomSelector = new WeightedRandomThingGenerator[EventType]()
    if(filteredTransitions.length > 0) {
      filteredTransitions.foreach { t => 
        randomSelector.add(t.to, t.probability)
      }
      Some(randomSelector.randomThing)
    } else {
      None
    }
  }
  
  def filterTransitionsByChannel(channel:Channel):mutable.ArrayBuffer[EventTransition] = {
    val filteredTransitions = new mutable.ArrayBuffer[EventTransition]()
    transitions.foreach { t =>  
      if(t._2.to.compatibleChannels.keySet.contains(channel.name)) {
        filteredTransitions.append(t._2)
      }
    }
    
    filteredTransitions
  }
  
  def generateEvent(eventTime:Option[LocalDateTime], user:User, channel:Channel, device:Device) = {
//    val before = user.exposedAttributes
    if(appendUserAttributes.nonEmpty) user.exposedAttributes ++= appendUserAttributes
    if(removeUserAttributes.nonEmpty) user.exposedAttributes --= removeUserAttributes
    
    val e = new Event(this, eventTime, user, channel, Some(device))
    
//    if(!before.equals(user.exposedAttributes)) {
//      println("before: ",user.userId, name, user.session.sessionId, before)
//      println("after: ",user.userId, name, user.session.sessionId, user.exposedAttributes)
//    }
//    println(e)
    e
  }
}