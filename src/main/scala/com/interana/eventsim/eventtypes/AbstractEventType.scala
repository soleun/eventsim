package com.interana.eventsim.eventtypes

import scala.collection.mutable
import com.interana.eventsim.User
import com.interana.eventsim.Channel

abstract class AbstractEventType {
  val name:String
  val compatibleChannels:mutable.HashMap[String,Channel]
  val compatibleUserLevels:List[String]
  val endOfSessionTimeGap:Integer
  
//  def isCompatible(user:User) = {
//    
//  }
}