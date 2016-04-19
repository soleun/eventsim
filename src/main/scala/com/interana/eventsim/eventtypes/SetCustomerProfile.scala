package com.interana.eventsim.eventtypes

import scala.collection.mutable
import com.interana.eventsim.Channel

class SetCustomerProfile(val compatibleChannels:mutable.HashMap[String,Channel], val compatibleUserLevels:List[String], val endOfSessionTimeGap:Integer) extends AbstractEventType {
  val name = "Set Customer Profile"

}