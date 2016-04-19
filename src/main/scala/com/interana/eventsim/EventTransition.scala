package com.interana.eventsim

import com.interana.eventsim.EventType

class EventTransition(val from:EventType, val to:EventType, val probability:Double, val expectedTimeInBetween:Double) {
  
}