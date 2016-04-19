package com.interana.eventsim

import scala.collection.mutable.HashMap
import com.interana.eventsim.buildin.RandomStringGenerator

class Channel(val name:String) extends Extractable {
  val attributes = new HashMap[String,Any]()
  
  def getNamespace() = {
    "Channel"
  }
  
  def getId() = {
    RandomStringGenerator.randomThing
  }
  
  def getCSVMap() = {
    Map("name" -> name)
  }
  
  def getAttributeMap() = {
    val attributeMap = new HashMap[String, Any]()
    attributeMap.put("id", getId())
    attributeMap.put("name", name)
    attributes.foreach{ a => 
      attributeMap.put(a._1, a._2)
    }
    
    attributeMap
  }
}

object Channel extends Actor {
  def getNamespace() = {
    "Channel"
  }
  
  def getCSVHeaders() = {
    List("name")
  }
}