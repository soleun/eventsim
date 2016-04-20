package com.interana.eventsim

import scala.collection.mutable.HashMap
import com.interana.eventsim.buildin.RandomStringGenerator

class Order extends Extractable {
  def getNamespace() = {
    "Order"
  }
  
  def getId() = {
    RandomStringGenerator.randomThing
  }
  
  def getCSVMap() = {
    Map()
  }
  
  val attributes = new HashMap[String, Any]()
  
  def getAttributeMap() = {
    val attributeMap = new HashMap[String, Any]()
    attributeMap.put("id", getId())
    attributes.foreach{ a => 
      attributeMap.put(a._1, a._2)
    }
    
    attributeMap
  }
}

object Order extends Actor {
  def getNamespace() = {
    "Order"
  }
  
  def getCSVHeaders() = {
    List()
  }
}