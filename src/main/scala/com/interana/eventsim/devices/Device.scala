package com.interana.eventsim.devices

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.HashMap
import com.interana.eventsim.buildin.RandomStringGenerator
import com.interana.eventsim.{Actor, Extractable}

class Device(val dType:String) extends Extractable {
  val deviceId:String = RandomStringGenerator.randomAlphanumericString(20)
  val supportedChannels:ArrayBuffer[String] = new ArrayBuffer[String]()
  
  def getNamespace() = {
    "Device"
  }
  
  def getId() = {
    deviceId
  }
  
  def getCSVMap() = {
    Map[String,Any]("id" -> deviceId, "type" -> dType)
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

object Device extends Actor {
  def getNamespace() = {
    "Device"
  }
  
  def getCSVHeaders() = {
    List("id", "type")
  }
}