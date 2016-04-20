package com.interana.eventsim

import scala.collection.mutable.HashMap

trait Extractable {
  val attributes: HashMap[String, Any]
  
  def getNamespace(): String
  def getCSVMap(): Map[String, Any]
  def getId(): String
  def getAttributeMap(): HashMap[String, Any]
}