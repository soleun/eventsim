package com.interana.eventsim

import scala.collection.mutable.HashMap

trait Extractable {
  def getNamespace(): String
  def getCSVMap(): Map[String, Any]
  def getId(): String
  def attributes: HashMap[String, Any]
  def getAttributeMap(): HashMap[String, Any]
}