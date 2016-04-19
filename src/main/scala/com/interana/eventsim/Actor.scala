package com.interana.eventsim

trait Actor {
  def getNamespace(): String
  def getCSVHeaders(): List[String]
}