package com.interana.eventsim

import scala.collection.mutable.HashMap

class Product(val m:Map[String, Any]) extends Extractable {
  def getNamespace() = {
    "Product"
  }
  
  val attributes = new HashMap[String, Any]()
  
  val asin = m.get("asin").get.asInstanceOf[String]
  val description:Option[String] = if (m.keySet("description")) Some(escape(m.get("description").get.asInstanceOf[String])) else None
  val title:Option[String] = if (m.keySet("title")) Some(escape(m.get("title").get.asInstanceOf[String])) else None
  val price:Option[Double] = if (m.keySet("price")) Some(m.get("price").get.asInstanceOf[Double]) else None
  val imUrl:Option[String] = if (m.keySet("imUrl")) Some(m.get("imUrl").get.asInstanceOf[String]) else None
  val related:Option[Map[String,List[String]]] = if (m.keySet("related")) Some(m.get("related").get.asInstanceOf[Map[String,List[String]]]) else None
  val salesRank:Option[Map[String,Double]] = if (m.keySet("salesRank")) Some(m.get("salesRank").get.asInstanceOf[Map[String,Double]]) else None
  val brand:Option[String] = if (m.keySet("brand")) Some(m.get("brand").get.asInstanceOf[String]) else None
  val categories:Option[List[List[String]]] = if (m.keySet("categories")) Some(m.get("categories").get.asInstanceOf[List[List[String]]]) else None
  
  attributes.put("description", description)
  attributes.put("title", title)
  attributes.put("price", price)
  attributes.put("imUrl", imUrl)
  attributes.put("brand", brand)
  
  def getId() = {
    asin
  }
  
  def getAttributeMap() = {
    val attributeMap = new HashMap[String, Any]()
    attributeMap.put("id", getId())
    attributes.foreach{ a => 
      a._2 match {
        case _: Some[Any] => {
          if (a._2.asInstanceOf[Some[Any]].isDefined) {
            attributeMap.put(a._1, a._2.asInstanceOf[Some[Any]].get)
          }
        }
        case _ => 
      }
    }
    
    attributeMap
  }
  
  def getCSVMap() = {
    val attributeMap = new HashMap[String, Any]()
    getAttributeMap.foreach{ p => 
      if(Product.getCSVHeaders().contains(p._1)) {
        attributeMap.put(p._1, p._2)
      }
    }
    
    attributeMap.toMap
  }
  
  def escape(raw: String): String = {
    import scala.reflect.runtime.universe._
    Literal(Constant(raw)).toString
  }
}

object Product extends Actor {
  def getNamespace() = {
    "Product"
  }
  
  def getCSVHeaders() = {
    List("id", "description", "title", "price", "imUrl", "brand")
  }
}