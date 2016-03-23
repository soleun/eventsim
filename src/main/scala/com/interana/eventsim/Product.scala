package com.interana.eventsim

class Product(val m:Map[String, Any]) {
  val asin = m.get("asin").get.asInstanceOf[String]
  val description:Option[String] = if (m.keySet("description")) Some(escape(m.get("description").get.asInstanceOf[String])) else None
  val title:Option[String] = if (m.keySet("title")) Some(escape(m.get("title").get.asInstanceOf[String])) else None
  val price:Option[Double] = if (m.keySet("price")) Some(m.get("price").get.asInstanceOf[Double]) else None
  val imUrl:Option[String] = if (m.keySet("imUrl")) Some(m.get("imUrl").get.asInstanceOf[String]) else None
  val related:Option[Map[String,List[String]]] = if (m.keySet("related")) Some(m.get("related").get.asInstanceOf[Map[String,List[String]]]) else None
  val salesRank:Option[Map[String,Double]] = if (m.keySet("salesRank")) Some(m.get("salesRank").get.asInstanceOf[Map[String,Double]]) else None
  val brand:Option[String] = if (m.keySet("brand")) Some(m.get("brand").get.asInstanceOf[String]) else None
  val categories:Option[List[List[String]]] = if (m.keySet("categories")) Some(m.get("categories").get.asInstanceOf[List[List[String]]]) else None
  
  def escape(raw: String): String = {
    import scala.reflect.runtime.universe._
    Literal(Constant(raw)).toString
  }
}