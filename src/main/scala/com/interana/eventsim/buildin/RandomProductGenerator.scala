package com.interana.eventsim.buildin

import java.io.FileInputStream
import java.util.zip.GZIPInputStream

import com.interana.eventsim.WeightedRandomThingGenerator

import scala.collection.mutable
import scala.io.Source
import scala.util.parsing.json._

object RandomProductGenerator extends WeightedRandomThingGenerator[String] {
  System.err.println("Loading product file...")
  val fis = new FileInputStream("data/metadata_top.strict.json.gz")
  val gis = new GZIPInputStream(fis)
  val s = Source.fromInputStream(gis,"ISO-8859-1")
  
  val pFileLines = s.getLines()
  
  val productMap = new mutable.HashMap[String,Map[String,Any]]()
  var count = 0
  
  for (md <- pFileLines) {
    val result = JSON.parseFull(md)
    
    result match {
      case Some(m:Map[String,Any]) => {
        this.add(m.get("asin").get.asInstanceOf[String], 1)
        productMap.put(m.get("asin").get.asInstanceOf[String], m)
      }
      case None => println("Failed.")
    }
    
    if (count % 10000 == 0) {
      println(count)
    }
    
    count += 1
  }
  
  def nextProduct(): (Option[String],Option[String],Option[String],Option[Double],Option[String],Option[Map[String,List[String]]],Option[Map[String,Double]],Option[String],Option[List[List[String]]]) = {
    var asin = Some(this.randomThing)
    val product = productMap.get(asin.get).get
    
    val description:Option[String] = if (product.keySet("description")) Some(escape(product.get("description").get.asInstanceOf[String])) else None
    val title:Option[String] = if (product.keySet("title")) Some(escape(product.get("title").get.asInstanceOf[String])) else None
    val price:Option[Double] = if (product.keySet("price")) Some(product.get("price").get.asInstanceOf[Double]) else None
    val imUrl:Option[String] = if (product.keySet("imUrl")) Some(product.get("imUrl").get.asInstanceOf[String]) else None
    val related:Option[Map[String,List[String]]] = if (product.keySet("related")) Some(product.get("related").get.asInstanceOf[Map[String,List[String]]]) else None
    val salesRank:Option[Map[String,Double]] = if (product.keySet("salesRank")) Some(product.get("salesRank").get.asInstanceOf[Map[String,Double]]) else None
    val brand:Option[String] = if (product.keySet("brand")) Some(product.get("brand").get.asInstanceOf[String]) else None
    val categories:Option[List[List[String]]] = if (product.keySet("categories")) Some(product.get("categories").get.asInstanceOf[List[List[String]]]) else None
    
    (asin, description, title, price, imUrl, related, salesRank, brand, categories)
  }
  
  def escape(raw: String): String = {
    import scala.reflect.runtime.universe._
    Literal(Constant(raw)).toString
  }
}