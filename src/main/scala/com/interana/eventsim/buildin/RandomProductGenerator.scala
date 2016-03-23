package com.interana.eventsim.buildin

import java.io.FileInputStream
import java.util.zip.GZIPInputStream

import com.interana.eventsim.WeightedRandomThingGenerator
import com.interana.eventsim.Product

import scala.collection.mutable
import scala.io.Source
import scala.util.parsing.json._

object RandomProductGenerator extends WeightedRandomThingGenerator[String] {
  System.err.println("Loading product file...")
  val fis = new FileInputStream("data/metadata_top.strict.json.gz")
  val gis = new GZIPInputStream(fis)
  val s = Source.fromInputStream(gis,"ISO-8859-1")
  
  val pFileLines = s.getLines()
  
  //val productMap = new mutable.HashMap[String,Map[String,Any]]()
  var productMap:Map[String,Product] = Map[String,Product]()
  var count = 0
  
  for (md <- pFileLines) {
    val result = JSON.parseFull(md)
    
    result match {
      case Some(m:Map[String,Any]) => {
        val asin = m.get("asin").get.asInstanceOf[String]
        
        this.add(asin, 1)
        productMap += (asin -> new Product(m))
      }
      case None => println("Failed.")
    }
    
    if (count % 10000 == 0) {
      println(count)
    }
    
    count += 1
  }
    
  def nextProduct(): (Product) = {
    var asin = Some(this.randomThing)
    val product = productMap(asin.get)
    
    product
  }
  
  def escape(raw: String): String = {
    import scala.reflect.runtime.universe._
    Literal(Constant(raw)).toString
  }
}