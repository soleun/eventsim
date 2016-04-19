package com.interana.eventsim

import java.time.{ZoneOffset, LocalDateTime}
import java.io.OutputStream
import java.text.SimpleDateFormat
import java.util.Date
import com.fasterxml.jackson.core.JsonGenerator

import scala.collection.mutable
import com.interana.eventsim.TimeUtilities._
import com.interana.eventsim.EventType
import com.interana.eventsim.EventTransition
import com.interana.eventsim.devices.Device
import com.interana.eventsim.Channel
import com.interana.eventsim.buildin._

class Event (val eventType:EventType, val eventTime:Option[LocalDateTime], val user:User, val channel:Channel, val device:Option[Device] = None) extends Extractable {
  def getNamespace() = {
    "Event"
  }
  
  def getId() = {
    RandomStringGenerator.randomThing
  }
  
  def attributes = new mutable.HashMap[String, Any]()
  
  def getAttributeMap() = {
    val attributeMap = new mutable.HashMap[String, Any]()
    attributeMap.put("id", getId())
    attributes.foreach{ a => 
      attributeMap.put(a._1, a._2)
    }
    
    attributeMap
  }
  
  val metaData = new mutable.HashMap[String,Any]()
  val additionalActors = processAdditionalActors()
  
  processEvent()
  
  def processEvent() = {
    eventType.name match {
      case "Add To Cart" => {
        if (user.lastProduct.isDefined) {
          user.cart.addItem(user.lastProduct.get, 1)
        }
      }
      case "Checkout" => user.cart.clearCart()
      case "Cart Abandoned" => user.cart.clearCart()
      case _ => 
    }
  }
  
  def processAdditionalActors() = {
    val additionalActors = new mutable.HashMap[String, Extractable]()
    
    if (!eventType.additionalActors.isEmpty) {
      eventType.additionalActors.foreach {actor => 
        actor.get("fetchRule").get match {
          case "new" => {
            actor.get("type").get match {
              case "Product" => {
                val product = RandomProductGenerator.nextProduct()
                additionalActors.put("Product", product)
                user.lastProduct = Some(product)
              }
            }
          }
          case "user.cart" => {
            additionalActors.put("Cart", user.cart)
          }
          case "user.lastProduct" => {
            if (user.lastProduct.isDefined) {
              additionalActors.put("Product", user.lastProduct.get)
            }
          }
          case _ => println(actor)
        }
      }
    }
    
    additionalActors
  }
  
  def getCSVMap() = {
    val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSSZ")
    Map("name" -> eventType.name, "time" -> sdf.format(new Date(eventTime.get.toInstant(ZoneOffset.UTC).toEpochMilli)))
  }
  
  def generateNextEventTime(t:EventTransition) = {
    Some(eventTime.get.plusNanos(exponentialRandomValue(user.alpha * t.expectedTimeInBetween * 1000000000).toLong))
  }
  
  override def toString = {
    "eventType: "+eventType.name+", eventTime: "+eventTime.get.toString()+", user: "+user.userId+", channel: "+channel.name+", device:"+device+", exposedUserAttributes: "+user.getAttributeMap()
  }
  
  def getAllCSVMap() = {
    val csvMap = new mutable.HashMap[String,Any]()
    getCSVMap.foreach { x => 
      csvMap.put(getNamespace()+"."+x._1, x._2)
    }
    user.getCSVMap().foreach{ x => 
      csvMap.put(user.getNamespace()+"."+x._1, x._2)
    }
    channel.getCSVMap().foreach{ x => 
      csvMap.put(channel.getNamespace()+"."+x._1, x._2)
    }
    user.session.getCSVMap().foreach{ x => 
      csvMap.put(user.session.getNamespace()+"."+x._1, x._2)
    } 
    if(device.isDefined) {
      device.get.getCSVMap().foreach{ x => 
        csvMap.put(device.get.getNamespace()+"."+x._1, x._2)
      } 
    }
    if(!additionalActors.isEmpty) {
      additionalActors.foreach((p: (String, Extractable)) => {
        p._2.getCSVMap.foreach{ x => 
          csvMap.put(p._2.getNamespace()+"."+x._1, x._2)
        }
      })
    }
    csvMap
  }
  
  def writeDP(writer:JsonGenerator) = {
    val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSSZ")
    
//    var included_categories = mutable.ArrayBuffer[String]()
//    var included_salesranks = mutable.ArrayBuffer[Integer]()
    
    writer.writeStartObject()
    writer.writeStringField("eventType", eventType.name)
    writer.writeStringField("eventTime", sdf.format(new Date(eventTime.get.toInstant(ZoneOffset.UTC).toEpochMilli)))
    if (user.getAttributeMap().keySet.contains("userId") && user.getAttributeMap().get("userId").isDefined) {
      writer.writeStringField("actors.CUSTOMER.CUSTOMER_ID", user.getAttributeMap().get("userId").get.asInstanceOf[String])
    }
    writer.writeStringField("actors.CUSTOMER.SESSION_ID", user.session.sessionId)
    
    // User Attributes
    writeAttributes(writer, user.getAttributeMap(), user.getNamespace(), sdf)
    
    // Device
    if (device.isDefined) {
      writer.writeStringField("labels.Device_Type", device.get.dType)
//      writer.writeStringField("labels.Device_Id", device.get.deviceId)
      writeAttributes(writer, device.get.getAttributeMap(), device.get.getNamespace(), sdf)
    }
    
    // Channel
    writeAttributes(writer, channel.getAttributeMap(), channel.getNamespace(), sdf)
    
    // Additional Actors
    if (!additionalActors.isEmpty) {
      additionalActors.keySet.foreach { actorType =>
        val actorObject = additionalActors.get(actorType).get
        writer.writeStringField("actors."+actorObject.getNamespace()+".ID", actorObject.getId())
        writeAttributes(writer, actorObject.getAttributeMap(), actorObject.getNamespace(), sdf)
      }
    }
    
    if (Main.tag.isDefined)
        writer.writeStringField("labels.TAG", Main.tag.get)
    
    writer.writeEndObject()
    writer.writeRaw('\n')
    writer.flush()
  }
  
  def writeAttributes(writer:JsonGenerator, attributeSet:mutable.HashMap[String, Any], prefix:String, sdf:SimpleDateFormat) = {
    if (!attributeSet.isEmpty) {
      attributeSet.foreach((p: (String, Any)) => {
        p._2 match {
          case _: Long => writer.writeNumberField("numbers."+prefix+"_"+p._1, p._2.asInstanceOf[Long])
          case _: Int => writer.writeNumberField("numbers."+prefix+"_"+p._1, p._2.asInstanceOf[Int])
          case _: Double => writer.writeNumberField("numbers."+prefix+"_"+p._1, p._2.asInstanceOf[Double])
          case _: Float => writer.writeNumberField("numbers."+prefix+"_"+p._1, p._2.asInstanceOf[Float])
          case _: String => writer.writeStringField("labels."+prefix+"_"+p._1, p._2.asInstanceOf[String])
          case _: Date => writer.writeStringField("labels."+prefix+"_"+p._1, sdf.format(p._2).toString)
          case _: Some[Any] => {
            val actualValue = p._2.asInstanceOf[Some[Any]].get
            actualValue match {
              case _: Long => writer.writeNumberField("numbers."+prefix+"_"+p._1, actualValue.asInstanceOf[Long])
              case _: Int => writer.writeNumberField("numbers."+prefix+"_"+p._1, actualValue.asInstanceOf[Int])
              case _: Double => writer.writeNumberField("numbers."+prefix+"_"+p._1, actualValue.asInstanceOf[Double])
              case _: Float => writer.writeNumberField("numbers."+prefix+"_"+p._1, actualValue.asInstanceOf[Float])
              case _: String => writer.writeStringField("labels."+prefix+"_"+p._1, actualValue.asInstanceOf[String])
              case _: Date => writer.writeStringField("labels."+prefix+"_"+p._1, sdf.format(actualValue).toString)
              case _ => println(p._1); println(actualValue); println(actualValue.getClass())
            }
          }
          case _ => println(p._1); println(p._2); println(p._2.getClass())
        }
      })
    }
  }
  
  def getAttributesByType(attributeSet:mutable.HashMap[String, Any], prefix:String, sdf:SimpleDateFormat, attributesByType:Option[mutable.HashMap[String, mutable.HashMap[String, Any]]]) = {
    var tempAttributesByType = attributesByType
    if (!tempAttributesByType.isDefined) {
      tempAttributesByType = Some(new mutable.HashMap[String, mutable.HashMap[String, Any]]())
      tempAttributesByType.get.put("labels", new mutable.HashMap[String, Any]())
      tempAttributesByType.get.put("values", new mutable.HashMap[String, Any]())
    }
    
    if (!attributeSet.isEmpty) {
      attributeSet.foreach((p: (String, Any)) => {
        p._2 match {
          case _: Long => tempAttributesByType.get.get("values").get.put(prefix+"_"+p._1, p._2.asInstanceOf[Long])
          case _: Int => tempAttributesByType.get.get("values").get.put(prefix+"_"+p._1, p._2.asInstanceOf[Int])
          case _: Double => tempAttributesByType.get.get("values").get.put(prefix+"_"+p._1, p._2.asInstanceOf[Double])
          case _: Float => tempAttributesByType.get.get("values").get.put(prefix+"_"+p._1, p._2.asInstanceOf[Float])
          case _: String => tempAttributesByType.get.get("labels").get.put(prefix+"_"+p._1, p._2.asInstanceOf[String])
          case _: Date => tempAttributesByType.get.get("labels").get.put(prefix+"_"+p._1, sdf.format(p._2).toString)
          case _: Some[Any] => {
            val actualValue = p._2.asInstanceOf[Some[Any]].get
            actualValue match {
              case _: Long => tempAttributesByType.get.get("values").get.put(prefix+"_"+p._1, actualValue.asInstanceOf[Long])
              case _: Int => tempAttributesByType.get.get("values").get.put(prefix+"_"+p._1, actualValue.asInstanceOf[Int])
              case _: Double => tempAttributesByType.get.get("values").get.put(prefix+"_"+p._1, actualValue.asInstanceOf[Double])
              case _: Float => tempAttributesByType.get.get("values").get.put(prefix+"_"+p._1, actualValue.asInstanceOf[Float])
              case _: String => tempAttributesByType.get.get("labels").get.put(prefix+"_"+p._1, actualValue.asInstanceOf[String])
              case _: Date => tempAttributesByType.get.get("labels").get.put(prefix+"_"+p._1, sdf.format(actualValue).toString)
              case _ => println(p._1); println(actualValue); println(actualValue.getClass())
            }
          }
          case _ => println(p._1); println(p._2); println(p._2.getClass())
        }
      })
    }
    
    tempAttributesByType
  }
  
  def writeCSV(writer:OutputStream) = {
    var CSVMap = List[Any]()
    
    Main.CSVHeaders.foreach { x => 
      if(getAllCSVMap().keySet.contains(x)) {
        CSVMap = CSVMap :+ getAllCSVMap().get(x).get
      } else {
        CSVMap = CSVMap :+ ""
      }
    }
    writer.write((CSVMap.mkString("\t")+"\n").getBytes)
  }
  
  def writeIM(writer:JsonGenerator) = {
    val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSSZ")
    var attributesByType:Option[mutable.HashMap[String, mutable.HashMap[String, Any]]] = None
    
    attributesByType = getAttributesByType(user.getAttributeMap(), user.getNamespace(), sdf, attributesByType)
    if (device.isDefined) {
      attributesByType = getAttributesByType(device.get.getAttributeMap(), device.get.getNamespace(), sdf, attributesByType)
    }
    attributesByType = getAttributesByType(channel.getAttributeMap(), channel.getNamespace(), sdf, attributesByType)
    
    // Additional Actors
    if (!additionalActors.isEmpty) {
      additionalActors.keySet.foreach { actorType =>
        val actorObject = additionalActors.get(actorType).get
//        writer.writeStringField("actors."+actorObject.getNamespace()+".ID", actorObject.getId())
        attributesByType = getAttributesByType(actorObject.getAttributeMap(), actorObject.getNamespace(), sdf, attributesByType)
      }
    }
    
    writer.writeStartObject()
    writer.writeStringField("event type", eventType.name)
    writer.writeStringField("event time", sdf.format(new Date(eventTime.get.toInstant(ZoneOffset.UTC).toEpochMilli)))
    writer.writeObjectFieldStart("actors")
    if (user.getAttributeMap().keySet.contains("userId") && user.getAttributeMap().get("userId").isDefined) {
      writer.writeStringField("customer", user.getAttributeMap().get("userId").get.asInstanceOf[String])
    }
    writer.writeStringField("session", user.session.sessionId)
    if (!additionalActors.isEmpty) {
      additionalActors.keySet.foreach { actorType =>
        val actorObject = additionalActors.get(actorType).get
        writer.writeStringField(actorObject.getNamespace(), actorObject.getId())
      }
    }
    writer.writeEndObject()
    
    writer.writeObjectFieldStart("labels")
    attributesByType.get.get("labels").get.foreach((p: (String, Any)) => {
      writer.writeStringField(p._1, p._2.asInstanceOf[String])
    })
    if (Main.tag.isDefined)
        writer.writeStringField("TAG", Main.tag.get)
    writer.writeEndObject()
    
    writer.writeObjectFieldStart("values")
    attributesByType.get.get("values").get.foreach((p: (String, Any)) => {
      p._2 match {
        case _: Long => writer.writeNumberField(p._1, p._2.asInstanceOf[Long])
        case _: Int => writer.writeNumberField(p._1, p._2.asInstanceOf[Int])
        case _: Double => writer.writeNumberField(p._1, p._2.asInstanceOf[Double])
        case _: Float => writer.writeNumberField(p._1, p._2.asInstanceOf[Float])
        case _ => 
      }
    })
    writer.writeEndObject()
    
    writer.writeEndObject()
    writer.writeRaw('\n')
    writer.flush()
  }
}

object Event extends Actor {
  def getNamespace() = {
    "Event"
  }
  
  def getCSVHeaders() = {
    List("name", "time")
  }
}