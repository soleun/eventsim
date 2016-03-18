package com.interana.eventsim

import java.io.{OutputStream, Serializable, FileWriter}
import java.time.{ZoneOffset, LocalDateTime}
import java.text.SimpleDateFormat
import java.util.Date

import com.fasterxml.jackson.core.{JsonEncoding, JsonFactory}
import com.interana.eventsim.config.ConfigFromFile

import scala.collection.mutable.ArrayBuffer
import scala.util.parsing.json.JSONObject
import scala.io.Source

class User(val alpha: Double,
           val beta: Double,
           val startTime: LocalDateTime,
           val initialSessionStates: scala.collection.Map[(String,String),WeightedRandomThingGenerator[State]],
           val auth: String,
           val props: Map[String,Any],
           var device: scala.collection.immutable.Map[String,Any],
           val initialLevel: String,
           val stream: OutputStream,
           val csvstream: OutputStream,
           val imstream: OutputStream
          ) extends Serializable with Ordered[User] {

  val userId = Counters.nextUserId
  var session = new Session(
    Some(Session.pickFirstTimeStamp(startTime, alpha, beta)),
      alpha, beta, initialSessionStates, auth, initialLevel)
  
  var currentCartValue = 0.0;
  var currentItemsInCart = 0;
  val eventsWithProductData:List[String] = List("Product Searched", "Return in Store", "Add To Cart", "Bar Code Scanned", "Checkout", "Product Compared", "Product Rated", "Product Recommended", "Product Reviewed", "Product Shared", "Product Viewed") 
  val eventsWithCustomerData:List[String] = List("Set Customer Profile")
  
  var lastEventTime = this.session.nextEventTimeStamp
  var prevSessionId = this.session.sessionId

  override def compare(that: User) =
    (that.session.nextEventTimeStamp, this.session.nextEventTimeStamp) match {
      case (None, None) => 0
      case (_: Some[LocalDateTime], None) => -1
      case (None, _: Some[LocalDateTime]) => 1
      case (thatValue: Some[LocalDateTime], thisValue: Some[LocalDateTime]) =>
        thatValue.get.compareTo(thisValue.get)
    }

  def nextEvent(): Unit = nextEvent(0.0)

  def nextEvent(prAttrition: Double) = {
    session.incrementEvent()
    if (session.done) {
      if (TimeUtilities.rng.nextDouble() < prAttrition ||
          session.currentState.auth == ConfigFromFile.churnedState.getOrElse("")) {
        session.nextEventTimeStamp = None
        // TODO: mark as churned
      }
      else {
        session = session.nextSession
      }
    }
  }

  private val EMPTY_MAP = Map()

  def eventString = {
    val showUserDetails = ConfigFromFile.showUserWithState(session.currentState.auth)
    val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ssZ")
    var m = device.+(
      "ts" -> sdf.format(new Date(session.nextEventTimeStamp.get.toInstant(ZoneOffset.UTC).toEpochMilli)),
      "userId" -> (if (showUserDetails) userId else ""),
      "sessionId" -> session.sessionId,
      "page" -> session.currentState.page,
      "auth" -> session.currentState.auth,
      "method" -> session.currentState.method,
      "status" -> session.currentState.status,
      "itemInSession" -> session.itemInSession
    )

    if (showUserDetails)
      m ++= props

    /* most of the event generator code is pretty generic, but this is hard-coded
     * for a fake music web site
     */
    if (session.currentState.page=="NextSong")
      m += (
        "artist" -> session.currentSong.get._2,
        "song" -> session.currentSong.get._3,
        "length" -> session.currentSong.get._4
        )

    val j = new JSONObject(m)
    j.toString()
  }


  val writer = User.jsonFactory.createGenerator(stream, JsonEncoding.UTF8)
  val imwriter = User.jsonFactory.createGenerator(imstream, JsonEncoding.UTF8)

  def writeEvent() = {
    // use Jackson streaming to maximize efficiency
    // (earlier versions used Scala's std JSON generators, but they were slow)
    val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ssZ")
    val sdfSimple = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val showUserDetails = ConfigFromFile.showUserWithState(session.currentState.auth)
    
    var included_categories:ArrayBuffer[String] = ArrayBuffer[String]()
    var included_salesranks:ArrayBuffer[Integer] = ArrayBuffer[Integer]()
    
    writer.writeStartObject()
    writer.writeStringField("eventTime", sdf.format(new Date(session.nextEventTimeStamp.get.toInstant(ZoneOffset.UTC).toEpochMilli)))
    writer.writeStringField("actors.CUSTOMER.CUSTOMER_ID", if (showUserDetails) userId.toString else "")
    writer.writeNumberField("actors.CUSTOMER.SESSION_ID", session.sessionId)
    writer.writeStringField("eventType", session.currentState.page)
    //writer.writeStringField("labels.AUTH", session.currentState.auth)
    //writer.writeStringField("labels.HTTP_METHOD", session.currentState.method)
    //writer.writeNumberField("labels.HTTP_STATUS", session.currentState.status)
    //writer.writeStringField("labels.USER_LEVEL", session.currentState.level)
    //writer.writeNumberField("labels.ITEM_IN_SESSION", session.itemInSession)
    if (showUserDetails && eventsWithCustomerData.contains(session.currentState.page)) {
      props.foreach((p: (String, Any)) => {
        p._2 match {
          case _: Long => writer.writeNumberField(p._1, p._2.asInstanceOf[Long])
          case _: Int => writer.writeNumberField(p._1, p._2.asInstanceOf[Int])
          case _: Double => writer.writeNumberField(p._1, p._2.asInstanceOf[Double])
          case _: Float => writer.writeNumberField(p._1, p._2.asInstanceOf[Float])
          case _: String => writer.writeStringField(p._1, p._2.asInstanceOf[String])
          case _: Date => writer.writeStringField(p._1, sdf.format(p._2).toString)
        }})
      if (Main.tag.isDefined)
        writer.writeStringField("actors.CUSTOMER.TAG", Main.tag.get)
    }
    if (eventsWithProductData.contains(session.currentState.page)) {
      val product = session.currentProduct.get
      
      //(asin, description, title, price, imUrl, related, salesRank, brand, categories)
      writer.writeStringField("labels.PRODUCT_ASIN", product._1.get.asInstanceOf[String])
      if (product._2 != None) {
        writer.writeStringField("labels.PRODUCT_DESCRIPTION", product._2.get.asInstanceOf[String])
      }
      if (product._3 != None) {
        writer.writeStringField("labels.PRODUCT_TITLE", product._3.get.asInstanceOf[String])
      }
      if (product._4 != None) {
        val price = product._4.get.asInstanceOf[Double]
        
        writer.writeNumberField("labels.PRODUCT_PRICE", price)
      }
      if (product._5 != None) {
        writer.writeStringField("labels.PRODUCT_IMURL", product._5.get.asInstanceOf[String])
      }
      if (product._6 != None) {
        val related = product._6.get.asInstanceOf[Map[String,List[String]]]
        
        for ((k,v) <- related) {
          writer.writeStringField("labels.PRODUCT_RELATED_"+k, v.mkString(","))
        }
      }
      if (product._8 != None) {
        writer.writeStringField("labels.PRODUCT_BRAND", product._8.get.asInstanceOf[String])
      }
      if (product._9 != None) {
        val categories = product._9.get.asInstanceOf[List[List[String]]]
        val first_categories = categories(0)
        
        for(c <- 1 until first_categories.length+1) {
          included_categories += first_categories(c-1)
          writer.writeStringField("labels.PRODUCT_CATEGORIES_"+c.toString(), first_categories(c-1))
        }
      }
      if (product._7 != None) {
        val salesRanks = product._7.get.asInstanceOf[Map[String,Double]]
        
        for (idx <- 0 until included_categories.length) {
          if (salesRanks.contains(included_categories.apply(idx))) {
            included_salesranks += salesRanks.get(included_categories.apply(idx)).get.toInt
            writer.writeNumberField("labels.PRODUCT_SALESRANK_"+idx.toString, salesRanks.get(included_categories.apply(idx)).get.toInt)
          }
        }
      }
    }
    if (session.currentState.page=="NextSong") {
      writer.writeStringField("artist", session.currentSong.get._2)
      writer.writeStringField("song",  session.currentSong.get._3)
      writer.writeNumberField("length", session.currentSong.get._4)
    }
    writer.writeEndObject()
    writer.writeRaw('\n')
    writer.flush()
    
    var propArray:Array[String] = Array()
    if (showUserDetails) {
      props.foreach((p: (String, Any)) => {
        var propItem:Any = None
        p._2 match {
          case _: Long => propItem = p._2.asInstanceOf[Long]
          case _: Int => propItem = p._2.asInstanceOf[Int]
          case _: Double => propItem = p._2.asInstanceOf[Double]
          case _: Float => propItem = p._2.asInstanceOf[Float]
          case _: String => propItem = p._2.asInstanceOf[String]
          case _: Date => propItem = sdfSimple.format(p._2)
        }
        propArray = propArray :+ propItem.toString
      })
    }
    val propString = propArray.mkString("\t")
    
    var productArray:ArrayBuffer[String] = ArrayBuffer[String]()
    if (eventsWithProductData.contains(session.currentState.page)) {
      val product = session.currentProduct.get
      
      //(asin, description, title, price, imUrl, related, salesRank, brand, categories)
      productArray += product._1.get.asInstanceOf[String]
      product._2 match {
        case Some(i) => productArray += i.asInstanceOf[String]
        case None => productArray += ""
      }
      product._3 match {
        case Some(i) => productArray += i.asInstanceOf[String]
        case None => productArray += ""
      }
      product._4 match {
        case Some(i) => productArray += i.asInstanceOf[Double].toString()
        case None => productArray += ""
      }
      product._5 match {
        case Some(i) => productArray += i.asInstanceOf[String]
        case None => productArray += ""
      }
      
      // dropping related
      
      product._8 match {
        case Some(i) => productArray += i.asInstanceOf[String]
        case None => productArray += ""
      }
      product._9 match {
        case Some(i) => {
          for (idx <- 0 until 6) {
            if (included_categories.isDefinedAt(idx)) {
              productArray += included_categories.apply(idx)
            } else {
              productArray += ""
            }
          }
        }
        case None => {
          for (idx <- 0 until 6) {
            productArray += ""
          }
        }
      }
      product._7 match {
        case Some(i) => {
          for (idx <- 0 until 6) {
            if (included_categories.isDefinedAt(idx) && included_salesranks.contains(included_categories.apply(idx))) {
              productArray += included_categories.apply(idx)
            } else {
              productArray += ""
            }
          }
        }
        case None => {
          for (idx <- 0 until 6) {
            productArray += ""
          }
        }
      }
    } else {
      for (idx <- 0 until 18) {
        productArray += ""
      }
    }
    val productString = productArray.mkString("\t")
    
    val csvString:String = "%s\t%s\t%d\t%s\t%s\t%d\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n".format(
        sdfSimple.format(new Date(session.nextEventTimeStamp.get.toInstant(ZoneOffset.UTC).toEpochMilli)),
        session.currentState.page,
        session.sessionId,
        sdfSimple.format(new Date(this.lastEventTime.get.toInstant(ZoneOffset.UTC).toEpochMilli)),
        session.previousState.page,
        this.prevSessionId,
        if (showUserDetails) userId.toString else "",
        if (Main.tag.isDefined) Main.tag.get else "None",
        session.currentState.auth,
        session.currentState.method,
        session.currentState.status,
        session.currentState.level,
        session.itemInSession,
        propString,
        productString
    )
    csvstream.write(csvString.getBytes)
    
    
    // Writing IM File
    imwriter.writeStartObject()
    imwriter.writeStringField("event type", session.currentState.page)
    imwriter.writeStringField("event time", sdf.format(new Date(session.nextEventTimeStamp.get.toInstant(ZoneOffset.UTC).toEpochMilli)))
    imwriter.writeObjectFieldStart("actors")
    imwriter.writeStringField("customer", if (showUserDetails) userId.toString else "")
    imwriter.writeNumberField("session", session.sessionId)
    imwriter.writeEndObject()
    
    imwriter.writeObjectFieldStart("labels")
    if (showUserDetails && eventsWithCustomerData.contains(session.currentState.page)) {
      props.foreach((p: (String, Any)) => {
        val key = p._1.split("""\.""").last.toLowerCase()
        p._2 match {
          case _: String => imwriter.writeStringField(key, p._2.asInstanceOf[String])
          case _: Date => imwriter.writeStringField(key, sdf.format(p._2).toString)
          case _ => 
        }})
      if (Main.tag.isDefined)
        imwriter.writeStringField("tag", Main.tag.get)
    }
    //imwriter.writeStringField("auth", session.currentState.auth)
    //imwriter.writeStringField("http_method", session.currentState.method)
    //imwriter.writeStringField("http_status", session.currentState.status.toString)
    //imwriter.writeStringField("user_level", session.currentState.level)
    if (eventsWithProductData.contains(session.currentState.page)) {
      val product = session.currentProduct.get
      
      //(asin, description, title, price, imUrl, related, salesRank, brand, categories)
      imwriter.writeStringField("product_asin", product._1.get.asInstanceOf[String])
      if (product._2 != None) {
        imwriter.writeStringField("product_description", product._2.get.asInstanceOf[String])
      }
      if (product._3 != None) {
        imwriter.writeStringField("product_title", product._3.get.asInstanceOf[String])
      }
      if (product._5 != None) {
        imwriter.writeStringField("product_imurl", product._5.get.asInstanceOf[String])
      }
      if (product._6 != None) {
        val related = product._6.get.asInstanceOf[Map[String,List[String]]]
        
        for ((k,v) <- related) {
          imwriter.writeStringField("product_related_"+k, v.mkString(","))
        }
      }
      if (product._8 != None) {
        imwriter.writeStringField("product_brand", product._8.get.asInstanceOf[String])
      }
      if (product._9 != None) {
        val categories = product._9.get.asInstanceOf[List[List[String]]]
        val first_categories = categories(0)
        
        for(c <- 1 until first_categories.length+1) {
          included_categories += first_categories(c-1)
          imwriter.writeStringField("product_categories_"+c.toString(), first_categories(c-1))
        }
      }
    }
    imwriter.writeEndObject()
    
    imwriter.writeObjectFieldStart("values")
    imwriter.writeNumberField("item_in_session", session.itemInSession)
    if (showUserDetails && eventsWithCustomerData.contains(session.currentState.page)) {
      props.foreach((p: (String, Any)) => {
        val key = p._1.split("""\.""").last.toLowerCase()
        p._2 match {
          case _: Long => imwriter.writeNumberField(key, p._2.asInstanceOf[Long])
          case _: Int => imwriter.writeNumberField(key, p._2.asInstanceOf[Int])
          case _: Double => imwriter.writeNumberField(key, p._2.asInstanceOf[Double])
          case _: Float => imwriter.writeNumberField(key, p._2.asInstanceOf[Float])
          case _ =>
        }})
    }
    if (eventsWithProductData.contains(session.currentState.page)) {
      val product = session.currentProduct.get
      
      //(asin, description, title, price, imUrl, related, salesRank, brand, categories)
      if (product._4 != None) {
        val price = product._4.get.asInstanceOf[Double]
        
        imwriter.writeNumberField("product_price", price)
      }
      if (product._7 != None) {
        val salesRanks = product._7.get.asInstanceOf[Map[String,Double]]
        
        for ((k,v) <- salesRanks) {
          val category_idx = included_categories.indexOf(k)
          if (category_idx > -1) {
            imwriter.writeNumberField("product_salesrank_"+category_idx.toString, v.toInt)  
          }
        }
      }
    }
    imwriter.writeEndObject()
    
    imwriter.writeEndObject()
    imwriter.writeRaw('\n')
    imwriter.flush()
    
    
    
    this.lastEventTime = session.nextEventTimeStamp
    this.prevSessionId = session.sessionId
  }

  def tsToString(ts: LocalDateTime) = ts.toString()

  def nextEventTimeStampString =
    tsToString(this.session.nextEventTimeStamp.get)

  def mkString = props.+(
    "alpha" -> alpha,
    "beta" -> beta,
    "startTime" -> tsToString(startTime),
    "initialSessionStates" -> initialSessionStates,
    "nextEventTimeStamp" -> tsToString(session.nextEventTimeStamp.get) ,
    "sessionId" -> session.sessionId ,
    "userId" -> userId ,
    "currentState" -> session.currentState)
}

object User {
  protected val jsonFactory = new JsonFactory()
  jsonFactory.setRootValueSeparator("")
}