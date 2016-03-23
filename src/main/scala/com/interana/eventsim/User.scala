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
           val initialSessionStates: Map[(String,String),WeightedRandomThingGenerator[State]],
           val auth: String,
           val props: Map[String,Any],
           var device: Map[String,Any],
           val initialLevel: String,
           val stream: OutputStream,
           val csvstream: OutputStream,
           val imstream: OutputStream
          ) extends Serializable with Ordered[User] {

  val userId = Counters.nextUserId
  var session = new Session(
    Some(Session.pickFirstTimeStamp(startTime, alpha, beta)),
      alpha, beta, initialSessionStates, auth, initialLevel)
  val cart = new Cart
  
  val showUserDetails = ConfigFromFile.showUserWithState(session.currentState.auth)
  
  var currentCartValue = 0.0;
  var currentItemsInCart = 0;
  val eventsWithProductData:List[String] = List("Product Searched", "Return in Store", "Add To Cart", "Bar Code Scanned", "Product Compared", "Product Rated", "Product Recommended", "Product Reviewed", "Product Shared", "Product Viewed") 
  val eventsWithCustomerData:List[String] = List("Set Customer Profile")
  
  var lastEventTime = this.session.nextEventTimeStamp
  var prevSessionId = this.session.sessionId
  var prevProducts = ArrayBuffer[Product]()
  var lastProduct:Option[Product] = Some(this.session.currentProduct)

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
    val eventType = session.currentState.page
    
    val currentProduct = if(eventsWithProductData.contains(session.currentState.page)) {
      Some(session.currentProduct)
    } else {
      None
    }
    
    var additionalProps:Option[Map[String,Any]] = None
    
    eventType match {
      case "Add To Cart" => {
        session.currentProduct = lastProduct.get
        val product = lastProduct
        cart.addItem(product.get, 1)
        additionalProps = cart.getCartProps()
      }
      case "Checkout" => {
        additionalProps = cart.getCheckoutProps()
        cart.clearCart()
      }
      case "Cart Abandoned" => {
        cart.clearCart()
      }
      case _ => {}
    }
    
    var (included_categories, included_salesranks) = writeDP(additionalProps)   
    writeCSV(included_categories, included_salesranks)   
    writeIM(included_categories, included_salesranks)

    this.lastEventTime = session.nextEventTimeStamp
    this.prevSessionId = session.sessionId
    this.lastProduct = currentProduct
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
    
    
  def writeDP(additionalProps:Option[Map[String,Any]]) = {
    val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSSZ")
    
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
        }
      })
      if (Main.tag.isDefined)
          writer.writeStringField("actors.CUSTOMER.TAG", Main.tag.get)
    }
    if (additionalProps.isDefined) {
      additionalProps.get.foreach((p: (String, Any)) => {
        p._2 match {
          case _: Long => writer.writeNumberField("numbers."+p._1, p._2.asInstanceOf[Long])
          case _: Int => writer.writeNumberField("numbers."+p._1, p._2.asInstanceOf[Int])
          case _: Double => writer.writeNumberField("numbers."+p._1, p._2.asInstanceOf[Double])
          case _: Float => writer.writeNumberField("numbers."+p._1, p._2.asInstanceOf[Float])
          case _: String => writer.writeStringField("labels."+p._1, p._2.asInstanceOf[String])
          case _: Date => writer.writeStringField("labels."+p._1, sdf.format(p._2).toString)
        }
      })
    }
    if (eventsWithProductData.contains(session.currentState.page)) {
      val product = session.currentProduct
      
      //(asin, description, title, price, imUrl, related, salesRank, brand, categories)
      writer.writeStringField("labels.PRODUCT_ASIN", product.asin)
      if (product.description.isDefined) {
        writer.writeStringField("labels.PRODUCT_DESCRIPTION", product.description.get.asInstanceOf[String])
      }
      if (product.title.isDefined) {
        writer.writeStringField("labels.PRODUCT_TITLE", product.title.get.asInstanceOf[String])
      }
      if (product.price.isDefined) {
        val price = product.price.get.asInstanceOf[Double]
        
        writer.writeNumberField("numbers.PRODUCT_PRICE", price)
      }
      if (product.imUrl.isDefined) {
        writer.writeStringField("labels.PRODUCT_IMURL", product.imUrl.get.asInstanceOf[String])
      }
      if (product.related.isDefined) {
        val related = product.related.get.asInstanceOf[Map[String,List[String]]]
        
        for ((k,v) <- related) {
          writer.writeStringField("labels.PRODUCT_RELATED_"+k, v.mkString(","))
        }
      }
      if (product.brand.isDefined) {
        writer.writeStringField("labels.PRODUCT_BRAND", product.brand.get.asInstanceOf[String])
      }
      if (product.categories.isDefined) {
        val categories = product.categories.get.asInstanceOf[List[List[String]]]
        val first_categories = categories(0)
        
        for(c <- 1 until first_categories.length+1) {
          included_categories += first_categories(c-1)
          writer.writeStringField("labels.PRODUCT_CATEGORIES_"+c.toString(), first_categories(c-1))
        }
      }
      if (product.salesRank.isDefined) {
        val salesRanks = product.salesRank.get.asInstanceOf[Map[String,Double]]
        
        for (idx <- 1 until included_categories.length+1) {
          if (salesRanks.contains(included_categories.apply(idx-1))) {
              included_salesranks += salesRanks.get(included_categories.apply(idx-1)).get.toInt
              writer.writeNumberField("labels.PRODUCT_SALESRANK_"+(idx).toString, salesRanks.get(included_categories.apply(idx-1)).get.toInt)
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
    
    (included_categories, included_salesranks)
  }
  
  def writeCSV(included_categories:ArrayBuffer[String], included_salesranks:ArrayBuffer[Integer]) = {
    val sdfSimple = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")
    
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
      val product = session.currentProduct
      
      //(asin, description, title, price, imUrl, related, salesRank, brand, categories)
      productArray += product.asin
      product.description match {
        case Some(i) => productArray += i.asInstanceOf[String]
        case None => productArray += ""
      }
      product.title match {
        case Some(i) => productArray += i.asInstanceOf[String]
        case None => productArray += ""
      }
      product.price match {
        case Some(i) => productArray += i.asInstanceOf[Double].toString()
        case None => productArray += ""
      }
      product.imUrl match {
        case Some(i) => productArray += i.asInstanceOf[String]
        case None => productArray += ""
      }
      
      // dropping related
      
      product.brand match {
        case Some(i) => productArray += i.asInstanceOf[String]
        case None => productArray += ""
      }
      product.categories match {
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
      product.salesRank match {
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
  }
  
  def writeIM(included_categories:ArrayBuffer[String], included_salesranks:ArrayBuffer[Integer]) = {
    val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSSZ")
    
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
      val product = session.currentProduct
      
      //(asin, description, title, price, imUrl, related, salesRank, brand, categories)
      imwriter.writeStringField("product_asin", product.asin)
      if (product.description.isDefined) {
        imwriter.writeStringField("product_description", product.description.get.asInstanceOf[String])
      }
      if (product.title.isDefined) {
        imwriter.writeStringField("product_title", product.title.get.asInstanceOf[String])
      }
      if (product.imUrl.isDefined) {
        imwriter.writeStringField("product_imurl", product.imUrl.get.asInstanceOf[String])
      }
      if (product.related.isDefined) {
        val related = product.related.get.asInstanceOf[Map[String,List[String]]]
        
        for ((k,v) <- related) {
          imwriter.writeStringField("product_related_"+k, v.mkString(","))
        }
      }
      if (product.brand.isDefined) {
        imwriter.writeStringField("product_brand", product.brand.get.asInstanceOf[String])
      }
      if (product.categories.isDefined) {
        val categories = product.categories.get.asInstanceOf[List[List[String]]]
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
      val product = session.currentProduct
      
      //(asin, description, title, price, imUrl, related, salesRank, brand, categories)
      if (product.price.isDefined) {
        val price = product.price.get.asInstanceOf[Double]
        
        imwriter.writeNumberField("product_price", price)
      }
      if (product.salesRank.isDefined) {
        val salesRanks = product.salesRank.get.asInstanceOf[Map[String,Double]]
        
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
  }
}

object User {
  protected val jsonFactory = new JsonFactory()
  jsonFactory.setRootValueSeparator("")
}