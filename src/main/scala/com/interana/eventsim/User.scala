package com.interana.eventsim

import java.io.{OutputStream, Serializable, FileWriter}
import java.time.{ZoneOffset, LocalDateTime}
import java.text.SimpleDateFormat
import java.util.Date
import com.fasterxml.jackson.core.{JsonEncoding, JsonFactory}
import com.interana.eventsim.config.ConfigFromFile
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.HashMap
import scala.util.parsing.json.JSONObject
import scala.io.Source
import com.interana.eventsim.buildin.{RandomFirstNameGenerator, RandomLastNameGenerator, RandomStringGenerator, RandomEmailGenerator, DeviceProperties, UserProperties}
import com.interana.eventsim.messages._
import com.interana.eventsim.devices._
import com.interana.eventsim.channels._
import com.interana.eventsim.channels.interaction._
import com.interana.eventsim.channels.marketing._
import com.interana.eventsim.EventType
import com.interana.eventsim.channels.Phone

// TODO: Refactor User class to make it easily understandable. User has multiple devices, each device generates multiple sessions.

class User(val startTime: LocalDateTime,
           val writers: Map[String,OutputStream]
          ) extends Serializable with Ordered[User] with Extractable {
  def getNamespace() = {
    "User"
  }
  
//  println(ConfigFromFile.eventTypes)
  /*
   * Configurations
   */
  
  private val sqrtE = Math.exp(0.5)
  def logNormalRandomValue = Math.exp(TimeUtilities.rng.nextGaussian()) / sqrtE
  
  val alpha = ConfigFromFile.alpha * logNormalRandomValue
  val beta = ConfigFromFile.beta * logNormalRandomValue
  val initialSessionStates = ConfigFromFile.initialStates
  var auth = ConfigFromFile.authGenerator.randomThing
  var userAttributes = UserProperties.customProps(ConfigFromFile.individualAttributeSetGenerator)
  val device = DeviceProperties.randomProps
  var level = ConfigFromFile.levelGenerator.randomThing
  
  
  /*
   * TODO: What needs to be defined
   * 	user id
   * 	basic profile (first name, last name, address, demographic information)
   * 	user-level event transitions
   * 	user status (logged in, logged out)
   * 	list of devices
   * 	list of sessions
   * 	channel preferences
   * 	coupon queue
   * 	loyalty information
   * 	cart, cartItem, orders
   * 	loyalty
   */
  
  // user id
  val userId = Counters.nextUserId
  
  // basic profile
  userAttributes.put("userId", userId.toString())
  userAttributes.put("firstName", RandomFirstNameGenerator.randomThing._1)
  userAttributes.put("lastName", RandomLastNameGenerator.randomThing)
  userAttributes.put("email", RandomEmailGenerator.randomThing)
  // refer to userAttribute Map for additional attributes
  
  var exposedAttributes = Set[String]()
  
  // user-level event transitions
  val transitions:HashMap[(EventType, EventType),Any] = new HashMap[(EventType, EventType),Any]()
  
  // list of devices
  val devices:HashMap[String,Device] = generateDevices(userAttributes("age").asInstanceOf[String])
  
  // channels
  val channels:HashMap[String,Channel] = generateChannels(devices)
  
  // list of sessions
  val sessions:HashMap[String,Session] = new HashMap[String,Session]()
  
  // channel preferences
  val channelPreferences = User.getChannelPreference(userAttributes("age").asInstanceOf[String], channels)
  
  // coupon stack
  val coupons:ArrayBuffer[Coupon] = new ArrayBuffer[Coupon]()
  
  // cart
  val cart = new Cart()
  
  // loyalty
  val loyaltyId = RandomStringGenerator.randomAlphanumericString(20)
  val loyaltyPoints = 0.0
  
  // entry points
  val entryPoints = ConfigFromFile.entryPoints

  
  
  var session = Session.createSession(this)
  
  val showUserDetails = ConfigFromFile.showUserWithState(session.currentState.auth)
  
  val eventsWithProductData:List[String] = List("Product Searched", "Return in Store", "Add To Cart", "Bar Code Scanned", "Product Compared", "Product Rated", "Product Recommended", "Product Reviewed", "Product Shared", "Product Viewed") 
  val eventsWithCustomerData:List[String] = List("Set Customer Profile")
  
  var lastEventTime = this.session.nextEventTimeStamp
  var prevSessionId = this.session.sessionId
  var prevProducts = ArrayBuffer[Product]()
  var lastProduct:Option[Product] = None
  
  def attributes = new HashMap[String, Any]()
  
  def getAttributeMap() = {
    getExposedAttributeMap
  }
  
  def getId() = {
    userId.toString()
  }
  
  def getCSVMap() = {
    userAttributes.toMap
  }
  

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
      val lastPage = if(session.previousState.isDefined) session.previousState.get.page else None
      if (session.previousState.isDefined) this.writeEvent()
      if (TimeUtilities.rng.nextDouble() < prAttrition ||
          ConfigFromFile.churnedStates.contains(lastPage)) {
        session.nextEventTimeStamp = None
        // TODO: mark as churned
      }
      else {
        session = session.nextSession
      }
    }
  }

  private val EMPTY_MAP = Map()

//  def eventString = {
//    val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ssZ")
//    var m = device.+(
//      "ts" -> sdf.format(new Date(session.nextEventTimeStamp.get.toInstant(ZoneOffset.UTC).toEpochMilli)),
//      "userId" -> (if (showUserDetails) userId else ""),
//      "sessionId" -> session.sessionId,
//      "page" -> session.currentState.page,
//      "auth" -> session.currentState.auth,
//      "method" -> session.currentState.method,
//      "status" -> session.currentState.status
////      "itemInSession" -> session.itemInSession
//    )
//
//    if (showUserDetails)
//      m ++= userAttributes
//
//    /* most of the event generator code is pretty generic, but this is hard-coded
//     * for a fake music web site
//     */
////    if (session.currentState.page=="NextSong")
////      m += (
////        "artist" -> session.currentSong.get._2,
////        "song" -> session.currentSong.get._3,
////        "length" -> session.currentSong.get._4
////        )
//
//    val j = new JSONObject(m)
//    j.toString()
//  }


  val writer = User.jsonFactory.createGenerator(writers("dp"), JsonEncoding.UTF8)
  val imwriter = User.jsonFactory.createGenerator(writers("im"), JsonEncoding.UTF8)
  val csvstream = writers("csv")
  
  def getDeviceForChannel(channel:Channel) = {
    val randomDeviceSelector = new WeightedRandomThingGenerator[Device]()
    devices.foreach(d => 
      if(d._2.supportedChannels.contains(channel.name)) {
        randomDeviceSelector.add(d._2, 1)
      }
    )
    
    randomDeviceSelector.randomThing
  }
  
  def writeEvent() = {    
    val eventType = session.currentState.page
    
//    val currentProduct = if(eventsWithProductData.contains(session.currentState.page)) {
//      Some(session.currentProduct)
//    } else {
//      None
//    }
        
    session.currentEvent.writeDP(writer)
    session.currentEvent.writeCSV(csvstream)
    session.currentEvent.writeIM(imwriter)

    this.lastEventTime = session.nextEventTimeStamp
    this.prevSessionId = session.sessionId
//    this.lastProduct = currentProduct
  }

  def tsToString(ts: LocalDateTime) = ts.toString()

  def nextEventTimeStampString =
    tsToString(this.session.nextEventTimeStamp.get)

  def mkString = userAttributes.+(
    "alpha" -> alpha,
    "beta" -> beta,
    "startTime" -> tsToString(startTime),
    "initialSessionStates" -> initialSessionStates,
    "nextEventTimeStamp" -> tsToString(session.nextEventTimeStamp.get) ,
    "sessionId" -> session.sessionId ,
    "userId" -> userId ,
    "currentState" -> session.currentState)
    
//    
//  def writeDP(additionalProps:Option[Map[String,Any]]) = {
//    val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSSZ")
//    
//    var included_categories:ArrayBuffer[String] = ArrayBuffer[String]()
//    var included_salesranks:ArrayBuffer[Integer] = ArrayBuffer[Integer]()
//    
//    writer.writeStartObject()
//    writer.writeStringField("eventTime", sdf.format(new Date(session.nextEventTimeStamp.get.toInstant(ZoneOffset.UTC).toEpochMilli)))
//    writer.writeStringField("actors.CUSTOMER.CUSTOMER_ID", if (showUserDetails) userId.toString else "")
//    writer.writeStringField("actors.CUSTOMER.SESSION_ID", session.sessionId)
//    writer.writeStringField("eventType", session.currentState.page)
//    //writer.writeStringField("labels.AUTH", session.currentState.auth)
//    //writer.writeStringField("labels.HTTP_METHOD", session.currentState.method)
//    //writer.writeNumberField("labels.HTTP_STATUS", session.currentState.status)
//    //writer.writeStringField("labels.USER_LEVEL", session.currentState.level)
//    //writer.writeNumberField("labels.ITEM_IN_SESSION", session.itemInSession)
//    if (showUserDetails && eventsWithCustomerData.contains(session.currentState.page)) {
//      userAttributes.foreach((p: (String, Any)) => {
//        p._2 match {
//          case _: Long => writer.writeNumberField(p._1, p._2.asInstanceOf[Long])
//          case _: Int => writer.writeNumberField(p._1, p._2.asInstanceOf[Int])
//          case _: Double => writer.writeNumberField(p._1, p._2.asInstanceOf[Double])
//          case _: Float => writer.writeNumberField(p._1, p._2.asInstanceOf[Float])
//          case _: String => writer.writeStringField(p._1, p._2.asInstanceOf[String])
//          case _: Date => writer.writeStringField(p._1, sdf.format(p._2).toString)
//        }
//      })
//      if (Main.tag.isDefined)
//          writer.writeStringField("actors.CUSTOMER.TAG", Main.tag.get)
//    }
//    if (additionalProps.isDefined) {
//      additionalProps.get.foreach((p: (String, Any)) => {
//        p._2 match {
//          case _: Long => writer.writeNumberField("numbers."+p._1, p._2.asInstanceOf[Long])
//          case _: Int => writer.writeNumberField("numbers."+p._1, p._2.asInstanceOf[Int])
//          case _: Double => writer.writeNumberField("numbers."+p._1, p._2.asInstanceOf[Double])
//          case _: Float => writer.writeNumberField("numbers."+p._1, p._2.asInstanceOf[Float])
//          case _: String => writer.writeStringField("labels."+p._1, p._2.asInstanceOf[String])
//          case _: Date => writer.writeStringField("labels."+p._1, sdf.format(p._2).toString)
//          case _ => println(p._1); println(p._2)
//        }
//      })
//    }
//    if (eventsWithProductData.contains(session.currentState.page)) {
//      val product = session.currentProduct
//      
//      //(asin, description, title, price, imUrl, related, salesRank, brand, categories)
//      writer.writeStringField("labels.PRODUCT_ASIN", product.asin)
//      if (product.description.isDefined) {
//        writer.writeStringField("labels.PRODUCT_DESCRIPTION", product.description.get.asInstanceOf[String])
//      }
//      if (product.title.isDefined) {
//        writer.writeStringField("labels.PRODUCT_TITLE", product.title.get.asInstanceOf[String])
//      }
//      if (product.price.isDefined) {
//        val price = product.price.get.asInstanceOf[Double]
//        
//        writer.writeNumberField("numbers.PRODUCT_PRICE", price)
//      }
//      if (product.imUrl.isDefined) {
//        writer.writeStringField("labels.PRODUCT_IMURL", product.imUrl.get.asInstanceOf[String])
//      }
//      if (product.related.isDefined) {
//        val related = product.related.get.asInstanceOf[Map[String,List[String]]]
//        
//        for ((k,v) <- related) {
//          writer.writeStringField("labels.PRODUCT_RELATED_"+k, v.mkString(","))
//        }
//      }
//      if (product.brand.isDefined) {
//        writer.writeStringField("labels.PRODUCT_BRAND", product.brand.get.asInstanceOf[String])
//      }
//      if (product.categories.isDefined) {
//        val categories = product.categories.get.asInstanceOf[List[List[String]]]
//        val first_categories = categories(0)
//        
//        for(c <- 1 until first_categories.length+1) {
//          included_categories += first_categories(c-1)
//          writer.writeStringField("labels.PRODUCT_CATEGORIES_"+c.toString(), first_categories(c-1))
//        }
//      }
//      if (product.salesRank.isDefined) {
//        val salesRanks = product.salesRank.get.asInstanceOf[Map[String,Double]]
//        
//        for (idx <- 1 until included_categories.length+1) {
//          if (salesRanks.contains(included_categories.apply(idx-1))) {
//              included_salesranks += salesRanks.get(included_categories.apply(idx-1)).get.toInt
//              writer.writeNumberField("labels.PRODUCT_SALESRANK_"+(idx).toString, salesRanks.get(included_categories.apply(idx-1)).get.toInt)
//          }
//        }
//      }
//    }
////    if (session.currentState.page=="NextSong") {
////      writer.writeStringField("artist", session.currentSong.get._2)
////      writer.writeStringField("song",  session.currentSong.get._3)
////      writer.writeNumberField("length", session.currentSong.get._4)
////    }
//    
//    writer.writeEndObject()
//    writer.writeRaw('\n')
//    writer.flush()
//    
//    (included_categories, included_salesranks)
//  }
//  
//  def writeCSV(included_categories:ArrayBuffer[String], included_salesranks:ArrayBuffer[Integer]) = {
//    val sdfSimple = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")
//    
//    var propArray:Array[String] = Array()
//    if (showUserDetails) {
//      userAttributes.foreach((p: (String, Any)) => {
//        var propItem:Any = None
//        p._2 match {
//          case _: Long => propItem = p._2.asInstanceOf[Long]
//          case _: Int => propItem = p._2.asInstanceOf[Int]
//          case _: Double => propItem = p._2.asInstanceOf[Double]
//          case _: Float => propItem = p._2.asInstanceOf[Float]
//          case _: String => propItem = p._2.asInstanceOf[String]
//          case _: Date => propItem = sdfSimple.format(p._2)
//        }
//        propArray = propArray :+ propItem.toString
//      })
//    }
//    val propString = propArray.mkString("\t")
//    
//    var productArray:ArrayBuffer[String] = ArrayBuffer[String]()
//    if (eventsWithProductData.contains(session.currentState.page)) {
//      val product = session.currentProduct
//      
//      //(asin, description, title, price, imUrl, related, salesRank, brand, categories)
//      productArray += product.asin
//      product.description match {
//        case Some(i) => productArray += i.asInstanceOf[String]
//        case None => productArray += ""
//      }
//      product.title match {
//        case Some(i) => productArray += i.asInstanceOf[String]
//        case None => productArray += ""
//      }
//      product.price match {
//        case Some(i) => productArray += i.asInstanceOf[Double].toString()
//        case None => productArray += ""
//      }
//      product.imUrl match {
//        case Some(i) => productArray += i.asInstanceOf[String]
//        case None => productArray += ""
//      }
//      
//      // dropping related
//      
//      product.brand match {
//        case Some(i) => productArray += i.asInstanceOf[String]
//        case None => productArray += ""
//      }
//      product.categories match {
//        case Some(i) => {
//          for (idx <- 0 until 6) {
//            if (included_categories.isDefinedAt(idx)) {
//              productArray += included_categories.apply(idx)
//            } else {
//              productArray += ""
//            }
//          }
//        }
//        case None => {
//          for (idx <- 0 until 6) {
//            productArray += ""
//          }
//        }
//      }
//      product.salesRank match {
//        case Some(i) => {
//          for (idx <- 0 until 6) {
//            if (included_categories.isDefinedAt(idx) && included_salesranks.contains(included_categories.apply(idx))) {
//              productArray += included_categories.apply(idx)
//            } else {
//              productArray += ""
//            }
//          }
//        }
//        case None => {
//          for (idx <- 0 until 6) {
//            productArray += ""
//          }
//        }
//      }
//    } else {
//      for (idx <- 0 until 18) {
//        productArray += ""
//      }
//    }
//    val productString = productArray.mkString("\t")
//    
//    val csvString:String = "%s\t%s\t%d\t%s\t%s\t%d\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n".format(
//        sdfSimple.format(new Date(session.nextEventTimeStamp.get.toInstant(ZoneOffset.UTC).toEpochMilli)),
//        session.currentState.page,
//        session.sessionId,
//        sdfSimple.format(new Date(this.lastEventTime.get.toInstant(ZoneOffset.UTC).toEpochMilli)),
//        if(session.previousState.isDefined) session.previousState.get.page else "",
//        this.prevSessionId,
//        if (showUserDetails) userId.toString else "",
//        if (Main.tag.isDefined) Main.tag.get else "None",
//        session.currentState.auth,
//        session.currentState.method,
//        session.currentState.status,
//        session.currentState.level,
////        session.itemInSession,
//        propString,
//        productString
//    )
//    csvstream.write(csvString.getBytes)
//  }
//  
//  def writeIM(included_categories:ArrayBuffer[String], included_salesranks:ArrayBuffer[Integer]) = {
//    val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSSZ")
//    
//    // Writing IM File
//    imwriter.writeStartObject()
//    imwriter.writeStringField("event type", session.currentState.page)
//    imwriter.writeStringField("event time", sdf.format(new Date(session.nextEventTimeStamp.get.toInstant(ZoneOffset.UTC).toEpochMilli)))
//    imwriter.writeObjectFieldStart("actors")
//    imwriter.writeStringField("customer", if (showUserDetails) userId.toString else "")
//    imwriter.writeStringField("session", session.sessionId)
//    imwriter.writeEndObject()
//    
//    imwriter.writeObjectFieldStart("labels")
//    if (showUserDetails && eventsWithCustomerData.contains(session.currentState.page)) {
//      userAttributes.foreach((p: (String, Any)) => {
//        val key = p._1.split("""\.""").last.toLowerCase()
//        p._2 match {
//          case _: String => imwriter.writeStringField(key, p._2.asInstanceOf[String])
//          case _: Date => imwriter.writeStringField(key, sdf.format(p._2).toString)
//          case _ => 
//        }})
//      if (Main.tag.isDefined)
//        imwriter.writeStringField("tag", Main.tag.get)
//    }
//    //imwriter.writeStringField("auth", session.currentState.auth)
//    //imwriter.writeStringField("http_method", session.currentState.method)
//    //imwriter.writeStringField("http_status", session.currentState.status.toString)
//    //imwriter.writeStringField("user_level", session.currentState.level)
//    if (eventsWithProductData.contains(session.currentState.page)) {
//      val product = session.currentProduct
//      
//      //(asin, description, title, price, imUrl, related, salesRank, brand, categories)
//      imwriter.writeStringField("product_asin", product.asin)
//      if (product.description.isDefined) {
//        imwriter.writeStringField("product_description", product.description.get.asInstanceOf[String])
//      }
//      if (product.title.isDefined) {
//        imwriter.writeStringField("product_title", product.title.get.asInstanceOf[String])
//      }
//      if (product.imUrl.isDefined) {
//        imwriter.writeStringField("product_imurl", product.imUrl.get.asInstanceOf[String])
//      }
//      if (product.related.isDefined) {
//        val related = product.related.get.asInstanceOf[Map[String,List[String]]]
//        
//        for ((k,v) <- related) {
//          imwriter.writeStringField("product_related_"+k, v.mkString(","))
//        }
//      }
//      if (product.brand.isDefined) {
//        imwriter.writeStringField("product_brand", product.brand.get.asInstanceOf[String])
//      }
//      if (product.categories.isDefined) {
//        val categories = product.categories.get.asInstanceOf[List[List[String]]]
//        val first_categories = categories(0)
//        
//        for(c <- 1 until first_categories.length+1) {
//          included_categories += first_categories(c-1)
//          imwriter.writeStringField("product_categories_"+c.toString(), first_categories(c-1))
//        }
//      }
//    }
//    imwriter.writeEndObject()
//    
//    imwriter.writeObjectFieldStart("values")
////    imwriter.writeNumberField("item_in_session", session.itemInSession)
//    if (showUserDetails && eventsWithCustomerData.contains(session.currentState.page)) {
//      userAttributes.foreach((p: (String, Any)) => {
//        val key = p._1.split("""\.""").last.toLowerCase()
//        p._2 match {
//          case _: Long => imwriter.writeNumberField(key, p._2.asInstanceOf[Long])
//          case _: Int => imwriter.writeNumberField(key, p._2.asInstanceOf[Int])
//          case _: Double => imwriter.writeNumberField(key, p._2.asInstanceOf[Double])
//          case _: Float => imwriter.writeNumberField(key, p._2.asInstanceOf[Float])
//          case _ =>
//        }})
//    }
//    if (eventsWithProductData.contains(session.currentState.page)) {
//      val product = session.currentProduct
//      
//      //(asin, description, title, price, imUrl, related, salesRank, brand, categories)
//      if (product.price.isDefined) {
//        val price = product.price.get.asInstanceOf[Double]
//        
//        imwriter.writeNumberField("product_price", price)
//      }
//      if (product.salesRank.isDefined) {
//        val salesRanks = product.salesRank.get.asInstanceOf[Map[String,Double]]
//        
//        for ((k,v) <- salesRanks) {
//          val category_idx = included_categories.indexOf(k)
//          if (category_idx > -1) {
//            imwriter.writeNumberField("product_salesrank_"+category_idx.toString, v.toInt)  
//          }
//        }
//      }
//    }
//    imwriter.writeEndObject()
//    
//    imwriter.writeEndObject()
//    imwriter.writeRaw('\n')
//    imwriter.flush()
//  }
  
  def pickDevice() = {
    var device:Option[Device] = None
    
    while(device.isEmpty) {
      var channelName = channelPreferences.randomThing
      if(channels.keySet.contains(channelName)) {
        device = Some(getDeviceForChannel(channels.get(channelName).get))
      }
    }
    
    device
  }
  
  // generate channels
  def generateChannels(devices:HashMap[String,Device]) = {
    val channels = HashMap[String,Channel]()
    
    devices.foreach{d => 
      d._2.supportedChannels.foreach { c => 
        if(!channels.keySet.contains(c)) {
          c match {
            case "Phone" => channels.put("Phone", new Phone())
            case "Email" => channels.put("Email", new Email())
            case "Social" => channels.put("Social", new Social())
            case "MobileApp" => channels.put("MobileApp", new MobileApp())
            case "Mail" => channels.put("Mail", new Mail())
            case "Website" => channels.put("Website", new Website())
            case "SMS" => channels.put("SMS", new SMS())
            case "DisplayAd" => channels.put("DisplayAd", new DisplayAd())
            case "SearchEngine" => channels.put("SearchEngine", new SearchEngine())
            case _ => println("unsupported channel", c)
          }
        }
      }
    }
    
    channels
  }
  
  
  // generate devices according to some 
  def generateDevices(age:String) = {
    val devices:HashMap[String,Device] = new HashMap[String,Device]()
    
    val distribution:(Double, Double, Double, Double) = age match {
      case "13-17" => (30.0, 15.0, 70.0, 5.0)
      case "18-24" => (30.0, 35.0, 90.0, 5.0)
      case "25-34" => (30.0, 35.0, 90.0, 10.0)
      case "35-44" => (30.0, 35.0, 80.0, 10.0)
      case "45-54" => (20.0, 25.0, 70.0, 10.0)
      case "55-64" => (15.0, 15.0, 50.0, 30.0)
      case "65-75" => (10.0, 15.0, 30.0, 25.0)
      case "+75" => (5.0, 10.0, 25.0, 10.0)
      case _ => (0.0, 0.0, 0.0, 0.0)
    }
    
    var generator = new WeightedRandomThingGenerator[String]()
    generator.add("desktop", distribution._1.toInt)
    generator.add("laptop", distribution._2.toInt)
    generator.add("mobilephone", distribution._3.toInt)
    generator.add("tablet", distribution._4.toInt)
    
    
    val numDevice = TimeUtilities.rng.nextInt(3)+1
    
    for(i <- Iterator.range(0, numDevice)) {
      val deviceType = generator.randomThing
      devices += (deviceType -> createDevice(deviceType))
    }
    
    devices
  }
  
  def createDevice(deviceType:String) = {
    deviceType match {
      case "desktop" => new Desktop()
      case "laptop" => new Laptop()
      case "mobilephone" => new MobilePhone()
      case "tablet" => new Tablet()
    }
  }
  
  def getExposedAttributeMap = {
    val exposedAttributeMap = new HashMap[String, Any]()
    exposedAttributes.foreach{ a:String =>
      if(userAttributes.keySet.contains(a)) {
        exposedAttributeMap.put(a, userAttributes.get(a).get)
      }
    }
    exposedAttributeMap
  }
}

object User extends Actor {
  protected val jsonFactory = new JsonFactory()
  jsonFactory.setRootValueSeparator("")
  
  // get channel preference
  def getChannelPreference(age:String, channels:HashMap[String,Channel]):WeightedRandomThingGenerator[String] = {
    /*
     *  Channel preferences are from zendesk whitepaper
     *  https://www.zendesk.com/resources/a-guide-to-multi-channel-customer-support/
     */
    
    val channelPreference:(Double, Double, Double, Double, Double, Double) = age match {
      case "13-17" => (29.4, 42.3, 36.4, 31.9, 5.9, 20.0)
      case "18-24" => (29.4, 42.3, 36.4, 31.9, 5.9, 20.0)
      case "25-34" => (46.3, 44.1, 20.7, 17.2, 9.6, 20.0)
      case "35-44" => (46.3, 44.1, 20.7, 17.2, 9.6, 20.0)
      case "45-54" => (46.3, 44.1, 20.7, 17.2, 9.6, 20.0)
      case "55-64" => (59.6, 22.5, 3.5, 3.0, 13.4, 20.0)
      case "65-75" => (59.6, 22.5, 3.5, 3.0, 13.4, 20.0)
      case "+75" => (55.6, 6.6, 0.7, 0.3, 17.2, 20.0)
      case _ => (0.0, 0.0, 0.0, 0.0, 0.0, 100.0)
    }
    
    val generator = new WeightedRandomThingGenerator[String]()
    if(channels.keySet.contains("Website") && channelPreference._1 > 0.0) {
      generator.add("Website", (channelPreference._1*10).toInt)
    }
    if(channels.keySet.contains("Email") && channelPreference._2 > 0.0) {
      generator.add("Email", (channelPreference._2*10).toInt)
    }
    if(channels.keySet.contains("Social") && channelPreference._3 > 0.0) {
      generator.add("Social", (channelPreference._3*10).toInt)
    }
    if(channels.keySet.contains("MobileApp") && channelPreference._4 > 0.0) {
      generator.add("MobileApp", (channelPreference._4*10).toInt)
    }
    if(channels.keySet.contains("Mail") && channelPreference._5 > 0.0) {
      generator.add("Mail", (channelPreference._5*10).toInt)
    }
    if(channels.keySet.contains("Offline") && channelPreference._6 > 0.0) {
      generator.add("Offline", (channelPreference._6*10).toInt)
    }
    
    generator
  }
  
  /*
 	* Actor Methods
 	*/
  
  def getNamespace() = {
    "User"
  }
  
  def getCSVHeaders() = {
    List("id", "firstName", "lastName", "email", "gender", "race", "income", "age", "education", "employment", "marital status", "car", "interest", "activity")
  }
}